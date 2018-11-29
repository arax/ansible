#!/usr/bin/env python

# Copyright (c) 2018, CESNET
#
# This module is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.

import argparse
import json
import logging
import os
import re
import sys
import yaml

from pypuppetdb import connect
from pypuppetdb.QueryBuilder import (
    AndOperator,
    EqualsOperator,
    InOperator,
    NullOperator,
    RegexOperator,
)

HOME_CONFIG = os.environ['HOME'] + '/.puppetdb.yml'
GLOBAL_CONFIG = '/etc/ansible/puppetdb.yml'
CONFIG_FILES = [HOME_CONFIG, GLOBAL_CONFIG]

NODE_ENVS = ['catalog_environment', 'facts_environment', 'report_environment']
DEFAULT_LAST_REPORT = 2
FACT_VALUE_TYPES = [str, unicode, int, float, bool]

LOGGER = logging.getLogger(__name__)


def to_json(inv_dict):
    return json.dumps(inv_dict, sort_keys=True, indent=2)


def from_yaml(path):
    with open(path, 'r') as f:
        ydict = yaml.safe_load(f)
    return ydict


def parse_config():
    cfg = dict()
    for f in CONFIG_FILES:
        if os.path.isfile(f) and os.access(f, os.R_OK):
            cfg = from_yaml(f)
            break

    return cfg


def parse_args():
    parser = argparse.ArgumentParser(description='PuppetDB Inventory Module')
    parser.add_argument('--environment',
                        help='PuppetDB environment name for query scoping')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debug output')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--list', action='store_true',
                       help='List active nodes')
    group.add_argument('--host', help='List details about the specific node')

    return parser.parse_args()


def db_connect(cfg):
    # Note: the `ca_cert` option is not supported by pypuppetdb
    #       hence setting an ENV variable for the requests lib
    ca_cert = cfg.pop('ca_cert', None)
    if ca_cert:
        os.environ['REQUESTS_CA_BUNDLE'] = ca_cert

    return connect(**cfg)


def resource_query_args(cfg):
    args = dict()

    op = AndOperator()
    op.add(EqualsOperator('environment', cfg['environment']))
    op.add(EqualsOperator('type', cfg['resource_type']))
    op.add(RegexOperator('title', cfg['title_regexp']))

    args['query'] = op

    return args


def node_query_args(cfg):
    args = {
        'with_status': True,
        'unreported': cfg['last_report'] if cfg['last_report'] else DEFAULT_LAST_REPORT,
    }

    op = AndOperator()
    for env in NODE_ENVS:
        op.add(EqualsOperator(env, cfg['environment']))
    op.add(NullOperator('deactivated', not cfg['deactivated']))
    op.add(NullOperator('expired', not cfg['expired']))

    args['query'] = op

    return args


def facts_query_args(node, cfg):
    args = dict()

    op = AndOperator()
    op.add(EqualsOperator('environment', cfg['environment']))
    op.add(EqualsOperator('certname', node))

    incl = InOperator('name')
    incl.add_array(cfg['facts_as_hostvars'])
    op.add(incl)

    args['query'] = op

    return args


def hostvars_from_facts(facts):
    host_vars = dict()
    for fact in facts:
        if type(fact.value) not in FACT_VALUE_TYPES:
            raise ValueError("Value of fact '%s' is not allowed (only primitive types)" % fact.name)
        host_vars[fact.name] = fact.value

    return host_vars


def main():
    args = parse_args()
    # TODO(xparak): implement `--host`
    if not args.list:
        raise RuntimeError('Only `--list` is currently supported')

    LOGGER.addHandler(logging.StreamHandler(sys.stderr))
    LOGGER.setLevel(logging.DEBUG) if args.debug else False

    cfg = parse_config()
    if args.environment:
        # Override query environment from static configuration if argument is given
        cfg['query']['environment'] = args.environment

    pdb = db_connect(cfg['puppetdb'])
    output = {'_meta': {'hostvars': dict()}}

    # Read nodes scoped to given query parameters and add facts for hostvars
    query_args = node_query_args(cfg['query'])
    nodes = list()
    for node in pdb.nodes(**query_args):
        LOGGER.debug("Discovered host %s as node last seen %s" % (node.name, node.report_timestamp))
        nodes.append(node.name)

        if cfg['query']['facts_as_hostvars']:
            query_args_facts = facts_query_args(node.name, cfg['query'])
            output['_meta']['hostvars'][node.name] = hostvars_from_facts(pdb.facts(**query_args_facts))

    # Read resources and convert them to host groups
    query_args = resource_query_args(cfg['query'])
    for resource in pdb.resources(**query_args):
        if resource.node not in nodes:
            LOGGER.debug("Skipping host %s based on query scope" % resource.node)
            continue

        group = re.sub(cfg['query']['title_strip'], '', resource.name).lower()
        group = re.sub('::', '_', group)
        LOGGER.debug("Normalized '%s' to '%s'" % (resource.name, group))

        if output.get(group) is None:
            LOGGER.debug("Discovered group '%s' with host '%s'" % (group, resource.node))
            output[group] = {'hosts': [resource.node], 'vars': dict()}
        else:
            LOGGER.debug("Adding host '%s' to group '%s'" % (resource.node, group))
            output[group]['hosts'].append(resource.node)

    print(to_json(output))
    sys.exit(0)

if __name__ == '__main__':
    main()
