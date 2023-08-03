from __future__ import annotations

import argparse
import logging
import os
import sys

import rich_argparse as rap


class CLIFormatter(logging.Formatter):
    """
    Include folder in the log
    """
    def format(self, record):
        record.filename = os.path.join(os.path.basename(os.path.dirname(record.pathname)), os.path.basename(record.filename))
        return super().format(record)


class OnelineArgumentFormatter(rap.ArgumentDefaultsRichHelpFormatter):
    def __init__(self, prog, max_help_position=30, **kwargs):
        super().__init__(prog, **kwargs)
        self._max_help_position = max_help_position

    def print_usage(self, file=None):
        if file is None:
            file = sys.stdout
        self._print_message(self.usage, file, False)

    def _format_usage(self, usage, actions, groups, prefix):
        # Do not include the default usage line
        return 'Usage:'


class CustomHelpFormatter(rap.RichHelpFormatter):
    def __init__(self, prog, indent_increment=2, max_help_position=30, width=None):
        super().__init__(prog, indent_increment, max_help_position, width)


class AkamaiParser(CustomHelpFormatter, argparse.ArgumentParser):
    def __init__(self, prog):
        super().__init__(prog,
                         max_help_position=30)

    '''
    def format_usage(self):
        is_subcommand = self._is_subcommand()
        if is_subcommand:
            return ''
        return super().format_usage()

    def _is_subcommand(self):
        return hasattr(self, '_subparsers') and self._subparsers is not None
    '''

    @classmethod
    def get_args(cls):
        parser = argparse.ArgumentParser(prog='Akamai CLI utility',
                                         formatter_class=AkamaiParser,
                                         conflict_handler='resolve', add_help=True,
                                         usage='Various akamai utilities to facilitate day to day work')
        parser.add_argument('-a', '--accountkey',
                            metavar='accountkey', type=str, dest='account_switch_key',
                            help='account switch key (Akamai Internal Only)')
        subparsers = parser.add_subparsers(title='Available commands', metavar='', dest='command')
        cls.all_command(subparsers)
        return parser.parse_args()

    @classmethod
    def create_main_command(cls, subparsers, name, help,
                            required_arguments=None,
                            optional_arguments=None,
                            subcommands=None,
                            options=None):

        action = subparsers.add_parser(name=name,
                                       help=help,
                                       add_help=True,
                                       formatter_class=OnelineArgumentFormatter)
        action.description = help  # Set the subcommand's help message as the description
        action.usage = f'%(prog)s {name} [options]'  # Set a custom usage format

        if subcommands:
            subparsers = action.add_subparsers(title=name, metavar='', dest='subcommand')
            for subcommand in subcommands:
                subcommand_name = subcommand['name']
                subcommand_help = subcommand['help']
                subcommand_required = subcommand.get('required_arguments', None)
                subcommand_optional = subcommand.get('optional_arguments', None)
                cls.create_main_command(subparsers, subcommand_name, subcommand_help,
                                        subcommand_required,
                                        subcommand_optional,
                                        subcommands=subcommand.get('subcommands', None))

        cls.add_arguments(action, required_arguments, optional_arguments)

        if options:
            options_group = action.add_argument_group('Options')
            for option in options:
                option_name = option['name']
                del option['name']
                try:
                    action_value = option['action']
                    del option['action']
                    options_group.add_argument(f'--{option_name}', action=action_value, **option)
                except KeyError:
                    options_group.add_argument(f'--{option_name}', metavar='', **option)
        return action

    @classmethod
    def add_mutually_exclusive_group(cls, action, argument, conflicting_argument):

        group = action.add_mutually_exclusive_group()
        group.add_argument(argument['name'], help=argument['help'], nargs='+')

        # Add the conflicting argument to the group as a mutually exclusive argument
        conflicting_argument_help = [arg['help'] for arg in argument if arg['name'] == conflicting_argument]
        group.add_argument(conflicting_argument, help=conflicting_argument_help, nargs='+')

    @classmethod
    def add_arguments(cls, action, required_arguments=None, optional_arguments=None):

        if required_arguments:
            required = action.add_argument_group('Required Arguments')
            for arg in required_arguments:
                name = arg['name']
                del arg['name']
                try:
                    action_value = arg['action']
                    del arg['action']
                    required.add_argument(f'--{name}', action=action_value, **arg)
                except KeyError:
                    required.add_argument(f'--{name}', metavar='', **arg)

        if optional_arguments:
            optional = action.add_argument_group('Optional Arguments')
            for arg in optional_arguments:
                if arg['name'] == '--group-id':
                    cls.add_mutually_exclusive_group(action, arg, '--property-id')
                elif arg['name'] == '--property-id':
                    cls.add_mutually_exclusive_group(action, arg, '--group-id')
                else:
                    name = arg['name']
                    del arg['name']
                    try:
                        action_value = arg['action']
                        del arg['action']
                        optional.add_argument(f'--{name}', required=False, action=action_value, **arg)
                    except KeyError:
                        optional.add_argument(f'--{name}', metavar='', required=False, **arg)

            optional.add_argument('-c', '--syntax-css', action='store', default='vs', help=argparse.SUPPRESS)
            optional.add_argument('-p', '--print-width', action='store_true', help=argparse.SUPPRESS)
            optional.add_argument('-v', '--verbose', action='store_true', help=argparse.SUPPRESS)
            optional.add_argument('--log-level',
                                  choices=['debug', 'info', 'warning', 'error', 'critical'],
                                  default='info',
                                  help='Set the log level. Too noisy, increase to warning',
                                 )

            optional.add_argument('-e', '--edgerc',
                                  metavar='', type=str, dest='section',
                                  help='location of the credentials file [$AKAMAI_EDGERC]')
            optional.add_argument('-s', '--section',
                                  metavar='', type=str, dest='section',
                                  help='section of the credentials file [$AKAMAI_EDGERC_SECTION]')

    @classmethod
    def all_command(cls, subparsers):
        actions = {}
        dc_sc = [{'name': 'behavior',
                  'help': 'list all behaviors on the property',
                  'required_arguments': [{'name': 'property', 'help': 'property name'}],
                  'optional_arguments': [{'name': 'version', 'help': 'version'},
                                         {'name': 'remove-tag', 'help': 'ignore JSON/XML tags from comparison', 'nargs': '+'}]},
                 {'name': 'custom-behavior',
                  'help': 'list custom behavior on the account',
                  'optional_arguments': [{'name': 'id', 'help': 'behaviorId', 'nargs': '+'},
                                         {'name': 'namecontains', 'help': 'behavior name contains keyword search'},
                                         {'name': 'hidexml', 'help': 'use this argument to hide XML result from the terminal', 'action': 'store_false'},
                                         {'name': 'lineno', 'help': 'show line number', 'action': 'store_true'}]},
                 ]
        actions['delivery-config'] = cls.create_main_command(
            subparsers,
            'delivery-config',
            help='many things you may need to (know about/check on/perform on) configs on the account',
            optional_arguments=[{'name': 'summary', 'help': 'only show account summary', 'action': 'store_true'},
                                {'name': 'output', 'help': 'output filename.extension ie akamai.xlsx'},
                                {'name': 'concurrency', 'help': 'increase concurrency to X', 'default': 1},
                                {'name': 'show', 'help': 'automatically launch Microsoft Excel after (Mac OS Only)', 'action': 'store_true'},
                                {'name': 'behavior', 'help': 'behaviors you want to audit on the property', 'nargs': '+'},
                                {'name': 'group-id', 'help': 'provide at least one groupId without prefix grp_ ', 'nargs': '+'},
                                {'name': 'property', 'help': 'provide at least one propertyId without prefix prp_ ', 'nargs': '+'},
                                ],
            subcommands=dc_sc,
            options=None)
        sec_sc = [{'name': 'hostname',
                  'help': 'audit hostnames not yet assigned to the security configurations',
                  'optional_arguments': [{'name': 'group-id', 'help': 'group-id', 'nargs': '+'},
                                         {'name': 'output', 'help': 'override excel output file (.xlsx)'},
                                         {'name': 'no-show', 'help': 'automatically open excel', 'action': 'store_true'}]
                  }]
        actions['security'] = cls.create_main_command(
                            subparsers,
                            'security',
                            help='collect detail about security configuration',
                            subcommands=sec_sc,
                            required_arguments=[{'name': 'config', 'help': 'security config name', 'nargs': '+'}],
                            optional_arguments=[{'name': 'version', 'help': 'security config version'},
                                                {'name': 'group-id', 'help': 'group-id', 'nargs': '+'},
                                                {'name': 'output', 'help': 'override excel output file (.xlsx)'},
                                                {'name': 'no-show', 'help': 'automatically open compare report in browser', 'action': 'store_true'}])
        return actions
