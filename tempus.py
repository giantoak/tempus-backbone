import argparse
from util.load import _init_once
parser = argparse.ArgumentParser(description='''Tempus: Make sense of geospatial
temporal economic data''')

subparsers = parser.add_subparsers(help='sub-command help')
parser_init = subparsers.add_parser('init',
                                    help='Initialize Tempus from conf file.')

parser_init.set_defaults(func=_init_once)
args = parser.parse_args()
args.func(args)
