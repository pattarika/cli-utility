from __future__ import annotations

from pathlib import Path
from time import perf_counter

from command import delivery_config as dc
from command import security as sec
from utils import _logging as lg
from utils.parser import AkamaiParser as Parser


if __name__ == '__main__':
    Path('output').mkdir(parents=True, exist_ok=True)
    start_time = perf_counter()
    args = Parser.get_args()
    logger = lg.setup_logger(args)

    if args.command == 'delivery-config':
        Path('output/delivery-config').mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'behavior':
            dc.get_property_all_behaviors(args, logger=logger)
        elif args.subcommand == 'custom-behavior':
            dc.get_custom_behavior(args, logger=logger)
        else:
            dc.main(args, logger=logger)

    if args.command == 'security':
        Path('output/security').mkdir(parents=True, exist_ok=True)
        if args.subcommand == 'hostname':
            sec.audit_hostname(args, logger)
        else:
            sec.list_config(args, logger)

    end_time = lg.log_cli_timing(start_time)
    logger.info(end_time)
