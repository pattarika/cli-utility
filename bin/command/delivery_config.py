from __future__ import annotations

import sys
from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd
from ak_api.identity_access import IdentityAccessManagement
from ak_utils import cpcode as cp
from ak_utils import papi as p
from ak_utils import siteshield as ss
from pandarallel import pandarallel
from rich import print_json
from rich.console import Console
from rich.syntax import Syntax
from tabulate import tabulate
from utils import dataframe
from utils import files
from yaspin import yaspin


pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_rows', None)


def main(args, logger):
    '''
    python bin/akamai-utility.py -a 1-5BYUG1 delivery-config --show \
    --group-id 116576 66711 215385 \
    --behavior cpCode origin mPulse allowPost
    '''
    if args.group_id and args.property:
        sys.exit(logger.error('Please use either --group-id or --property, not both'))

    concurrency = int(args.concurrency) if args.concurrency else None
    # display full account name
    iam = IdentityAccessManagement(args.account_switch_key, logger=logger)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = iam.show_account_summary(account)
    account_folder = f'output/delivery-config/{account}'
    Path(account_folder).mkdir(parents=True, exist_ok=True)
    filepath = f'{account_folder}/account_detail.xlsx' if args.output is None else f'output/{args.output}'

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, logger=logger)
    cpc = cp.CpCodeWrapper(account_switch_key=args.account_switch_key)
    if args.behavior:
        original_behaviors = [x.lower() for x in args.behavior]
    sheet = {}
    if args.property:
        all_properties = []
        for property in args.property:
            status, resp = papi.search_property_by_name(property)
            # print_json(data=resp)
            if status != 200:
                logger.info(f'property {property:<50} not found')
                break
            else:
                logger.debug(f'{papi.group_id} {papi.contract_id} {papi.property_id}')
                stg, prd = papi.property_version(resp)
                all_properties.append((papi.account_id, papi.contract_id, papi.group_id, property, papi.property_id, stg, prd))

        properties_df = pd.DataFrame(all_properties, columns=['accountId', 'contractId', 'groupId', 'propertyName', 'propertyId', 'stagingVersion', 'productionVersion'])
        properties_df['groupName'] = properties_df['groupId'].apply(lambda x: papi.get_group_name(x))
        properties_df['latestVersion'] = properties_df['propertyId'].apply(lambda x: papi.get_property_version_latest(x)['latestVersion'])
        properties_df['assetId'] = properties_df['propertyId'].apply(lambda x: papi.get_property_version_latest(x)['assetId'])

        logger.debug('Collecting hostname')
        properties_df['hostname'] = properties_df[['propertyId']].apply(lambda x: papi.get_property_hostnames(*x), axis=1)
        properties_df['hostname_count'] = properties_df['hostname'].str.len()
        # show one hostname per list and remove list syntax
        properties_df['hostname'] = properties_df[['hostname']].apply(lambda x: ',\n'.join(x.iloc[0]) if not x.empty else '', axis=1)

        logger.debug('Collecting updatedDate')
        properties_df['updatedDate'] = properties_df.apply(lambda row: papi.get_property_version_detail(row['propertyId'], row['latestVersion'], 'updatedDate'), axis=1)

        logger.debug('Collecting productId')
        properties_df['productId'] = properties_df.apply(lambda row: papi.get_property_version_detail(
            row['propertyId'], int(row['productionVersion']) if pd.notnull(row['productionVersion']) else row['latestVersion'], 'productId'), axis=1)

        logger.debug('Collecting ruleFormat')
        properties_df['ruleFormat'] = properties_df.apply(lambda row: papi.get_property_version_detail(
            row['propertyId'], int(row['productionVersion']) if pd.notnull(row['productionVersion']) else row['latestVersion'], 'ruleFormat'), axis=1)

        logger.debug('Collecting property url')
        properties_df['propertyURL'] = properties_df.apply(lambda row: papi.property_url(row['assetId'], row['groupId']), axis=1)
        properties_df['url'] = properties_df.apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['propertyURL'], row['propertyName']), axis=1)

        # del properties_df['propertyName']  # drop original column
        properties_df = properties_df.rename(columns={'url': 'propertyName(hyperlink)'})  # show column with hyperlink instead
        properties_df = properties_df.rename(columns={'groupName_url': 'groupName'})  # show column with hyperlink instead
        properties_df = properties_df.sort_values(by=['groupName', 'propertyName'])
        properties_df['ruletree'] = properties_df.apply(
                        lambda row: papi.get_property_ruletree(row['propertyId'], int(row['productionVersion'])
                                                            if pd.notnull(row['productionVersion']) else row['latestVersion']), axis=1)
        # properties.loc[pd.notnull(properties['cpcode_unique_value']) & (properties['cpcode_unique_value'] == ''), 'cpcode'] = '0'

        if args.behavior:
            pandarallel.initialize(progress_bar=False, verbose=0)
            properties_df = papi.check_behavior(original_behaviors, properties_df, cpc)

        # columns = ['accountId', 'groupId', 'groupName',
        columns = ['propertyName', 'propertyId', 'latestVersion', 'stagingVersion', 'productionVersion',
                   'updatedDate', 'productId', 'ruleFormat', 'hostname_count', 'hostname']
        properties_df['propertyId'] = properties_df['propertyId'].astype(str)

        if args.behavior:
            columns.extend(sorted(original_behaviors))
            if 'cpcode' in original_behaviors:
                columns.remove('cpcode')
                columns.extend(['cpcode_count', 'cpcode', 'cpcode_name'])
            if 'origin' in original_behaviors:
                columns.remove('origin')
                columns.extend(['origin_count', 'origin'])

        columns.extend(['propertyName(hyperlink)'])
        properties_df = properties_df[columns].copy()
        properties_df = properties_df.reset_index(drop=True)
        sheet['properties'] = properties_df

    else:
        # build group structure as displayed on control.akamai.com
        logger.warning('Collecting properties summary for the account')
        if args.group_id is None:
            logger.critical(' 200 properties take ~  7 minutes')
            logger.critical(' 800 properties take ~ 30 minutes')
            logger.critical('2200 properties take ~ 80 minutes')
            logger.critical('please consider using --group-id to reduce total properties')

        with yaspin() as sp:
            allgroups_df, columns = papi.account_group_summary()
        if allgroups_df is None:
            sys.exit()
        else:
            allgroups_df['groupId'] = allgroups_df['groupId'].astype(str)  # change groupId to str before load into excel

        if args.group_id:
            groups = args.group_id
            group_df = allgroups_df[allgroups_df['groupId'].isin(groups)].copy()
            group_df = group_df.reset_index(drop=True)
        else:
            group_df = allgroups_df[allgroups_df['propertyCount'] > 0].copy()
            group_df = group_df.reset_index(drop=True)

        if not group_df.empty:
            print()
            columns.remove('groupName')
            print(tabulate(group_df[columns], headers=columns, showindex=True, tablefmt='github'))

        # warning for large account
        if not args.group_id:
            print()
            if group_df.shape[0] > 0:
                logger.warning(f'total groups {allgroups_df.shape[0]}, only {group_df.shape[0]} groups have properties.')
            total = allgroups_df['propertyCount'].sum()
            all_groups = group_df['groupId'].unique().tolist()
            modified_list = [word for word in all_groups]
            all_groups = ' '.join(modified_list)
            logger.warning(f'--group-id {all_groups}')

        if args.summary is True:
            sheet = {}
            sheet['account_summary'] = group_df
            files.write_xlsx(filepath, sheet, freeze_column=1) if not group_df.empty else None
            files.open_excel_application(filepath, args.show, group_df)
            return None

        # collect properties detail for all groups
        properties_df = pd.DataFrame()
        if group_df.empty:
            logger.info('no property to collect.')
        else:
            print()
            total = group_df['propertyCount'].sum()
            if total == 0:
                logger.info('no property to collect.')
            else:
                logger.critical('collecting properties ...')
                prop0 = perf_counter()
                account_properties = papi.property_summary(group_df, concurrency)
                if len(account_properties) > 0:
                    df = pd.concat(account_properties, axis=0)
                    df['ruletree'] = df.parallel_apply(
                        lambda row: papi.get_property_ruletree(int(row['propertyId']),
                                                                int(row['productionVersion'])
                                                                if pd.notnull(row['productionVersion'])
                                                                else row['latestVersion']), axis=1)
                    df = df.rename(columns={'url': 'propertyName(hyperlink)'})  # show column with hyperlink instead
                    df = df.rename(columns={'groupName_url': 'groupName'})  # show column with hyperlink instead
                    df = df.sort_values(by=['groupName', 'propertyName'])
                    prop1 = perf_counter()
                    msg = 'collecting properties'
                    logger.critical(f'{msg:<40} finished  {prop1 - prop0:.2f} seconds')

                    columns = ['accountId', 'groupId', 'groupName', 'propertyName', 'propertyId',
                               'latestVersion', 'stagingVersion', 'productionVersion', 'updatedDate',
                               'productId', 'ruleFormat', 'hostname_count', 'hostname', 'ruletree']

                    if args.behavior:
                        print()
                        logger.critical('collecting behavior ...')
                        t0 = perf_counter()
                        df = papi.check_behavior(original_behaviors, df, cpc)
                        columns.extend(sorted(original_behaviors))
                        if 'cpcode' in original_behaviors:
                            columns.remove('cpcode')
                            columns.extend(['cpcode_count', 'cpcode', 'cpcode_name'])
                        if 'origin' in original_behaviors:
                            columns.remove('origin')
                            columns.extend(['origin_count', 'origin'])
                        msg = 'collecting behaviors'
                        t1 = perf_counter()
                        logger.critical(f'{msg:<40} finished  {t1 - t0:.2f} seconds')

                    columns.extend(['propertyName(hyperlink)'])
                    df['propertyId'] = df['propertyId'].astype(str)  # for excel format
                    df = df[columns].copy()
                    df = df.reset_index(drop=True)
                    df['hostname'] = df[['hostname']].parallel_apply(lambda x: dataframe.split_elements_newline(x[0])
                                                        if len(x[0]) > 0 else '', axis=1)

                    columns.remove('ruletree')
                    properties_df = df[columns]
                    sheet['properties'] = properties_df

        # add hyperlink to groupName column
        print()
        t0 = perf_counter()
        logger.critical('collecting hyperlink ...')
        if args.group_id is not None:
            sheet['group_filtered'] = add_group_url(group_df, papi)
        if not allgroups_df.empty:
            sheet['account_summary'] = add_group_url(allgroups_df, papi)
        msg = 'collecting hyperlink'
        t1 = perf_counter()
        logger.critical(f'{msg:<40} finished  {t1 - t0:.2f} seconds')

    logger.debug(properties_df.columns.values.tolist()) if not properties_df.empty else None
    print()
    if 'custombehavior' in properties_df.columns.values.tolist():
        logger.info('checking custom behavior ...')
        status, response = papi.list_custom_behaviors()
        if status == 200:
            custom_behavior_df = pd.DataFrame(response)
            columns = custom_behavior_df.columns.values.tolist()
            if columns:
                for x in ['xml', 'updatedDate', 'sharingLevel', 'description', 'status', 'updatedByUser', 'approvedByUser']:
                    columns.remove(x)
                sheet['custom_behavior'] = custom_behavior_df

    files.write_xlsx(filepath, sheet, freeze_column=1) if not properties_df.empty else None
    files.open_excel_application(filepath, args.show, properties_df)
    columns.append('ruletree')
    properties_with_ruletree_df = df[columns]
    return properties_with_ruletree_df


def get_property_all_behaviors(args, logger):
    '''
    python bin/akamai-utility.py -a 1-5BYUG1 delivery-config behavior \
        --property XXXXX \
        --group-id  116576 66711 215385 \
        --behavior cpCode origin mPulse allowPost \
        --show
    '''
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, logger=logger)
    status, resp = papi.search_property_by_name(args.property)
    if status == 200:
        version = int(args.version) if args.version else None
        if version is None:
            stg, prd = papi.property_version(resp)
            version = prd
    else:
        sys.exit(logger.error(resp))

    tree_status, json_response = papi.property_ruletree(papi.property_id, version, args.remove_tag)
    if tree_status == 200:
        # print_json(data=json_response)
        behaviors = papi.get_property_behavior(json_response['rules'])
        unique_behaviors = sorted(list(set(behaviors)))
        logger.debug(unique_behaviors)
        behaviors_cli = '[' + ' '.join(unique_behaviors) + ']'

        if len(unique_behaviors) > 0:
            logger.info('Behaviors founded')
            logger.warning(behaviors_cli)
            print()
            logger.critical('You can use the list to compare behavior between 2 delivery configs')
            logger.info('>> akamai util diff behavior --property A B --behavior allHttpInCacheHierarchy allowDelete allowOptions allowPatch allowPost')


def get_property_advanced_behavior(args, logger):
    '''
    python bin/akamai-utility.py -a AANA-2NUHEA delivery-config metadata --property xxx yyy --advBehavior
    '''

    if args.version and len(args.property) > 1:
        sys.exit(logger.error('If --version is specified, we can lookup one property'))

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, logger=logger)
    iam = IdentityAccessManagement(args.account_switch_key, logger=logger)
    account = iam.search_account_name(value=args.account_switch_key)[0]
    account = iam.show_account_summary(account)
    account_folder = f'output/delivery-config/{account}'
    Path(account_folder).mkdir(parents=True, exist_ok=True)
    filepath = f'{account_folder}/metadata.xlsx'

    sheet = {}
    options = []
    columns = ['property', 'type', 'xml', 'path']
    for property in args.property:
        print()
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.critical(f'property {property:<50} not found')
            break
        else:
            stg, prd = papi.property_version(resp)
            version = prd
            if args.version:
                version = args.version
                logger.warning(f'lookup requested v{version}')
        property_name = f'{property}_v{version}'
        status, json = papi.property_ruletree(papi.property_id, version)
        if status != 200:
            sys.exit(logger.error(f'{json["title"]}. please provide correct version'))

        papi_rules = p.PapiWrapper(account_switch_key=args.account_switch_key, logger=logger)
        behaviors = papi_rules.collect_property_behavior(property_name, json['rules'])
        if len(behaviors) > 0:
            db = pd.DataFrame(behaviors)
            db = db[db['name'] == 'advanced'].copy()
        if db.empty:
            logger.info('advanced behavior not found')
        else:
            db = db.reset_index(drop=True)
            db = db.rename(columns={'json_or_xml': 'xml'})
            db['type'] = 'advBehavior'
            db = db[columns]
            options.append(db)

        criteria = papi_rules.collect_property_criteria(property_name, json['rules'])
        if len(criteria) > 0:
            dc = pd.DataFrame(criteria)
            dc = dc[dc['name'] == 'matchAdvanced'].copy()
        if dc.empty:
            logger.info('advanced match not found')
        else:
            dc = dc.reset_index(drop=True)
            dc = dc.rename(columns={'json_or_xml': 'xml'})
            dc['type'] = 'advMatch'
            dc = dc[columns]
            options.append(dc)

        adv_override = papi.get_property_advanced_override(papi.property_id, version)
        if adv_override is not None:
            do = pd.DataFrame.from_dict({property_name: adv_override}, orient='index', columns=['xml'])
            if do.empty:
                logger.info('advanced override not found')
            else:
                do.index.name = 'property'
                do = do.reset_index()
                do['type'] = 'advOverride'
                do['path'] = ''
                do = do[columns]
                options.append(do)

    if len(options) > 0:
        df = pd.concat(options).reset_index(drop=True)
        if args.hidexml is True:
            for property, type, path, xml_string in df[['property', 'type', 'path', 'xml']].values:
                print()
                logger.warning(f'{type:<20} {property:<70} {path}')
                syntax = Syntax(xml_string, 'xml', theme='solarized-dark', line_numbers=args.lineno)
                console = Console()
                console.print(syntax)
        sheet['advancedXML'] = df
    if sheet:
        print()
        files.write_xlsx(filepath, sheet, show_index=False)
        files.open_excel_application(filepath, not args.no_show, df)


def get_property_advanced_override(args, logger):
    '''
    python bin/akamai-utility.py -a AANA-2NUHEA delivery-config metadata --property xxx yyy --advOverride
    '''
    if args.version and len(args.property) > 1:
        sys.exit(logger.error('If --version is specified, we can lookup one property'))

    papi = p.PapiWrapper(account_switch_key=args.account_switch_key)
    console = Console()
    property_dict = {}
    property_list = []
    sheet = {}
    print()
    logger.warning('Searching for advanced override ...')
    for property in args.property:
        status, resp = papi.search_property_by_name(property)
        if status != 200:
            logger.info(f'property {property:<50} not found')
            break
        else:
            stg, prd = papi.property_version(resp)
            version = prd
            if args.version:
                version = args.version
                logger.critical(f'lookup requested v{version}')

        _ = papi.get_property_ruletree(papi.property_id, version)

        title = f'{property}_v{version}'
        adv_override = papi.get_property_advanced_override(papi.property_id, version)
        if adv_override:
            property_dict[title] = [adv_override]
            property_list.append(property_dict)
            sheet_df = pd.DataFrame.from_dict({title: adv_override}, orient='index', columns=['xml'])
            sheet_df.index.name = 'property'
            sheet_df['type'] = 'advOverride'

            sheet_df = sheet_df.reset_index()

            sheet_name = f'{property}_v{version}'
            if len(sheet_name) > 26:
                sheet_name = f'{papi.property_id}_v{version}'
            sheet[sheet_name] = sheet_df

            if args.hidexml is True:
                syntax = Syntax(adv_override, 'xml', theme='solarized-dark', line_numbers=args.lineno)
                console.print(syntax)
        else:
            logger.critical(f'{title:<50} no advanced override')

    if sheet:
        print()
        filepath = 'advancedOverride.xlsx'
        files.write_xlsx(filepath, sheet, show_index=True)
        files.open_excel_application(filepath, True, sheet_df)


def get_custom_behavior(args, logger):
    papi = p.PapiWrapper(account_switch_key=args.account_switch_key, logger=logger)
    status, response = papi.list_custom_behaviors()
    if status == 200:
        if len(response) == 0:
            sys.exit(logger.info('No custome behavior found'))
        df = pd.DataFrame(response)
        columns = df.columns.values.tolist()
        for x in ['xml', 'updatedDate', 'sharingLevel', 'description', 'status', 'updatedByUser', 'approvedByUser']:
            columns.remove(x)

        if args.id:
            df = df[df['behaviorId'].isin(args.id)].copy()

        if args.namecontains:
            df = df[df['name'].str.contains(args.namecontains)].copy()

        print()
        if df.empty:
            logger.warning('No custom behavior found based on your search')
        else:
            df = df.sort_values(by='name')
            df = df.reset_index(drop=True)

        if args.hidexml is True:
            for behaviorId, name, xml_string in df[['behaviorId', 'name', 'xml']].values:
                print()
                logger.warning(f'{behaviorId:<15} "{name}"')
                syntax = Syntax(xml_string, 'xml', theme='solarized-dark', line_numbers=args.lineno)
                console = Console()
                console.print(syntax)
        else:
            if not df.empty:
                print(tabulate(df[columns], headers=columns, tablefmt='simple'))
                print()
                logger.warning('remove --hidexml to show XML')


# BEGIN helper method
def load_config_from_xlsx(papi, filepath: str, sheet_name: str | None = None, filter: str | None = None, logger=None):
    '''
    excel must have header rows
    '''
    df = pd.read_excel(f'{filepath}', sheet_name=sheet_name, index_col=None)
    if filter:
        mask = np.column_stack([df[col].astype(str).str.contains(fr'{filter}', na=False) for col in df])
        df = df.loc[mask.any(axis=1)]
    df['stagingVersion'] = df['stagingVersion'].astype(int)
    df['productionVersion'] = df['productionVersion'].astype(int)
    if 'activationId' in df.columns.values.tolist():
        df['activationId'] = df['activationId'].astype(int)

    df['url'] = df.apply(lambda row: papi.property_url(row['assetId'], row['groupId']), axis=1)

    columns = ['propertyId', 'propertyName', 'stagingVersion', 'productionVersion']
    if 'activationId' in df.columns.values.tolist():
        columns.append('activationId')
    df = df[columns].copy()
    logger.info(f'Original Data from Excel\n{df}')
    return df[columns]


def add_group_url(df: pd.DataFrame, papi) -> pd.DataFrame:
    pandarallel.initialize(progress_bar=False, nb_workers=5, verbose=0)
    df['accountId'] = papi.account_switch_key
    df['groupURL'] = df.parallel_apply(lambda row: papi.group_url(row['groupId']), axis=1)
    df['groupName_url'] = df.parallel_apply(lambda row: files.make_xlsx_hyperlink_to_external_link(row['groupURL'], row['propertyCount']) if row['propertyCount'] else '', axis=1)
    del df['groupURL']
    del df['propertyCount']
    df = df.rename(columns={'groupName_url': 'propertyCount'})  # show column with hyperlink instead
    summary_columns = ['accountId', 'contractId', 'groupId', 'groupName']
    if 'parentGroupId' in df.columns.values.tolist():
        summary_columns.extend(['parentGroupId', 'propertyCount'])
    else:
        summary_columns.extend(['propertyCount'])
    return df[summary_columns]
# END helper method
