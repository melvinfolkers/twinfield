import logging
import os

import pandas as pd
from .run_settings import get_twinfield_settings
from . import functions, modules, transform
from .functions import select_office
from .modules import read_offices
from .transform import maak_samenvatting
from tqdm import tqdm


def scoping_offices(offices):
    login = get_twinfield_settings()
    all_offices = read_offices(login)

    if len(offices):
        logging.info("{} administraties beschikbaar".format(len(all_offices)))
        scoping = all_offices[all_offices.name.isin(offices)]
        logging.info("{} administraties geselecteerd".format(len(scoping)))
    else:
        logging.info(f"alle {len(all_offices)} administraties in scope.")
        scoping = all_offices

    return scoping

def set_update(run_params, offices, module):

    df = functions.import_files(run_params, module)
    if not len(df):
        return offices
    succes = df["administratienaam"].unique().tolist()
    offices = offices[~offices.name.isin(succes)]

    return offices

def set_rerun(run_params, module):

    offices = scoping_offices(run_params.offices)

    if run_params.offices:
        return offices[offices.name.isin(run_params.offices)]

    df = functions.import_files(run_params, module)
    try:
        errors = df[~df.faultcode.isna()]["administratienummer"].tolist()
    except:
        logging.info("no errors")
        return offices.head(n=0)

    logging.info(f"{len(errors)} administraties zijn niet goed geimporteerd.")
    rerun = offices[offices.index.isin(errors)]

    return rerun


def import_all(run_params):

    offices = scoping_offices(run_params.offices)

    if "030_1" in run_params.modules:
        pull_transactions(offices, run_params)
        maak_samenvatting(run_params)

    if "040_1" in run_params.modules:
        pull_consolidatie(offices, run_params)

    if "100" in run_params.modules:
        offices = scoping_offices(run_params.offices)
        if run_params.rerun:
            offices = set_rerun(run_params, run_params.module_names.get("100"))
            pull_openstaande_debiteuren(offices, run_params)
            return logging.info('rerun afgerond!')
        else:
            offices = set_update(run_params, offices, run_params.module_names.get("100"))

        pull_openstaande_debiteuren(offices, run_params)
        offices = set_rerun(run_params, run_params.module_names.get("100"))
        pull_openstaande_debiteuren(offices, run_params)


    if "200" in run_params.modules:
        offices = scoping_offices(run_params.offices)

        if run_params.rerun:
            offices = set_rerun(run_params, "openstaande_crediteuren")
            pull_openstaande_crediteuren(offices, run_params)
            return logging.info('rerun afgerond!')
        else:
            offices = set_update(run_params, offices, run_params.module_names.get("200"))

        pull_openstaande_crediteuren(offices, run_params)
        offices = set_rerun(run_params, run_params.module_names.get("200"))
        pull_openstaande_crediteuren(offices, run_params)

def add_metadata(df, office, rows):

    df["administratienaam"] = rows["name"]
    df["administratienummer"] = office
    df["wm"] = rows["shortname"]

    return df


def pull_openstaande_debiteuren(offices, run_params):
    logging.info("\t" + 3 * "-" + "openstaande debiteuren" + 3 * "-")
    for office, rows in tqdm(offices.iterrows(), desc="administraties", total=offices.shape[0]):
        #logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
        # refresh login (session id) for every run

        login = get_twinfield_settings()
        select_office(office, param=login)
        periodes = functions.periods_from_start(run_params)
        period = request_openstaande_debiteuren_data(login, run_params, periodes)
        period = add_metadata(period, office, rows)
        period.to_pickle(os.path.join(run_params.pickledir, "{}_openstaande_debiteuren.pkl".format(office)))


def pull_openstaande_crediteuren(offices, run_params):
    logging.info("\t" + 3 * "-" + "openstaande crediteuren" + 3 * "-")

    for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
        # logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
        # refresh login (session id) for every run

        login = get_twinfield_settings()
        select_office(office, param=login)
        periodes = functions.periods_from_start(run_params)
        period = request_openstaande_crediteuren_data(login, run_params, periodes)
        period = add_metadata(period, office, rows)
        period.to_pickle(os.path.join(run_params.pickledir, "{}_openstaande_crediteuren.pkl".format(office)))


def pull_consolidatie(offices, run_params):

    for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
        logging.debug("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
        # refresh login (session id) for every run

        login = get_twinfield_settings()
        select_office(office, param=login)
        periodes = functions.period_groups(window="year")
        period = request_consolidatie_data(login, run_params, periodes)
        period = add_metadata(period, office, rows)
        period.to_pickle(os.path.join(run_params.pickledir, "{}_consolidatie.pkl".format(office)))


def pull_transactions(offices, run_params):
    for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
        logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")

        # refresh login (session id) for every run

        login = get_twinfield_settings()

        select_office(office, param=login)

        periodes = functions.period_groups(window="two_months")

        period = request_transaction_data(login, run_params, periodes)

        period = add_metadata(period, office, rows)

        period.to_pickle(os.path.join(run_params.pickledir, "{}_transactions.pkl".format(office)))


def request_transaction_data(login, run_params, periodes):
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_030_1(login, run_params, periode)
        batch = transform.format_030_1(batch)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_consolidatie_data(login, run_params, periodes):
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_040_1(login, run_params, periode)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_openstaande_debiteuren_data(login,run_params, periodes):
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_100(login, run_params, periode)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_openstaande_crediteuren_data(login, run_params, periodes):
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_200(login, run_params, periode)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data
