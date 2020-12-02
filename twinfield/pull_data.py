import logging
import os

import pandas as pd
from . import functions, modules, transform
from .functions import select_office
from .modules import read_offices
from .transform import maak_samenvatting
from tqdm import tqdm
from .credentials import twinfield_login


def scoping_offices(offices, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    offices: list of offices
    login: login parameters (SessionParameters)


    Returns: dataframe of scoped offices
    -------

    """

    all_offices = read_offices(login)

    if len(offices):
        logging.info(f"{len(all_offices)} administraties beschikbaar")
        scoping = all_offices[all_offices.name.isin(offices)]
        logging.info(f"{len(scoping)} administraties geselecteerd")
    else:
        scoping = all_offices  # [:1]
        logging.info(f"alle {len(scoping)} administraties in scope.")

    return scoping


def set_update(run_params, offices, module) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)

    offices: list of offices
    module

    Returns: only offices that are not already imported to the temp directory
    -------

    """

    df = functions.import_files(run_params, module)
    if not len(df):
        return offices
    succes = df["administratienaam"].unique().tolist()
    offices = offices[~offices.name.isin(succes)]

    return offices


def set_rerun(run_params, module, login) -> pd.DataFrame:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)
    module: selected module
    login:  login parameters (SessionParameters)

    Returns: dataframe of offices that are not correctly imported
    -------

    """

    offices = scoping_offices(run_params.offices, login)

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


def import_all(run_params) -> None:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)

    Returns None. Runs the appropriate import script based on the selected module.
    -------

    """

    login = twinfield_login()
    offices = scoping_offices(run_params.offices, login)

    if "030_1" in run_params.modules:
        pull_transactions(offices, run_params, login)
        maak_samenvatting(run_params)

    if "040_1" in run_params.modules:
        pull_consolidatie(offices, run_params, login)

    if "100" in run_params.modules:

        if run_params.rerun:
            offices = set_rerun(run_params, run_params.module_names.get("100"), login)
            pull_openstaande_debiteuren(offices, run_params, login)
            return logging.info("rerun afgerond!")
        else:
            offices = set_update(run_params, offices, run_params.module_names.get("100"))

        pull_openstaande_debiteuren(offices, run_params, login)
        offices = set_rerun(run_params, run_params.module_names.get("100"), login)
        pull_openstaande_debiteuren(offices, run_params, login)

    if "200" in run_params.modules:

        if run_params.rerun:
            offices = set_rerun(run_params, "openstaande_crediteuren", login)
            pull_openstaande_crediteuren(offices, run_params, login)
            return logging.info("rerun afgerond!")
        else:
            offices = set_update(run_params, offices, run_params.module_names.get("200"))

        pull_openstaande_crediteuren(offices, run_params, login)
        offices = set_rerun(run_params, run_params.module_names.get("200"), login)
        pull_openstaande_crediteuren(offices, run_params, login)


def add_metadata(df, office, rows) -> pd.DataFrame:
    """

    Parameters
    ----------
    df: original dataframe containing records of selected module
    office: officode
    rows: row of dataframe containing office metadata

    Returns
    -------

    """

    df["administratienaam"] = rows["name"]
    df["administratienummer"] = office
    df["wm"] = rows["shortname"]

    return df


def pull_openstaande_debiteuren(offices, run_params, login) -> None:
    """

    Parameters
    ----------
    offices: selected offices to perform request
    run_params:  input parameters of script (set at start of script)
    login:  login parameters (SessionParameters)

    Returns None. exports the data for the module to a pickle file in the tmp directory
    -------

    """

    logging.info("\t" + 3 * "-" + "openstaande debiteuren" + 3 * "-")
    for office, rows in tqdm(offices.iterrows(), desc="administraties", total=offices.shape[0]):
        # logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
        # refresh login (session id) for every run

        select_office(office, param=login)
        periodes = functions.periods_from_start(run_params)
        period = request_openstaande_debiteuren_data(login, run_params, periodes)
        period = add_metadata(period, office, rows)
        period.to_pickle(
            os.path.join(run_params.pickledir, f"{office}_openstaande_debiteuren.pkl")
        )


def pull_openstaande_crediteuren(offices, run_params, login) -> None:
    """

    Parameters
    ----------
    offices: selected offices to perform request
    run_params:  input parameters of script (set at start of script)
    login:  login parameters (SessionParameters)

    Returns None. exports the data for the module to a pickle file in the tmp directory
    -------

    """

    logging.info("\t" + 3 * "-" + "openstaande crediteuren" + 3 * "-")
    for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
        # logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
        # refresh login (session id) for every run

        select_office(office, param=login)
        periodes = functions.periods_from_start(run_params)
        period = request_openstaande_crediteuren_data(login, run_params, periodes)
        period = add_metadata(period, office, rows)
        period.to_pickle(
            os.path.join(run_params.pickledir, f"{office}_openstaande_crediteuren.pkl")
        )


def pull_consolidatie(offices, run_params, login) -> None:
    """

    Parameters
    ----------
    offices: selected offices to perform request
    run_params:  input parameters of script (set at start of script)
    login:  login parameters (SessionParameters)

    Returns None. exports the data for the module to a pickle file in the tmp directory
    -------

    """

    for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
        logging.debug("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
        # refresh login (session id) for every run

        select_office(office, param=login)
        periodes = functions.period_groups(window="year")
        period = request_consolidatie_data(login, run_params, periodes)
        period = add_metadata(period, office, rows)
        period.to_pickle(os.path.join(run_params.pickledir, f"{office}_consolidatie.pkl"))


def pull_transactions(offices, run_params, login) -> None:
    """

    Parameters
    ----------
    offices: selected offices to perform request
    run_params:  input parameters of script (set at start of script)
    login:  login parameters (SessionParameters)

    Returns None. exports the data for the module to a pickle file in the tmp directory
    -------

    """

    for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
        logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")

        # refresh login (session id) for every run

        select_office(office, param=login)

        periodes = functions.period_groups(window="two_months")

        period = request_transaction_data(login, run_params, periodes)

        period = add_metadata(period, office, rows)

        period.to_pickle(
            os.path.join(
                run_params.pickledir, f"{office}_transactions_{run_params.jaar}.pkl"
            )
        )


def request_transaction_data(login, run_params, periodes) -> pd.DataFrame:
    """

    Parameters
    ----------
    login:  login parameters (SessionParameters)
    run_params:  input parameters of script (set at start of script)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_030_1(login, run_params, periode)
        batch = transform.format_030_1(batch)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_consolidatie_data(login, run_params, periodes) -> pd.DataFrame:
    """

    Parameters
    ----------
    login:  login parameters (SessionParameters)
    run_params:  input parameters of script (set at start of script)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """

    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_040_1(login, run_params, periode)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_openstaande_debiteuren_data(login, run_params, periodes) -> pd.DataFrame:
    """

    Parameters
    ----------
    login:  login parameters (SessionParameters)
    run_params:  input parameters of script (set at start of script)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_100(login, run_params, periode)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_openstaande_crediteuren_data(login, run_params, periodes) -> pd.DataFrame:
    """

    Parameters
    ----------
    login:  login parameters (SessionParameters)
    run_params:  input parameters of script (set at start of script)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_200(login, run_params, periode)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data
