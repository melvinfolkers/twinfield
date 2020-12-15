import logging
import os

import pandas as pd
from . import functions, modules, transform
from .functions import select_office
from .modules import read_offices
from tqdm import tqdm
from .credentials import twinfield_login


def scoping_offices(offices: list, login) -> pd.DataFrame:
    """
    Parameters
    ----------
    offices: list
        list of offices
    login
        login parameters (SessionParameters)


    Returns
    -------
    scoping: pd.DataFrame
        dataframe of scoped offices
    """

    all_offices = read_offices(login)

    if offices:
        logging.info(f"{len(all_offices)} administraties beschikbaar")
        scoping = all_offices[all_offices["name"].isin(offices)]
        logging.info(f"{len(scoping)} administraties geselecteerd")
    else:
        scoping = all_offices
        logging.info(f"alle {len(scoping)} administraties in scope.")

    return scoping


def set_update(run_params, offices, module) -> pd.DataFrame:
    """
    Parameters
    ----------
    run_params
        input parameters of script (set at start of script)
    offices
        list of offices
    module

    Returns
    -------
    offices: pd.DataFrame
        Only offices that are not already imported to the temp directory
    """

    df = functions.import_files(run_params)
    if not len(df):
        return offices
    succes = df["administratienaam"].unique().tolist()
    offices = offices[~offices.name.isin(succes)]

    return offices


def set_rerun(run_params, login) -> pd.DataFrame:
    """
    Parameters
    ----------
    run_params
        input parameters of script (set at start of script)
    module
        selected module
    login
        login parameters (SessionParameters)

    Returns
    -------
    rerun: pd.DataFrame
        dataframe of offices that are not correctly imported
    """

    offices = scoping_offices(run_params.offices, login)
    df = functions.import_files(run_params)

    if "faultcode" in df.columns:
        errors = df.loc[~df["faultcode"].isna(), "administratienummer"].tolist()
    else:
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

    if run_params.module == "030_1":
        # pull_transactions(offices, run_params, login)
        pull_data_twinfield(offices, run_params, login)

    if run_params.module == "040_1":
        # pull_consolidatie(offices, run_params, login)
        pull_data_twinfield(offices, run_params, login)

    if run_params.module == "100":
        if run_params.rerun:
            offices = set_rerun(run_params, login)
            # pull_openstaande_debiteuren(offices, run_params, login)
            pull_data_twinfield(offices, run_params, login)
            return logging.info("rerun afgerond!")
        else:
            offices = set_update(run_params, offices, run_params.module_names.get("100"))

        # pull_openstaande_debiteuren(offices, run_params, login)
        pull_data_twinfield(offices, run_params, login)
        offices = set_rerun(run_params, login)
        # pull_openstaande_debiteuren(offices, run_params, login)
        pull_data_twinfield(offices, run_params, login)

    if run_params.module == "200":
        if run_params.rerun:
            offices = set_rerun(run_params, login)
            # pull_openstaande_crediteuren(offices, run_params, login)
            pull_data_twinfield(offices, run_params, login)
            return logging.info("rerun afgerond!")
        else:
            offices = set_update(run_params, offices, run_params.module_names.get("200"))

        # pull_openstaande_crediteuren(offices, run_params, login)
        pull_data_twinfield(offices, run_params, login)
        offices = set_rerun(run_params, login)
        # pull_openstaande_crediteuren(offices, run_params, login)
        pull_data_twinfield(offices, run_params, login)


def add_metadata(df, office, rows) -> pd.DataFrame:
    """
    Parameters
    ----------
    df
        original dataframe containing records of selected module
    office
        officode
    rows
        row of dataframe containing office metadata

    Returns
    -------
    df
    """
    df["administratienaam"] = rows["name"]
    df["administratienummer"] = office
    df["wm"] = rows["shortname"]

    return df


def pull_data_twinfield(offices, run_params, login) -> None:
    """
    Get data from Twinfield API and write to pickle files.
    Parameters
    ----------
    offices
        selected offices to perform request
    run_params
        input parameters of script (set at start of script)
    login
        login parameters (SessionParameters)

    Returns
    -------
    None, exports the data for the module to a pickle file in the /tmp directory.
    """
    for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
        logging.debug(f"\t {3 * '-'} {rows['shortname']} {3 * '-'}")
        # refresh login (session id) for every run
        select_office(office, param=login)
        if run_params.module in ["100", "200"]:
            periodes = functions.periods_from_start(run_params)
            batch = request_openstaande_debiteuren_data(login, periodes)
        elif run_params.module in ["030_1", "040_1"]:
            periodes = functions.period_groups(window="year")
            batch = request_consolidatie_data(login, periodes, run_params.jaar)
        batch = add_metadata(batch, office, rows)
        batch.to_pickle(os.path.join(run_params.pickledir, f"{office}_{run_params.module}.pkl"))


# def pull_openstaande_debiteuren(offices, run_params, login) -> None:
#     """
#     Parameters
#     ----------
#     offices
#         selected offices to perform request
#     run_params
#         input parameters of script (set at start of script)
#     login
#         login parameters (SessionParameters)
#
#     Returns
#     -------
#     None. exports the data for the module to a pickle file in the tmp directory
#     """
#
#     logging.info("\t" + 3 * "-" + "openstaande debiteuren" + 3 * "-")
#     for office, rows in tqdm(offices.iterrows(), desc="administraties", total=offices.shape[0]):
#
#         select_office(office, param=login)
#         periodes = functions.periods_from_start(run_params)
#         period = request_openstaande_debiteuren_data(login, periodes)
#         period = add_metadata(period, office, rows)
#         period.to_pickle(os.path.join(run_params.pickledir, f"{office}_{run_params.module}.pkl"))
#
#
# def pull_openstaande_crediteuren(offices, run_params, login) -> None:
#     """
#
#     Parameters
#     ----------
#     offices: selected offices to perform request
#     run_params:  input parameters of script (set at start of script)
#     login:  login parameters (SessionParameters)
#
#     Returns None. exports the data for the module to a pickle file in the tmp directory
#     -------
#
#     """
#
#     logging.info("\t" + 3 * "-" + "openstaande crediteuren" + 3 * "-")
#     for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
#         # logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
#         # refresh login (session id) for every run
#
#         select_office(office, param=login)
#         periodes = functions.periods_from_start(run_params)
#         period = request_openstaande_crediteuren_data(run_params, periodes)
#         period = add_metadata(period, office, rows)
#         period.to_pickle(
#             os.path.join(run_params.pickledir, f"{office}_{run_params.module}.pkl")
#         )
#
#
# def pull_consolidatie(offices, run_params, login) -> None:
#     """
#
#     Parameters
#     ----------
#     offices: selected offices to perform request
#     run_params:  input parameters of script (set at start of script)
#     login:  login parameters (SessionParameters)
#
#     Returns None. exports the data for the module to a pickle file in the tmp directory
#     -------
#
#     """
#
#     for office, rows in tqdm(offices.iterrows(), total=offices.shape[0]):
#         logging.debug("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")
#         # refresh login (session id) for every run
#
#         select_office(office, param=login)
#         periodes = functions.period_groups(window="year")
#         period = request_consolidatie_data(login, periodes, run_params.jaar)
#         period = add_metadata(period, office, rows)
#         period.to_pickle(os.path.join(run_params.pickledir, f"{office}_{run_params.module}.pkl"))
#
#
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
        period = request_transaction_data(login, periodes, run_params.jaar)
        period = add_metadata(period, office, rows)
        period.to_pickle(os.path.join(run_params.pickledir, f"{office}_{run_params.module}.pkl"))


def request_transaction_data(login, periodes, jaar) -> pd.DataFrame:
    """

    Parameters
    ----------
    login:  login parameters (SessionParameters)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_module(login, periode, "030_1", jaar)
        batch = transform.format_030_1(batch)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_consolidatie_data(login, periodes, jaar) -> pd.DataFrame:
    """
    Parameters
    ----------
    login:  login parameters (SessionParameters)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """

    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_module(login, periode, "040_1", jaar)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_openstaande_debiteuren_data(login, periodes) -> pd.DataFrame:
    """

    Parameters
    ----------
    login:  login parameters (SessionParameters)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_module(login, periode, "100")
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def request_openstaande_crediteuren_data(login, periodes) -> pd.DataFrame:
    """

    Parameters
    ----------
    login:  login parameters (SessionParameters)
    periodes: list of period offsets that will be iterated over in the requests.

    Returns dataset containing records for selected module
    -------

    """
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_module(login, periode, "200")
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data
