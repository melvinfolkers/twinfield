import logging
import os

import pandas as pd
from .run_settings import get_twinfield_settings
from . import functions, modules, transform
from .transform import maak_samenvatting
from .functions import select_office
from .modules import read_offices


def import_all(run_params, offices=None):

    login = get_twinfield_settings()
    all_offices = read_offices(login)

    if offices:

        logging.info("{} administraties beschikbaar".format(len(all_offices)))
        all_offices = all_offices[all_offices.name.isin(offices)]
        logging.info("{} administraties geselecteerd".format(len(all_offices)))

    pull_transactions(all_offices, run_params)

    # andere modules

    maak_samenvatting(run_params)


def add_metadata(df, office, rows):

    df["administratienaam"] = rows["name"]
    df["administratienummer"] = office
    df["wm"] = rows["shortname"]

    return df


def pull_transactions(offices, run_params):
    for office, rows in offices.iterrows():
        logging.info("\t" + 3 * "-" + str(rows["shortname"]) + 3 * "-")

        # refresh login (session id) for every run

        login = get_twinfield_settings()

        select_office(office, param=login)

        periodes = functions.period_groups(window="two_months")

        period = request_period(login, run_params, periodes)

        period = add_metadata(period, office, rows)

        period.to_pickle(
            os.path.join(run_params.pickledir, "{}_transactions.pkl".format(office))
        )


def request_period(login, run_params, periodes):
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_030_1(login, run_params, periode)
        batch = transform.format_030_1(batch)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data
