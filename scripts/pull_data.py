import logging
import os

import pandas as pd

from scripts import functions, modules, transform
from scripts.transform import maak_samenvatting
from scripts.functions import select_office
from scripts.modules import read_offices
from scripts.credentials import twinfield_login

def import_all(run_params, officecode=None, jaar='2019'):

    login = twinfield_login()
    offices = read_offices(login)

    if officecode != None:
        offices = offices[offices.index == officecode]

    pull_transactions(offices, jaar, run_params)
    maak_samenvatting(run_params)


def pull_transactions(offices, jaar, run_params):
    for office, rows in offices.iterrows():
        logging.info('\t' + 3 * '-' + str(rows['shortname']) + 3 * '-')

        # refresh login (session id) for every run

        login = twinfield_login()

        select_office(office, param=login)

        periodes = functions.period_groups(window='two_months')

        period = request_period(login, jaar, periodes)

        period['administratienaam'] = rows['name']
        period['administratienummer'] = office
        period['wm'] = rows['shortname']

        period.to_pickle(os.path.join(run_params.pickledir, '{}_transactions.pkl'.format(office)))


def request_period(login, jaar, periodes):
    data = pd.DataFrame()

    for periode in periodes:
        batch = modules.read_030_1(login, jaar, periode)
        batch = transform.format_030_1(batch)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data
