import logging
import os

import pandas as pd

from . import functions, modules, transform
from .transform import maak_samenvatting
from .functions import select_office
from .modules import read_offices
from .credentials import twinfield_login

def import_all(run_params, jaar='2019', offices = None):

    login = twinfield_login()
    all_offices = read_offices(login)

    if offices:
        print(offices)
        print('voor' ,len(all_offices))
        all_offices = all_offices[all_offices.name.isin(offices)]
        print('na', len(all_offices))
    pull_transactions(all_offices, jaar, run_params)
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
