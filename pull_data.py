import pandas as pd
from functions import select_office
from modules import read_offices
import modules
import transform
import functions
import logging
from export import upload_data

def pull_transactions(offices,login, jaar = 2019):

    data = pd.DataFrame()

    for office, rows in offices.iterrows():

        logging.info(3 * '-' + rows['name'] + 3 * '-')
        select_office(office, param = login)

        periodes = functions.period_groups(window = 'two_months')

        period = request_period(login,jaar, periodes)
        data = pd.concat([data, period], axis=0, ignore_index=True, sort=False)

        data['administratienaam'] = rows['name']
        data['administratienummer'] = office
        data['wm'] = rows['shortname']

    return data

def request_period(login,jaar, periodes):

    data = pd.DataFrame()

    for periode in periodes:

        batch = modules.read_030_1(login, jaar, periode)
        batch = transform.format_030_1(batch)
        data = pd.concat([data, batch], axis=0, ignore_index=True, sort=False)

    return data


def run_transactions(run_params,login, start, officecode = None, jaar = '2019'):

    offices = read_offices(login)

    if officecode != None:
        offices = offices[offices.index == officecode]

    trans = pull_transactions(offices, login, jaar)

    upload_data('2019', trans, start, run_params)

    return trans