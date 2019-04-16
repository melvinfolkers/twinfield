import pandas as pd
from functions import select_office
import modules
import transform
import functions
import logging

def pull_transactions(offices,login, jaar = 2019):

    ttl = pd.DataFrame()

    for office, rows in offices.iterrows():

        logging.info(3 * '-' + rows['name'] + 3 * '-')
        select_office(office, param = login)

        periodes = functions.period_groups(window = 'two_months')

        for periode in periodes:

            data = modules.read_030_1(login, jaar, periode)

            data['administratienaam'] = rows['name']
            data['administratienummer'] = office
            data['wm'] = rows['shortname']

            ttl = pd.concat([ttl, data], axis = 0, ignore_index= True, sort = False)

    ttl = transform.format_030_1(ttl)

    return ttl
