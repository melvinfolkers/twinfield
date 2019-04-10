import pandas as pd
from functions import select_office
import modules

def pull_transactions(offices,login, jaar = None):

    ttl = pd.DataFrame()

    if not jaar:
        jaar = '2019'
    else:
        None

    for office, rows in offices.iterrows():

        select_office(office, param = login)

        data = modules.read_030_1(login, jaar)

        data['administratienaam'] = rows['name']
        data['wm'] = rows['shortname']

        ttl = pd.concat([ttl, data], axis = 0, ignore_index= True)

    return ttl
