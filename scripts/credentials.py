from scripts.functions import SessionParameters
import os


def twinfield_login():

    user = os.environ.get('TW_USER_SA')
    password = os.environ.get('TW_PW_SA')
    organisation = os.environ.get('TW_ORG_SA')

    login = SessionParameters(user=user,
                              pw=password,
                              organisation= organisation)

    return login