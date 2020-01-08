from .scripts.modules import read_offices
from .scripts.credentials import twinfield_login

def officelist():

    login = twinfield_login()
    offices = read_offices(login)

    return offices



