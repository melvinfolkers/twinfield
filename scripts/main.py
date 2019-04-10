

from  functions import SessionParameters
from modules import read_offices
from pull_data import pull_transactions



if __name__ == "__main__":

    login = SessionParameters(user ='Python',
                                        pw = r'U3RhZmZpbmcyMDE5IQ==\n',
                                        organisation = 'Associates')

    offices = read_offices(login)

    offices = offices.head(n=1)

    jaar = '2019'
    trans = pull_transactions(offices,login, jaar)

    tablename = f'py_twinfield_{jaar}'

    push_to_azure(trans.head(n=0), tablename)
    upload_to_blob(trans, tablename)