

from  functions import SessionParameters
from modules import read_offices
from pull_data import pull_transactions

from export import push_to_azure
from export import upload_to_blob
from mailing import send_mail

from datetime import datetime

if __name__ == "__main__":

    start = datetime.now()

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

    runtime = datetime.now() - start

    send_mail(subject = f'ADF: Twinfield data {jaar} geupload', message = f'uploaddtijd: {runtime}')