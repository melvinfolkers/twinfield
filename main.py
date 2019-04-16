
import os
from functions import SessionParameters

from modules import read_offices
from pull_data import pull_transactions

from export import push_to_azure
from export import upload_to_blob
from mailing import send_mail

from datetime import datetime
import logging

class RunParameters():

    def __init__(self):

        self.projectdir = os.getcwd()

        self.logdir = RunParameters.create_dir(destination=os.path.join(self.projectdir, 'data', 'log'))
        self.stagingdir = RunParameters.create_dir(destination=os.path.join(self.projectdir, 'data', 'staging'))

    def create_dir(destination):

        try:
            if not os.path.exists(destination):
                os.makedirs(destination)
        except OSError:
            logging.warning('Error Creating directory. ' + destination)
        return destination


def set_logging():

    start = datetime.now()

    logfilename = 'runlog_' + start.strftime(format='%Y%m%d_%H%M') + '.log'
    full_path = os.path.join(run_params.logdir, logfilename)

    logging.getLogger().setLevel(logging.INFO)
    logging.basicConfig(filename=full_path, level=logging.INFO,format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    return start

def run_imports(officecode = None, jaar = '2019'):

    offices = read_offices(login)

    if officecode != None:
        offices = offices[offices.index == officecode]

    trans = pull_transactions(offices, login, jaar)

    return trans


def upload_data(jaar, data,start):

    tablename = f'twinfield_{jaar}'

    push_to_azure(data.head(n=0), tablename) # zorg dat het schema in met juiste veldeigenschappen klaarstaat in Azure (o regels)
    upload_to_blob(data, tablename, run_params.stagingdir)

    send_mail(subject=f'ADF: Twinfield data {jaar} geupload',
                  message=f'uploaddtijd: {datetime.now() - start} \naantal transacties: {len(data)}')


    logging.info(f'Finished in {datetime.now() - start} \n number of transactions: {len(data)}')


if __name__ == "__main__":

    run_params = RunParameters()

    login = SessionParameters(user='Python',
                              pw=r'U3RhZmZpbmcyMDE5IQ==\n',
                              organisation='Associates')

    start = set_logging()  # maakt een logbestand aan en bepaald starttijd
    jaar = '2018'

    data = run_imports(jaar = jaar, officecode= '1064667')
    upload_data(jaar, data,start)


