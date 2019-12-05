
import logging
from scripts.functions import import_files
from scripts.export import upload_data


def upload_all(jaar, run_params, start):
    logging.info('start met uploaden van datasets')

    data = import_files(run_params, 'transactions')
    upload_data(jaar, data, start, run_params)

    sv = import_files(run_params,'summary')
    upload_data('sv_' + jaar, sv, start, run_params)