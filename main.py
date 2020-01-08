from .scripts.functions import  set_logging, RunParameters
from .scripts.pull_data import import_all
from .scripts.upload import upload_all
from datetime import datetime
import atexit
from functions import offices_in_scope

from exitaction import exitfunction

from functions import update_runstate
def mainscript(run_params, jaar, refresh, upload):
    start = set_logging(run_params)

    if refresh:
        import_all(run_params, jaar=jaar)

    if upload:
        upload_all(jaar, run_params, start)


def mainflask(run_params):
    start = datetime.now()

    update_runstate(run_params, runstate=4)

    atexit.register(exitfunction, run_params=run_params)

    if run_params.refresh.data:
        #print('hierzo ->', offices_in_scope(run_params))
        import_all(run_params, jaar=run_params.jaar.data, offices = offices_in_scope(run_params))

    if run_params.upload.data:
        upload_all(run_params.jaar.data, run_params, start)

    update_runstate(run_params, runstate=2)




