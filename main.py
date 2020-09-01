from scripts import ljp
from scripts.functions import set_logging
from scripts.pull_data import import_all
from scripts.upload import  upload_all
from scripts.run_settings import set_run_parameters


def mainscript():

    run_params = set_run_parameters('notebooks/run_settings_fy2020.yml')
    start = set_logging(run_params.logdir)


    if run_params.refresh:
        import_all(run_params, offices=['Carrière Uitzendbureau B.V.'])

    if run_params.upload:
        upload_all(run_params, start)


if __name__ == "__main__":

    mainscript()
