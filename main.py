from scripts.functions import set_logging
from scripts.pull_data import import_all
from scripts.upload import upload_all
from scripts.run_settings import set_run_parameters


def mainscript():

    run_params = set_run_parameters("run_settings.yml")
    start = set_logging(run_params.logdir)

    if run_params.refresh:
        import_all(run_params)

    if run_params.upload:
        upload_all(run_params, start)


if __name__ == "__main__":

    mainscript()
