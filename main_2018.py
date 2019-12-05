from scripts.functions import  set_logging, RunParameters
from scripts.pull_data import import_all
from scripts.upload import upload_all


def mainscript(run_params, jaar, refresh, upload):
    start = set_logging(run_params)

    if refresh:
        import_all(run_params, jaar=jaar)

    if upload:
        upload_all(jaar, run_params, start)


if __name__ == "__main__":

    run_params = RunParameters()

    jaren = ['2018']

    for jaar in jaren:
        mainscript(run_params, jaar, refresh = False, upload = True)
