from twinfield.pull_data import import_all
from twinfield.upload import upload_all
from twinfield.functions import RunParameters

from dotenv import load_dotenv

load_dotenv()


def run(run_params) -> None:
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)

    Returns None. executes the query scripts.
    -------

    """

    if run_params.refresh:

        import_all(run_params)

    if run_params.upload:
        upload_all(run_params)


if __name__ == "__main__":

    run_params = RunParameters(
        jaar="2019", refresh=True, upload=True, modules=["200"], offices=[], rerun=False
    )

    run(run_params)
