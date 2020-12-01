from twinfield.messages import create_messages_files
from twinfield.send_soap_msg import run as send_soap_msg
from twinfield.export import append_responses
from twinfield.functions import RunParameters

from dotenv import load_dotenv

load_dotenv()


def insert(run_params):
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)

    Returns: None. Runs the insert scripts of this twinfield package.
    -------

    """
    create_messages_files(run_params)

    if run_params.upload:
        r = send_soap_msg(run_params)
        append_responses(r, run_params)


if __name__ == "__main__":

    run_params = RunParameters(
        jaar="2020", refresh=True, upload=True, modules="read_dimensions", offices=[], rerun=False
    )

    insert(run_params)
