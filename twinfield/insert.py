from twinfield.messages import create_messages_files
from twinfield.send_soap_msg import run as send_soap_msg
from twinfield.functions import RunParameters

from dotenv import load_dotenv

load_dotenv(override=True)


def insert(run_params):
    """

    Parameters
    ----------
    run_params:  input parameters of script (set at start of script)

    Returns: None. Runs the insert scripts of this twinfield package.
    -------

    """
    create_messages_files(run_params)

    r = send_soap_msg(run_params)

    return r


if __name__ == "__main__":
    run_params = RunParameters(jaar="2020", module="read_dimensions", offices=[], rerun=False)
    insert(run_params)
