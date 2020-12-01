from twinfield.messages import create_messages_files
from twinfield.send_soap_msg import run as send_soap_msg
from twinfield.export import append_responses
from twinfield.functions import RunParameters

from dotenv import load_dotenv

load_dotenv()


def run(run_params):
    create_messages_files(run_params)

    if run_params.upload:
        r = send_soap_msg(run_params)
        append_responses(r, run_params)


if __name__ == "__main__":

    run_params = RunParameters(
        jaar="2020", refresh=True, upload=True, modules=["040_1"], offices=[], rerun=False
    )

    run(run_params)
