import logging

import azure.functions as func

from .pull_data import import_all
from dotenv import load_dotenv
import sentry_sdk
from sentry_sdk.integrations.serverless import serverless_function
from .functions import RunParameters

sentry_sdk.init(
    "https://03f0371227ad473a89d7358c89c2e6c5@o472336.ingest.sentry.io/5515670",
    traces_sample_rate=1.0,
)

load_dotenv()


@serverless_function
def run(run_params):
    """
    Parameters
    ----------
    run_params
        input parameters of script (set at start of script)

    Returns
    -------
    None. serverless function for azure deployment
    """
    if run_params.refresh:
        import_all(run_params)

    if run_params.upload:
        upload_all(run_params)


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Parameters
    ----------
    req
        the request received from the Azure function endpoint

    Returns
    -------
    HTML response with succes/failure message
    """

    options = {
        "100": "openstaande_debiteuren",
        "200": "openstaande_crediteuren",
        "030_1": "mutaties",
        "040_1": "consolidatie",
    }

    module = req.params.get("module")
    jaar = req.params.get("jaar")

    logging.info(f"Python HTTP trigger: request voor tabel {options.get(module)}")

    if not jaar:
        jaar = "2020"
    else:
        jaar = req.params.get("jaar")

    if not module:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            module = req_body.get("module")

    if module:

        run_params = RunParameters(
            jaar=jaar, refresh=True, upload=True, modules=[module], offices=[], rerun=False
        )
        run(run_params)

        return func.HttpResponse(
            f"Script {options.get(module)} van Twinfield is afgerond. Zie teams voor het resultaat"
        )
    else:
        return func.HttpResponse(
            f"Module is niet opgegeven of niet bekend geef met parameter 'module' "
            f"aan welk module je wil draaien, te kiezen uit {', '.join(options.keys())}.",
            status_code=200,
        )
