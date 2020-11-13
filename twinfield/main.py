from .pull_data import import_all
from .upload import upload_all
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

    if run_params.refresh:

        import_all(run_params)

    if run_params.upload:
        upload_all(run_params)


if __name__ == "__main__":


    run_params = RunParameters(
        jaar="2020", refresh=True, upload=False, modules=["040_1"], offices = None, rerun = False)
    )

    run(run_params)
