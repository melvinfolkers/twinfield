from scripts.functions import set_logging
from scripts.pull_data import import_all
from scripts.upload import upload_all
from scripts.run_settings import set_run_parameters
from dotenv import load_dotenv
import sentry_sdk
from sentry_sdk.integrations.serverless import serverless_function


sentry_sdk.init(
    "https://03f0371227ad473a89d7358c89c2e6c5@o472336.ingest.sentry.io/5515670",
    traces_sample_rate=1.0,
)


load_dotenv()


@serverless_function
def mainscript():

    run_params = set_run_parameters("../run_settings.yml")
    set_logging(run_params.logdir)

    if run_params.refresh:
        import_all(run_params)

    if run_params.upload:
        upload_all(run_params)


if __name__ == "__main__":

    mainscript()
