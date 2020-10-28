import logging
from .functions import import_files
from .export import upload_data
from df_to_azure.main import run as df_to_azure
from . import transform
from .mailing import send_mail


def upload_all(run_params, start):
    logging.info("start met uploaden van datasets")

    if "030_1" in run_params.modules:
        data = import_files(run_params, "transactions")
        upload_data(run_params.jaar, data, start, run_params)

        sv = import_files(run_params, "summary")
        upload_data("sv_" + run_params.jaar, sv, start, run_params)

    if "040_1" in run_params.modules:
        data = import_files(run_params, "consolidatie")
        data = transform.format_040_1(data)
        df_to_azure(data, f"consolidatie_{run_params.jaar}", schema="twinfield")

    if "100" in run_params.modules:
        data = import_files(run_params, "openstaande_debiteuren")
        data = transform.format_100(data)
        df_to_azure(data, f"openstaande_debiteuren", schema="twinfield")
        send_mail("Twinfield openstaande debiteuren", message=f"{len(data)} records geüpload naar Azure")

    if "200" in run_params.modules:
        data = import_files(run_params, "openstaande_crediteuren")
        data = transform.format_200(data)
        df_to_azure(data, f"openstaande_crediteuren", schema="twinfield")
        send_mail("Twinfield openstaande crediteuren", message=f"{len(data)} records geüpload naar Azure")
