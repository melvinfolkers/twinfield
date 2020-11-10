import logging
from .functions import import_files

# from .export import upload_data
from df_to_azure.export import run as df_to_azure
from . import transform
from scripts.report import send_teams_message


def upload_all(run_params):
    logging.info("start met uploaden van datasets")

    if "030_1" in run_params.modules:
        data = import_files(run_params, "transactions")
        df_to_azure(
            df=data,
            tablename=f"transacties_{run_params.jaar}",
            schema="twinfield",
            method="create",
            local=True,
        )

        sv = import_files(run_params, "summary")

        df_to_azure(
            df=sv,
            tablename=f"sv_{run_params.jaar}",
            schema="twinfield",
            method="create",
            local=True,
        )

        send_teams_message(
            tables={f"Transacties {run_params.jaar}": data, f"Samenvatting {run_params.jaar}": sv}
        )

    if "040_1" in run_params.modules:

        data = import_files(run_params, "consolidatie")
        data = transform.format_040_1(data)
        df_to_azure(
            df=data,
            tablename=f"consolidatie_{run_params.jaar}",
            schema="twinfield",
            method="create",
            local=True,
        )

        send_teams_message(tables={f"Consolidatie {run_params.jaar}": data})

    if "100" in run_params.modules:
        data = import_files(run_params, "openstaande_debiteuren")
        data = transform.format_100(data)
        df_to_azure(
            df=data,
            tablename="openstaande_debiteuren",
            schema="twinfield",
            method="create",
            local=True,
        )

        send_teams_message(tables={"Openstaande debiteurenlijst": data})

    if "200" in run_params.modules:
        data = import_files(run_params, "openstaande_crediteuren")
        data = transform.format_200(data)
        df_to_azure(
            df=data,
            tablename="openstaande_crediteuren",
            schema="twinfield",
            method="create",
            local=True,
        )

        send_teams_message(tables={"Openstaande crediteurenlijst": data})
