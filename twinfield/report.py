import logging
import os
from datetime import datetime
import pymsteams
from .functions import stop_time

URL_AZURE_FUNCTION = "https://staffing-twinfield.azurewebsites.net/api/twinfield"


def create_message(title, body) -> pymsteams.connectorcard:
    """

    Parameters
    ----------
    title: Title of the teams message
    body: Body of the teams message

    Returns teams message class from (connectorcard) from pymsteams
    -------

    """
    WEBHOOK = os.environ.get("teams_webhook")
    if not WEBHOOK:
        raise ValueError("Geen teams webhook gevonden")

    myTeamsMessage = pymsteams.connectorcard(WEBHOOK)
    myTeamsMessage.color("3d79ab")
    myTeamsMessage.title(title)
    myTeamsMessage.text(body)
    # myTeamsMessage.addLinkButton("Consolidatie", f"{URL_AZURE_FUNCTION}?module=040_1")
    # myTeamsMessage.addLinkButton(
    #     "Openstaande posten debiteuren", f"{URL_AZURE_FUNCTION}?module=100"
    # )
    # myTeamsMessage.addLinkButton(
    #     "Openstaande posten crediteuren", f"{URL_AZURE_FUNCTION}?module=200"
    # )
    # myTeamsMessage.addLinkButton("Transacties 2020", f"{URL_AZURE_FUNCTION}?module=030_1?jaar=2020")
    # myTeamsMessage.addLinkButton("Transacties 2019", f"{URL_AZURE_FUNCTION}?module=030_1?jaar=2019")
    return myTeamsMessage


def create_section(msg, section_title, act_title, act_subtitle, act_body):

    act_img_url = "https://www.zypp.io/static/assets/icons/API building.png"
    # create the section
    myMessageSection = pymsteams.cardsection()

    # Section Title

    if section_title:
        myMessageSection.title(section_title)

    # Activity Elements
    myMessageSection.activityTitle(act_title)
    myMessageSection.activitySubtitle(act_subtitle)
    myMessageSection.activityImage(act_img_url)
    myMessageSection.activityText(act_body)

    msg.addSection(myMessageSection)

    return msg


def send_teams_message(tables, run_params):
    runtime = stop_time(run_params.start)

    if len(tables) == 0:
        return logging.info("geen bericht aangemaakt.")

    title = "Twinfield read"

    if len(tables) == 1:
        body = "1 tabel geëxporteerd. Zie onderstaand deze activiteit."
    else:
        body = (
            f"In het totaal zijn er {len(tables)} "
            f"tabellen geëxporteerd. Zie onderstaande activiteiten"
        )

    msg = create_message(title, body)

    for tablename, table in tables.items():
        msg = create_section(
            msg,
            section_title=run_params.modules,
            act_title=tablename,
            act_subtitle=f"{table.shape[0]} records geupload naar Azure.",
            act_body=f"afgerond in: {runtime}",
        )

    msg.send()


def send_insert_message(table, messages, run_params):
    runtime = stop_time(run_params.start)

    title = "Twinfield insert"

    body = "Zie onderstaande activiteit."

    msg = create_message(title, body)

    msg = create_section(
        msg,
        section_title=run_params.modules,
        act_title=f"script: {title}",
        act_subtitle=f"""{len(messages)} records aangeboden aan de twinfield API.
        Dit heeft geleid tot {table.shape[0]} responses.""",
        act_body=f"afgerond in: {runtime}",
    )

    msg.send()
