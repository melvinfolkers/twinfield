import logging
import os
from datetime import datetime
import pymsteams

WEBHOOK = "https://outlook.office.com/webhook/ee14f5e5-e9b2-464f-9a46-bda4c2ffb4f3@d8395682-5b2b-4f62-b3ec-2d438a29f6ea/IncomingWebhook/30ba4b33abf747038290ebb349ea09be/022ad6c2-8872-40cd-9cdd-5284a686e4b7"
URL_AZURE_FUNCTION = "https://staffing-twinfield.azurewebsites.net/api/twinfield"


def create_message(title, body):
    myTeamsMessage = pymsteams.connectorcard(WEBHOOK)
    myTeamsMessage.color("3d79ab")
    myTeamsMessage.title(title)
    myTeamsMessage.text(body)
    myTeamsMessage.addLinkButton("Consolidatie", f"{URL_AZURE_FUNCTION}?module=040_1")
    myTeamsMessage.addLinkButton(
        "Openstaande posten debiteuren", f"{URL_AZURE_FUNCTION}?module=100"
    )
    myTeamsMessage.addLinkButton(
        "Openstaande posten crediteuren", f"{URL_AZURE_FUNCTION}?module=200"
    )
    myTeamsMessage.addLinkButton(
        "Transacties 2020", f"{URL_AZURE_FUNCTION}?script=upload_dimensions?jaar=2020"
    )
    myTeamsMessage.addLinkButton(
        "Transacties 2019", f"{URL_AZURE_FUNCTION}?script=upload_dimensions?jaar=2019"
    )
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


def send_teams_message(tables):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M%:%S")

    if len(tables) == 0:
        return logging.info("geen bericht aangemaakt.")

    title = f"Twinfield DWH"

    if len(tables) == 1:
        body = f"""1 tabel "geëxporteerd". Zie onderstaand deze activiteit."""
    else:
        body = f"""In het totaal zijn er {len(tables)} tabellen geëxporteerd. Zie onderstaande activiteiten"""

    msg = create_message(title, body)

    for tablename, table in tables.items():
        msg = create_section(
            msg,
            section_title=None,
            act_title=tablename,
            act_subtitle=f"{table.shape[0]} records geupload naar Azure.",
            act_body=f"afgerond op: {stamp}",
        )

    msg.send()
