import logging

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(levelname)-5s] [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

MODULES = {
    "100": "openstaande_debiteuren",
    "200": "openstaande_crediteuren",
    "030_1": "mutaties",
    "040_1": "consolidatie",
    "dimensions_deb": "stamgegevens_debiteuren",
    "dimensions_crd": "stamgegevens_crediteuren",
    "dimensions_kpl": "stamgegevens_kpl",
}

TEMPLATES = {
    "vrk": "template_transactions.xml",
    "ink": "template_transactions.xml",
    "memo": "template_transactions.xml",
    "salesinvoice": "template_salesinvoices.xml",
    "dimensions": "upload_dimensions.xml",
}
