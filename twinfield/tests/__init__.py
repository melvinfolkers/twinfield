import logging

from keyvault import secrets_to_environment

from twinfield import TwinfieldApi

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(levelname)-5s] [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

secrets_to_environment("twinfield-test")

tw = TwinfieldApi()
