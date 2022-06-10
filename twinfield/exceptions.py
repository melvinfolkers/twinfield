class KostenPlaatsError(Exception):
    pass


class ServerError(Exception):
    pass


class FaultCodeNotFoundError(Exception):
    """Exception when faultcode not found"""

    pass


class TwinfieldFaultCode(Exception):
    """Exception when Twinfield has raised a faultcode"""

    pass


class SelectOfficeError(Exception):
    """Exception when selecting office in twinfield is not succesful"""

    pass


class EnvironmentVariablesError(Exception):
    """Exception when not all env variables are set"""

    pass
