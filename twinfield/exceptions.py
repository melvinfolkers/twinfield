class KostenPlaatsError(Exception):
    pass


class ServerError(Exception):
    pass


class FaultCodeNotFoundError(Exception):
    """Exception when faultcode not found"""

    pass


class EnvironmentVariablesError(Exception):
    """Exception when not all env variables are set"""

    pass
