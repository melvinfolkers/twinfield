import logging
from typing import Union
import pandas as pd
from twinfield.pull_data import import_all
from twinfield.functions import RunParameters, import_files


def query(module: str, jaar: Union[int, str] = None, offices: list = None) -> pd.DataFrame:
    """
    Import data from Twinfield using the API.

    Parameters
    ----------
    module: str
        Browse code for the Twinfield module to be imported.
    jaar: int or str
        Year of the scope.
    offices: list
        List of the offices in scope and to be imported.

    Returns
    -------
    df: pd.DataFrame
        DataFrame containing for requested module, year and all offices in scope.
    """
    run_params = RunParameters(jaar=jaar, module=module, offices=offices, rerun=False)
    logging.info(
        f"{3 * '*'} Starting import of {run_params.module_names.get(run_params.module)} {3 * '*'}"
    )
    import_all(run_params)
    df = import_files(run_params)

    return df


def insert(module, xml_messages):
    pass