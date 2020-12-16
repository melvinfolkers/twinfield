import logging
from typing import Union
import pandas as pd
from twinfield.pull_data import import_all
from twinfield.functions import RunParameters, import_files, remove_and_create_dir


def query(
    module: str,
    jaar: Union[int, str] = None,
    offices: list = None,
    rerun: bool = False,
    clean: bool = True,
) -> Union[pd.DataFrame, tuple]:
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
    rerun: bool
        set to True in case some modules are not correctly imported in previous run.
    clean: bool
        clean up pickle directory and remove pickle files.
    Returns
    -------
    df: pd.DataFrame or tuple of dateframes
        DataFrame containing for requested module, year and all offices in scope.
    """
    run_params = RunParameters(jaar=jaar, module=module, offices=offices, rerun=rerun)

    logging.info(
        f"{3 * '*'} Starting import of {run_params.module_names.get(run_params.module)} {3 * '*'}"
    )
    import_all(run_params)

    if run_params.module in ["dimensions_deb", "dimensions_crd"]:
        dim = import_files(run_params, run_params.module)
        dim_address = import_files(run_params, f"{run_params.module}_addresses")
        return dim, dim_address

    df = import_files(run_params, run_params.module)
    # clean up directory where files are stored
    if clean:
        remove_and_create_dir(run_params.pickledir)

    return df


def insert(module, xml_messages):
    pass
