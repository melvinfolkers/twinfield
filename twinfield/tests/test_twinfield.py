import logging

from twinfield.tests import tw


def test_module_100():
    module = "100"
    df = tw.query_by_year(code=module, year=2021)
    logging.info(f"imported {df.shape[0]} records for module {module}")


def test_dimensions_deb():
    dim_type = "DEB"
    df = tw.dimensions(dim_type=dim_type)

    logging.info(f"imported {df.shape[0]} records for module {dim_type}")


def test_dimensions_deb_with_addresses():
    dim_type = "DEB"
    df, df_address = tw.dimensions(dim_type=dim_type, addresses=True)

    logging.info(f"imported {df.shape[0]} records for module {dim_type}")
    logging.info(f"imported {df_address.shape[0]} addresses  module {dim_type}")


def test_dimensions_cred():
    dim_type = "CRD"
    tw.offices = ["1074700"]
    df = tw.dimensions(dim_type=dim_type)

    logging.info(f"imported {df.shape[0]} records for module {dim_type}")


def test_dimensions_cred_with_addresses():
    dim_type = "CRD"
    df, df_address = tw.dimensions(dim_type=dim_type, addresses=True)

    logging.info(f"imported {df.shape[0]} records for module {dim_type}")
    logging.info(f"imported {df_address.shape[0]} addresses  module {dim_type}")


def test_dimensions_kpl():
    dim_type = "KPL"
    tw.offices = ["1074700"]
    df = tw.dimensions(dim_type=dim_type)

    logging.info(f"imported {df.shape[0]} records for module {dim_type}")


def test_module_030_1():
    module = "030_1"
    df = tw.query_by_year(code=module, year=2021)
    logging.info(f"imported {df.shape[0]} records for module {module}")


def test_module_with_filter():
    module = "100"
    filters = {
        "fin.trs.line.matchstatus": ["equal", {"from": "available"}],
    }
    df = tw.query_by_year(code=module, year=2021, filters=filters)
    logging.info(f"imported {df.shape[0]} records for module {module}")


def test_module_200():
    module = "200"
    df = tw.query_by_year(code=module, year=2021)
    logging.info(f"imported {df.shape[0]} records for module {module}")


if __name__ == "__main__":
    tw.offices = ["1074700"]
    test_module_100()
    test_module_200()
    test_module_030_1()
    test_module_with_filter()
    test_dimensions_deb()
    test_dimensions_deb_with_addresses()
    test_dimensions_cred()
    test_dimensions_cred_with_addresses()
    test_dimensions_kpl()
