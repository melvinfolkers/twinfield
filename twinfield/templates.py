import os


def import_xml(filename) -> str:
    """

    Parameters
    ----------
    filename: filename of the xml_template

    Returns: the xml template
    -------

    """
    function_dir = os.path.dirname(os.path.realpath(__file__))

    file_path = os.path.join(function_dir, filename)
    file = open(file_path, "r", encoding="utf-16")
    template = file.read()

    return template


def soap_metadata(param, module) -> str:
    """

    Parameters
    ----------
    param:  login parameters (SessionParameters)

    Returns: the xml template
    -------

    """

    #   <office>{office}</office>

    xml = f"""<read>
    <type>browse</type>
    <code>{module}</code>
    </read>"""

    body = import_xml(os.path.join("xml_templates", "template_metadata.xml")).format(
        param.session_id, xml
    )

    return body
