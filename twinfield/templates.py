import os


def import_xml(filename):
    function_dir = os.path.dirname(os.path.realpath(__file__))

    file_path = os.path.join(function_dir, filename)
    file = open(file_path, "r", encoding="utf-16")
    template = file.read()

    return template


def soap_metadata(param, module):

    #   <office>{office}</office>

    xml = """<read>
    <type>browse</type>
    <code>{}</code>
    </read>""".format(
        module
    )

    body = import_xml("template_metadata.xml").format(
        param.session_id, xml
    )

    return body
