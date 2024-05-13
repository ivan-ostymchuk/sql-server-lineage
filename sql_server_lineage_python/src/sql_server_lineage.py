from ctypes import cdll, POINTER, c_char_p, c_int, Array, CDLL, Structure
from typing import Dict, List
from distutils.sysconfig import get_config_var
from pathlib import Path
import json

class Error(Structure):
    _fields_ = [('err', c_char_p)]

    def __del__(self):
        # Free the memory allocated to error.
        if self.err is not None:
            del_error(self)

    def raise_if_err(self):
        if self.err is not None:
            raise IOError(self.err.decode())


class Lineage(Structure):
    # Multiple return values can be grabbed in a struct.
    _fields_ = [
        ('lineage', c_char_p),
        ('err', Error),
    ]

# Location of shared library
here = Path(__file__).absolute().parent
ext_suffix = get_config_var('EXT_SUFFIX')
so_file = str(here / ('_sql_server_lineage' + ext_suffix))
print(so_file)

# Load functions from shared library set their signatures
lib: CDLL = cdll.LoadLibrary(so_file)
extract_lineage_f = lib.extractLineage
extract_lineage_f.argtypes = [POINTER(c_char_p), c_int]
extract_lineage_f.restype = Lineage

del_error = lib.delError
del_error.argtypes = [Error]

generate_html = lib.generateHtmlLineage
generate_html.argtypes = [POINTER(c_char_p), c_int, c_char_p]
generate_html.restype = Error


def get_lineage(stored_procedures: List[str]) -> Dict:
    """Generate the lineage for the stored procedures provided.

    The format of the lineage data structure is the following:
        {'lineage': {'dbo.sink_table': {'dbo.stored_procedure': ['dbo.source', 'another_schema.another_source']}}}
    If multiple stored procedures write data to the same table then we will have:
        {'lineage': {'dbo.sink_table': {'dbo.stored_procedure_1': ['dbo.source'], 'dbo.stored_procedure_2': ['another_schema.another_source']}}}

    Args:
        - stored_procedures: The list of stored procedures to generate the lineage for.

    Returns:
        dict: Lineage dictionary.

    Raises:
        Error: if something went wrong during the lineage generation.
    """
    len_array: int = len(stored_procedures)
    ptr: Array[c_char_p] = (c_char_p * len_array)()
    ptr[:] = [s.encode() for s in stored_procedures]
    resultLineage: Lineage = extract_lineage_f(ptr, len_array)
    resultLineage.err.raise_if_err()
    lineage_nested: Dict = json.loads(resultLineage.lineage)

    return lineage_nested['lineage']

def generate_html_lineage(stored_procedures: List[str], filename: str = None) -> None:
    """Generate the lineage html file for the stored procedures provided.

    The html file should be read from left to right:
        sources -> stored_procedures -> sink_table

    Args:
        - stored_procedures: The list of stored procedures to generate the lineage for.
        - filename: Name of the html file, it must include the .html extension.

    Returns:
        None.

    Raises:
        ValueError: if .html extension has not been provided or if the extension is not .html
        Error: if something went wrong during the html generation.
    """
    if not filename:
        filename = "lineage_generated.html"
    else:
        splitted: str = filename.split(".")
        if len(splitted) == 1:
            raise ValueError("The filename provided should contain .html extension")
        elif splitted[1] != "html":
            raise ValueError("The filename provided should contain .html extension")
        
    len_array: int = len(stored_procedures)
    ptr: Array[c_char_p] = (c_char_p * len_array)()
    ptr[:] = [s.encode() for s in stored_procedures]
    filename_encoded: bytes = filename.encode()
    err: Error = generate_html(ptr, len_array, filename_encoded)
    err.raise_if_err()
