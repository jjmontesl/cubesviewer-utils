
from .cubesutils import pandas2cubes, cubesviewer_jupyter

__author__ = 'Jose Juan Montes [@jjmontesl]'
__description__ = 'Integration of Cubes and CubesViewer with Pandas, Jupyter Notebook and Django.'
__version__ = '0.1.0'


"""
def _jupyter_server_extension_paths():
    return [{
        "module": "cubesext"
    }]


def load_jupyter_server_extension(nbapp):
    nbapp.log.info("CubesViewer notebook module enabled.")
"""


# Jupyter Extension points
def _jupyter_nbextension_paths():
    return [dict(
        section="notebook",
        # the path is relative to the `cubesext` extension directory
        src="static",
        # directory in the `nbextension/` namespace
        dest="cubesext",
        # _also_ in the `nbextension/` namespace
        #require="cubesext/main"
        )]




