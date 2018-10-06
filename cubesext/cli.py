#

from sqlalchemy.engine import create_engine
from cubes.workspace import Workspace
import tempfile
import sqlalchemy
import cubetl
from cubetl import olap, cubes, sql
from cubetl.core.bootstrap import Bootstrap
from cubetl.core import cubetlconfig
from cubes import server
import slugify
import os
import sys
import subprocess
import signal
import logging
import time
import argparse
from cubesext import sql2cubes, cubes_serve
from cubesutils import cubesviewer_serve

if sys.version_info >= (3, 0):
    from configparser import ConfigParser
else:
    from ConfigParser import SafeConfigParser as ConfigParser


logger = logging.getLogger(__name__)


class CubesViewerTools:

    def main(self, argv):

        parser = argparse.ArgumentParser(description='Cubes and CubesViewer Tools')  #, usage=
        parser.add_argument('command', help='Subcommand to run: <sql2cubes>')
        args = parser.parse_args(argv[1:2])
        command = "command_" + args.command
        if not hasattr(self, command):
            print('Unrecognized command: %s' % args.command)
            parser.print_help()
            exit(1)

        # Use dispatch pattern to invoke method with the given name
        getattr(self, command)(argv)

    def command_sql2cubes(self, argv):

        parser = argparse.ArgumentParser(description='CubesViewer SQL to Cubes')  #, usage="cubesext sql2cubes <db_url>")
        parser.add_argument('db_url', help='Database connection URL (SQLAlchemy format)')
        parser.add_argument('-m', '--model', dest="model_path", nargs='?', type=str, default=None)
        parser.add_argument('-s', '--cubes', dest="serve_cubes", action="store_true", default=False)
        parser.add_argument('--cv', dest="serve_cv", action="store_true", default=True)
        parser.add_argument('--no-cv', dest="serve_cv", action="store_false")
        parser.add_argument('--browser', dest="browser", action="store_true", default=True)
        parser.add_argument('--no-browser', dest="browser", action="store_false")
        args = parser.parse_args(argv[2:])

        db_url = args.db_url
        # If it's a sqlite file, convert into SQLAlchemy URL
        if db_url.endswith('.sqlite3') and os.path.exists(db_url):
            print ("Database URL: %s" % db_url)
            db_url = 'sqlite:///%s' % db_url

        model_path = sql2cubes(db_url, model_path=args.model_path, tables=None, dimensions=None, debug=False)
        print ("Database URL: %s" % db_url)
        print ("Cubes model path: %s" % model_path)

        if args.serve_cubes:
            process = cubes_serve(db_url, model_path,
                                  host="localhost",
                                  port=5000,
                                  allow_cors_origin="*",
                                  debug=False,
                                  json_record_limit=5000)

        if args.serve_cv:
            process_cv = cubesviewer_serve(cubes_url="http://localhost:5000")

            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            process.wait()

            # Open browser if appropriate

        if args.serve_cubes:
            try:
                process.wait()
            except KeyboardInterrupt:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                process.wait()


def main():
    tools = CubesViewerTools()
    tools.main(sys.argv)


if __name__ == '__main__':
    main()
