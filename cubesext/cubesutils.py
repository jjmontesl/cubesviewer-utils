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

if sys.version_info >= (3, 0):
    from configparser import ConfigParser
else:
    from ConfigParser import SafeConfigParser as ConfigParser


def pandas2cubes(dataframe):
    """
    """
    # Load dataframe to in-memory sqlite

    (tmpfile, db_path) = tempfile.mkstemp(suffix='.sqlite3', prefix='cubesext-db-')
    db_url = 'sqlite:///' + db_path

    engine = create_engine(db_url)
    connection = engine.connect()
    dataframe.to_sql("pandacube", connection)

    return sql2cubes(engine)


def sql2cubes(engine, model_path=None, tables=None, dimensions=None, debug=False):

    exclude_columns = ['key']
    force_dimensions = dimensions if dimensions else []

    metadata = sqlalchemy.MetaData()
    metadata.reflect(engine)

    connection = sql.Connection()
    connection.id = "cubesutils.connection"
    connection.url = engine.url

    # Create Cubetl context
    bootstrap = Bootstrap()
    ctx = bootstrap.init(debug=debug)
    ctx.debug = True

    # Load yaml library definitions that are dependencies
    cubetlconfig.load_config(ctx, os.path.dirname(__file__) + "/cubetl-datetime.yaml")


    olapmappers = {}  # Indexed by table name
    factdimensions = {}  # Indexed by table_name
    facts = {}  # Indexed by table name

    def coltype(dbcol):
        if str(dbcol.type) in ("FLOAT", "REAL", "DECIMAL"):
            return "Float"
        elif str(dbcol.type) in ("INTEGER", "BIGINT"):
            return "Integer"
        elif str(dbcol.type) in ("BOOLEAN", "TEXT") or str(dbcol.type).startswith("VARCHAR"):
            return "String"
        return None

    for dbtable in metadata.sorted_tables:

        if dbtable.name in ('sqlite_sequence'):
            continue

        print("+ Table: %s" % dbtable.name)

        tablename = slugify.slugify(dbtable.name, separator="_")

        # Define fact
        fact = olap.Fact()
        fact.id = "cubesutils.%s.fact" % (tablename)
        fact.name = slugify.slugify(dbtable.name, separator="_")
        fact.label = dbtable.name
        fact.dimensions = []
        fact.measures = []
        fact.attributes = []

        facts[dbtable.name] = fact

        olapmapper = olap.OlapMapper()
        olapmapper.id = "cubesutils.%s.olapmapper" % (tablename)
        olapmapper.mappers = []
        olapmapper.include = []

        factmappings = []

        for dbcol in dbtable.columns:

            if dbcol.name in exclude_columns:
                continue

            print("+-- Column: %s [type=%s, null=%s, pk=%s, fk=%s]" % (dbcol.name, dbcol.type, dbcol.nullable, dbcol.primary_key, dbcol.foreign_keys))

            if dbcol.primary_key:
                if (str(dbcol.type) == "INTEGER"):
                    factmappings.append( { 'name': slugify.slugify(dbcol.name, separator="_"),
                                           'pk': True,
                                           'type': 'Integer' } )
                elif str(dbcol.type) == "TEXT" or str(dbcol.type).startswith("VARCHAR"):
                    factmappings.append( { 'name': slugify.slugify(dbcol.name, separator="_"),
                                           'pk': True,
                                           'type': 'String' } )
                else:
                    raise Exception("Unknown column type (%s) for primary key column: %s" % (dbcol.type, dbcol.name))

            elif dbcol.foreign_keys and len(dbcol.foreign_keys) > 0:

                if len(dbcol.foreign_keys) > 1:
                    raise Exception("Multiple foreign keys found for column: %s" % (dbcol.name))

                related_fact = list(dbcol.foreign_keys)[0].column.table.name

                if related_fact == dbtable.name:
                    # Reference to self
                    # TODO: This does not account for circular dependencies across other entities
                    continue

                factdimension = None
                if related_fact in factdimensions:
                    factdimension = factdimensions[related_fact]
                else:
                    factdimension = olap.FactDimension()
                    factdimension.id = "cubesutils.%s.dim.%s" % (tablename, slugify.slugify(related_fact, separator="_"))
                    factdimension.name = slugify.slugify(related_fact, separator="_")
                    factdimension.label = related_fact
                    factdimension.fact = facts[related_fact]
                    cubetl.container.add_component(factdimension)

                    factdimensions[related_fact] = factdimension

                # Create an alias
                aliasdimension = olap.AliasDimension()
                aliasdimension.dimension = factdimension
                aliasdimension.id = "cubesutils.%s.dim.%s.%s" % (tablename, slugify.slugify(related_fact, separator="_"), slugify.slugify(dbcol.name, separator="_"))
                aliasdimension.name = slugify.slugify(dbcol.name, separator="_").replace("_id", "")
                aliasdimension.label = slugify.slugify(dbcol.name, separator="_").replace("_id", "")
                cubetl.container.add_component(aliasdimension)

                fact.dimensions.append(aliasdimension)

                mapper = olap.sql.FactDimensionMapper()
                mapper.entity = aliasdimension
                mapper.mappings = [{ 'name': slugify.slugify(dbcol.name, separator="_").replace("_id", ""),
                                     'column': dbcol.name,
                                     'pk': True
                                  }]
                olapmapper.include.append(olapmappers[related_fact])
                olapmapper.mappers.append(mapper)

            elif (dbcol.name in force_dimensions) or coltype(dbcol) == "String":

                # Create dimension
                dimension = olap.Dimension()
                dimension.id = "cubesutils.%s.dim.%s" % (tablename, slugify.slugify(dbcol.name, separator="_"))
                dimension.name = slugify.slugify(dbtable.name, separator="_") + "_" + slugify.slugify(dbcol.name, separator="_")
                dimension.label = dbcol.name
                dimension.attributes = [{
                    "pk": True,
                    "name": slugify.slugify(dbtable.name, separator="_") + "_" + slugify.slugify(dbcol.name, separator="_"),
                    "type": coltype(dbcol)
                    }]

                cubetl.container.add_component(dimension)
                fact.dimensions.append(dimension)

                mapper = olap.sql.EmbeddedDimensionMapper()
                mapper.entity = dimension
                #mapper.table = dbtable.name
                #mapper.connection = connection
                #mapper.lookup_cols = dbcol.name
                mapper.mappings = [{ 'name': slugify.slugify(dbtable.name, separator="_") + "_" + slugify.slugify(dbcol.name, separator="_"),
                                     'column': slugify.slugify(dbcol.name, separator="_") }]
                olapmapper.mappers.append(mapper)

            elif str(dbcol.type) in ("FLOAT", "REAL", "DECIMAL", "INTEGER"):

                measure = {
                    "name": dbcol.name,
                    "label": dbcol.name,
                    "type": "Integer" if str(dbcol.type) in ["INTEGER"] else "Float"
                }
                fact.measures.append(measure)

            elif str(dbcol.type) in ("DATETIME"):

                factdimension = cubetl.container.get_component_by_id("cubetl.datetime.date")

                # Create an alias to a datetime dimension
                aliasdimension = olap.AliasDimension()
                aliasdimension.dimension = factdimension
                aliasdimension.id = "cubesutils.%s.dim.%s.%s" % (slugify.slugify(dbtable.name, separator="_"), "datetime", slugify.slugify(dbcol.name, separator="_"))
                aliasdimension.name = slugify.slugify(dbtable.name, separator="_") + "_" + slugify.slugify(dbcol.name, separator="_").replace("_id", "")
                aliasdimension.label = slugify.slugify(dbtable.name, separator="_") + " " + slugify.slugify(dbcol.name, separator="_").replace("_id", "")
                cubetl.container.add_component(aliasdimension)

                fact.dimensions.append(aliasdimension)

                mapper = olap.sql.EmbeddedDimensionMapper()
                mapper.entity = aliasdimension
                mapper.mappings = [{ 'name': 'year', 'column': dbcol.name, 'extract': 'year' },
                                   { 'name': 'quarter', 'column': dbcol.name, 'extract': 'quarter' },
                                   { 'name': 'month', 'column': dbcol.name, 'extract': 'month' },
                                   { 'name': 'week', 'column': dbcol.name, 'extract': 'week' },
                                   { 'name': 'day', 'column': dbcol.name, 'extract': 'day' }]
                #olapmapper.include.append(olapmappers[related_fact])
                olapmapper.mappers.append(mapper)

            else:

                print("    Cannot map column '%s' (type: %s)" % (dbcol.name, dbcol.type))


        mapper = olap.sql.FactMapper()
        mapper.entity = fact
        mapper.table = dbtable.name
        mapper.connection = connection
        if len(factmappings) > 0:
            mapper.mappings = factmappings
        else:
            mapper.mappings = [ { 'name': 'index', 'pk': True, 'type': 'Integer' } ]
        olapmapper.mappers.append(mapper)

        #  mappings:
        #  - name: id
        #    pk: True
        #    type: Integer
        #    value: ${ int(m["id"]) }

        cubetl.container.add_component(fact)
        olapmappers[dbtable.name] = olapmapper

    # Export process
    modelwriter = cubes.Cubes10ModelWriter()
    modelwriter.id = "cubesutils.export-cubes"
    modelwriter.olapmapper = olap.OlapMapper()
    modelwriter.olapmapper.include = [i for i in olapmappers.values()]

    #modelwriter.olapmapper.mappers = [ ]
    #for om in olapmappers:
    #    for m in om.mappers:
    #        modelwriter.olapmapper.mappers.append(m)
    #        print(m.entity)
    cubetl.container.add_component(modelwriter)

    # Launch process
    ctx.start_node = "cubesutils.export-cubes"
    result = bootstrap.run(ctx)
    model_json = result["cubesmodel_json"]

    # Write model
    if model_path:
        with open(model_path, "w") as tmpfile:
            tmpfile.write(model_json)
    else:
        (tmpfile, model_path) = tempfile.mkstemp(suffix='.json', prefix='cubesext-model-')
        os.write(tmpfile, model_json.encode("utf-8"))
        os.close(tmpfile)

    #workspace = Workspace()
    #workspace.register_default_store("sql", url=connection.url)

    # Load model
    #workspace.import_model("model.json")

    return (engine.url, model_path)



def cubes_serve(db_url, model_path, host="localhost", port=5000, allow_cors_origin="*", debug=False, json_record_limit=5000):
    """
    """

    config = ConfigParser()

    # When adding sections or items, add them in the reverse order of
    # how you want them to be displayed in the actual file.
    # In addition, please note that using RawConfigParser's and the raw
    # mode of ConfigParser's respective set functions, you can assign
    # non-string values to keys internally, but will receive an error
    # when attempting to write to a file or when you get it in non-raw
    # mode. SafeConfigParser does not allow such assignments to take place.

    config.add_section('server')
    config.set('server', 'host', host)
    config.set('server', 'port', str(port))
    config.set('server', 'json_record_limit', str(json_record_limit))
    config.set('server', 'processes', '1')
    config.set('server', 'use_reloader', "False")
    config.set('server', 'allow_cors_origin', allow_cors_origin)

    config.add_section('store')
    config.set('store', 'type', 'sql')
    config.set('store', 'url', str(db_url))

    config.add_section('models')
    config.set('models', 'main', model_path)

    server.run_server(config, debug=debug)


def cubesviewer_serve(host="localhost", port="8085"):
    """
    """

    # Launch server on workspace

    # Serve studio on localhostand open browser?
    pass


def cubesviewer_jupyter():

    # JUPYTER INTEGRATION

    from IPython.display import display, HTML

    html = """

        <link rel="stylesheet" href="{{ STATIC_URL }}lib/bootstrap/bootstrap.css" />
        <link rel="stylesheet" href="{{ STATIC_URL }}lib/angular-ui-grid/ui-grid.css" />
        <link rel="stylesheet" href="{{ STATIC_URL }}lib/font-awesome/css/font-awesome.css" />
        <link rel="stylesheet" href="{{ STATIC_URL }}lib/nvd3/nv.d3.css" />
        <link rel="stylesheet" href="{{ STATIC_URL }}lib/cubesviewer/cubesviewer.css" />
        <link rel="stylesheet" href="{{ STATIC_URL }}lib/bootstrap-submenu/css/bootstrap-submenu.css" /> <!-- after cubesviewer.css! -->

        <!--<script src="{{ STATIC_URL }}lib/jquery/jquery.js"></script>-->
        <!--<script src="{{ STATIC_URL }}lib/bootstrap/bootstrap.js"></script>-->
        <script src="{{ STATIC_URL }}lib/bootstrap-submenu/js/bootstrap-submenu.js"></script>
        <script src="{{ STATIC_URL }}lib/angular/angular.js"></script>
        <script src="{{ STATIC_URL }}lib/angular-cookies/angular-cookies.js"></script>
        <script src="{{ STATIC_URL }}lib/cubesviewer/cubesviewer.js"></script>
        <script src="{{ STATIC_URL }}lib/angular-bootstrap/ui-bootstrap-tpls.js"></script>
        <script src="{{ STATIC_URL }}lib/angular-ui-grid/ui-grid.js"></script>
        <script src="{{ STATIC_URL }}lib/d3/d3.js"></script>
        <script src="{{ STATIC_URL }}lib/nvd3/nv.d3.js"></script>
        <!--<script src="{{ STATIC_URL }}lib/flotr2/flotr2.min.js"></script>-->

        <div id="cv_embedded_01"><i>CubesViewer View</i></div>

        <script type="text/javascript">

          //Reference to the created view
          var view1 = null;

          // Initialize CubesViewer when document is ready
          $(document).ready(function() {

              console.debug("Initializing CubesViewer cell in Jupyter Notebook.");

              var cubesUrl = "http://localhost:5000";

              // Initialize CubesViewer system
              cubesviewer.init({
                  cubesUrl: cubesUrl
              });

              // Add views
              cubesviewer.apply(function() {
                  //view1 = cubesviewer.createView('#cv_embedded_01', "cube", serializedViewOrObject);
                  var serializedView = {"mode":"summary", "cubename":"estat_earn_ses", "name": "Sample View"};

                  view1 = cubesviewer.createView('#cv_embedded_01', "pandacube", serializedView);
              });

          });

        </script>
    """

    html = html.replace("{{ STATIC_URL }}", "/nbextensions/cubesext/static/")

    display(HTML(html))


