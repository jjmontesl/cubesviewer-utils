#

from sqlalchemy.engine import create_engine
from cubes.workspace import Workspace
import tempfile
import cubes
import configparser
import sqlalchemy
import cubetl
from cubetl import olap, cubes, sql
from cubetl.core.bootstrap import Bootstrap
import slugify


def pandas2cubes(dataframe):
    """
    """
    # Load dataframe to in-memory sqlite

    (tmpfile, db_path) = tempfile.mkstemp(suffix='.sqlite3', prefix='pandacubes')
    db_url = 'sqlite:///' + db_path

    engine = create_engine(db_url)
    connection = engine.connect()
    dataframe.to_sql("pandacube", connection)

    return sql2cubes(engine)


def sql2cubes(engine, tables=None):

    metadata = sqlalchemy.MetaData()
    metadata.reflect(engine)

    connection = sql.Connection()
    connection.id = "cubesutils.connection"
    connection.url = engine.url

    # Create Cubetl context
    bootstrap = Bootstrap()
    ctx = bootstrap.init()
    ctx.debug = True

    olapmappers = []

    for dbtable in metadata.sorted_tables:

        #print("+ Table: %s" % dbtable.name)

        tablename = slugify.slugify(dbtable.name, separator="_")

        # Define fact
        fact = olap.Fact()
        fact.id = "cubesutils.%s.fact" % (tablename)
        fact.label = dbtable.name
        fact.dimensions = []
        fact.measures = []
        fact.attributes = []

        olapmapper = olap.OlapMapper()
        olapmapper.id = "cubesutils.%s.olapmapper" % (tablename)
        olapmapper.mappers = []

        for dbcol in dbtable.columns:
            #print("+-- Column: %s [type=%s, null=%s, pk=%s, fk=%s]" % (dbcol.name, dbcol.type, dbcol.nullable, dbcol.primary_key, dbcol.foreign_keys))

            if str(dbcol.type) == "TEXT":
                # Create dimension
                dimension = olap.Dimension()
                dimension.id = "cubesutils.%s.dim.%s" % (tablename, slugify.slugify(dbcol.name, separator="_"))
                dimension.name = slugify.slugify(dbcol.name, separator="_")
                dimension.label = dbcol.name
                dimension.attributes = [{
                    "pk": True,
                    "name": slugify.slugify(dbcol.name, separator="_"),
                    "type": "String"
                    }]

                cubetl.container.add_component(dimension)
                fact.dimensions.append(dimension)

                mapper = olap.sql.EmbeddedDimensionMapper()
                mapper.entity = dimension
                #mapper.table = dbtable.name
                #mapper.connection = connection
                #mapper.lookup_cols = dbcol.name
                mapper.mappings = [{ 'name': slugify.slugify(dbcol.name, separator="_") }]
                olapmapper.mappers.append(mapper)


        mapper = olap.sql.FactMapper()
        mapper.entity = fact
        mapper.table = dbtable.name
        mapper.connection = connection
        mapper.mappings = [ { 'name': 'index', 'pk': True, 'type': 'Integer' } ]
        olapmapper.mappers.append(mapper)

        #  mappings:
        #  - name: id
        #    pk: True
        #    type: Integer
        #    value: ${ int(m["id"]) }

        cubetl.container.add_component(fact)
        olapmappers.append(olapmapper)

    # Export process
    modelwriter = cubes.Cubes10ModelWriter()
    modelwriter.id = "cubesutils.export-cubes"
    modelwriter.olapmapper = olap.OlapMapper()
    modelwriter.olapmapper.include = olapmappers
    cubetl.container.add_component(modelwriter)

    # Launch process
    ctx.start_node = "cubesutils.export-cubes"
    bootstrap.run(ctx)


    # Create cubes workspace
    #workspace = Workspace()
    #workspace.register_default_store("sql", url=connection.url)

    # Load model
    #workspace.import_model("model.json")

    #return workspace



def cubes_serve(workspace):
    """
    """

    config = configparser.RawConfigParser()

    # When adding sections or items, add them in the reverse order of
    # how you want them to be displayed in the actual file.
    # In addition, please note that using RawConfigParser's and the raw
    # mode of ConfigParser's respective set functions, you can assign
    # non-string values to keys internally, but will receive an error
    # when attempting to write to a file or when you get it in non-raw
    # mode. SafeConfigParser does not allow such assignments to take place.
    #config.add_section('Section1')
    #config.set('Section1', 'an_int', '15')
    #config.set('Section1', 'a_bool', 'true')
    #config.set('Section1', 'a_float', '3.1415')
    #config.set('Section1', 'baz', 'fun')
    #config.set('Section1', 'bar', 'Python')
    #config.set('Section1', 'foo', '%(bar)s is %(baz)s!')

    #cubes.server.run_server(config, debug=False)


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

              var cubesUrl = "http://cubesdemo.cubesviewer.com";

              // Initialize CubesViewer system
              cubesviewer.init({
                  cubesUrl: cubesUrl,
                  gaTrackEvents: true
              });

              // Sample serialized view (Based on cubes-examples project data)
              var serializedView =
                  '{"cubename":"webshop_sales","controlsHidden":false,"name":"Cube Webshop / Sales","mode":"chart","drilldown":["country:continent"],"cuts":[],"datefilters":[],"rangefilters":[],"xaxis":"date_sale@daily:month","yaxis":"price_total_sum","charttype":"lines-stacked","columnHide":{},"columnWidths":{},"columnSort":{},"chart-barsvertical-stacked":true,"chart-disabledseries":{"key":"product@product:product_category","disabled":{"Books":false,"Sports":false,"Various":false,"Videos":false}}}';

              // Add views
              cubesviewer.apply(function() {
                  view1 = cubesviewer.createView('#cv_embedded_01', "cube", serializedView);
              });

          });

        </script>
    """

    html = html.replace("{{ STATIC_URL }}", "/nbextensions/cubesext/static/")

    display(HTML(html))


