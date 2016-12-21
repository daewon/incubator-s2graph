package org.apache.s2graph.core.tinkerpop

import java.io.File
import java.util
import java.util.concurrent.atomic.AtomicLong

import org.apache.commons.configuration.Configuration
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{ColumnMeta, Label, ServiceColumn}
import org.apache.s2graph.core.types.{HBaseType, InnerVal, VertexId}
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData
import org.apache.tinkerpop.gremlin.structure.{Element, Graph, T}
import org.apache.tinkerpop.gremlin.{AbstractGraphProvider, LoadGraphWith}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.s2graph.core.utils.logger

object S2GraphProvider {
  val Implementation: Set[Class[_]] = Set(
    classOf[S2Edge],
    classOf[S2Vertex],
    classOf[S2Property[_]],
    classOf[S2VertexProperty[_]],
    classOf[S2Graph]
  )
}
class S2GraphProvider extends AbstractGraphProvider {

  override def getBaseConfiguration(s: String, aClass: Class[_], s1: String, graphData: GraphData): util.Map[String, AnyRef] = {
    val config = ConfigFactory.load()
//    val dbUrl =
//      if (config.hasPath("db.default.url")) config.getString("db.default.url")
//      else "jdbc:mysql://localhost:3306/graph_dev"

    val dbUrl = "jdbc:mysql://localhost:3306/graph_dev"
    val m = new java.util.HashMap[String, AnyRef]()
    m.put(Graph.GRAPH, classOf[S2Graph].getName)
    m.put("db.default.url", dbUrl)
    m.put("db.default.driver", "com.mysql.jdbc.Driver")
    m
  }

  private val H2Prefix = "jdbc:h2:file:"

  override def clear(graph: Graph, configuration: Configuration): Unit =
    if (graph != null) {
      val s2Graph = graph.asInstanceOf[S2Graph]
      if (s2Graph.isRunning) {
        val labels = Label.findAll()
        labels.groupBy(_.hbaseTableName).values.foreach { labelsWithSameTable =>
          labelsWithSameTable.headOption.foreach { label =>
            s2Graph.management.truncateStorage(label.label)
          }
        }
        s2Graph.shutdown(modelDataDelete = true)
        logger.info("S2Graph Shutdown")
      }
    }

  override def getImplementations: util.Set[Class[_]] = S2GraphProvider.Implementation.asJava

  override def loadGraphData(graph: Graph, loadGraphWith: LoadGraphWith, testClass: Class[_], testName: String): Unit = {
    val s2Graph = graph.asInstanceOf[S2Graph]
    val mnt = s2Graph.getManagement()
    val service = s2Graph.DefaultService
    val column = s2Graph.DefaultColumn

    logger.info(s"LoadGraphData => testClass: $testClass TestName: $testName")

    Management.deleteLabel("knows")

    var knowsProp = Vector(
      Prop("weight", "0.0", "double"),
      Prop("data", "-", "string"),
      Prop("year", "-1", "integer"),
      Prop("boolean", "false", "boolean"),
      Prop("float", "0.0", "float"),
      Prop("double", "0.0", "double"),
      Prop("long", "0.0", "long"),
      Prop("string", "-", "string"),
      Prop("integer", "-", "integer"),
      Prop("aKey", "-", "string")
    )

   // Change dataType for ColumnMeta('aKey') for PropertyFeatureSupportTest
    if (testClass.getSimpleName == "PropertyFeatureSupportTest") {
      knowsProp = knowsProp.filterNot(_.name == "aKey")

      val dataType = if (testName.toLowerCase.contains("boolean")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "false", "boolean")
        "boolean"
      } else if (testName.toLowerCase.contains("integer")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0", "integer")
        "integer"
      } else if (testName.toLowerCase.contains("long")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0", "long")
        "long"
      } else if (testName.toLowerCase.contains("double")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0.0", "double")
        "double"
      } else if (testName.toLowerCase.contains("float")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "0.0", "float")
        "float"
      } else if (testName.toLowerCase.contains("string")) {
        knowsProp = knowsProp.filterNot(_.name == "aKey") :+ Prop("aKey", "-", "string")
        "string"
      } else {
        "string"
      }

      val DefaultColumn = s2Graph.DefaultColumn
      ColumnMeta.findByName(DefaultColumn.id.get, "aKey", useCache = false).foreach(cm => ColumnMeta.delete(cm.id.get))
      ColumnMeta.findOrInsert(DefaultColumn.id.get, "aKey", dataType, useCache = false)
    }

    // knows props
    if (testClass.getSimpleName == "EdgeTest" && testName == "shouldAutotypeDoubleProperties") {
      mnt.createLabel("knows", service.serviceName, "vertex", "string", service.serviceName, "vertex", "string",
        true, service.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": true}"""))
    } else {
      mnt.createLabel("knows", service.serviceName, "person", "integer", service.serviceName, "person", "integer",
        true, service.serviceName, Nil, knowsProp, "strong", None, None, options = Option("""{"skipReverse": true}"""))
    }

    val personColumn = Management.createServiceColumn(service.serviceName, "person", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))
    val softwareColumn = Management.createServiceColumn(service.serviceName, "software", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("lang", "-", "string")))
    val productColumn = Management.createServiceColumn(service.serviceName, "product", "integer", Nil)
    val dogColumn = Management.createServiceColumn(service.serviceName, "dog", "integer", Nil)
//    val vertexColumn = Management.createServiceColumn(service.serviceName, "vertex", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "-1", "integer"), Prop("lang", "scala", "string")))

    val created = mnt.createLabel("created", service.serviceName, "person", "integer", service.serviceName, "software", "integer",
      true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)

    val bought = mnt.createLabel("bought", service.serviceName, "person", "integer", service.serviceName, "product", "integer",
      true, service.serviceName, Nil, Seq(Prop("x", "-", "string"), Prop("y", "-", "string")), "strong", None, None,
      options = Option("""{"skipReverse": true}"""))

    val test = mnt.createLabel("test", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Seq(Prop("xxx", "-", "string")), "weak", None, None,
      options = Option("""{"skipReverse": true}"""))

    val self = mnt.createLabel("self", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": true}"""))

    val friends = mnt.createLabel("friends", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil,
      "weak", None, None,
      options = Option("""{"skipReverse": true}"""))

    val friend = mnt.createLabel("friend", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil,
      Seq(
        Prop("name", "-", "string"),
        Prop("location", "-", "string"),
        Prop("status", "-", "string")
      ),
      "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val hate = mnt.createLabel("hate", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    val collaborator = mnt.createLabel("collaborator", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil,
      Seq(
        Prop("location", "-", "string")
      ),
      "strong", None, None,
       options = Option("""{"skipReverse": true}""")
    )

    val test1 = mnt.createLabel("test1", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )
    val test2 = mnt.createLabel("test2", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )
    val test3 = mnt.createLabel("test3", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil, "weak", None, None,
      options = Option("""{"skipReverse": false}""")
    )

    super.loadGraphData(graph, loadGraphWith, testClass, testName)
  }

  override def convertId(id: scala.Any, c: Class[_ <: Element]): AnyRef = {
    val isVertex = c.toString.contains("Vertex")
    if (isVertex) {
      VertexId(ServiceColumn.findAll().head, InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION))
    } else {
      EdgeId(
        InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION),
        InnerVal.withStr(id.toString, HBaseType.DEFAULT_VERSION),
        "_s2graph",
        "out",
        System.currentTimeMillis()
      )
    }
  }

  override def convertLabel(label: String): String = {
    label
  }
}
//public class TinkerGraphProvider extends AbstractGraphProvider {
//
//  private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
//        add(TinkerEdge.class);
//        add(TinkerElement.class);
//        add(TinkerGraph.class);
//        add(TinkerGraphVariables.class);
//        add(TinkerProperty.class);
//        add(TinkerVertex.class);
//        add(TinkerVertexProperty.class);
//    }};
//
//  @Override
//  public Map<String, Object> getBaseConfiguration(final String graphName, final Class<?> test, final String testMethodName,
//  final LoadGraphWith.GraphData loadGraphWith) {
//    final TinkerGraph.DefaultIdManager idManager = selectIdMakerFromGraphData(loadGraphWith);
//    final String idMaker = (idManager.equals(TinkerGraph.DefaultIdManager.ANY) ? selectIdMakerFromTest(test, testMethodName) : idManager).name();
//    return new HashMap<String, Object>() {{
//            put(Graph.GRAPH, TinkerGraph.class.getName());
//            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_ID_MANAGER, idMaker);
//            put(TinkerGraph.GREMLIN_TINKERGRAPH_EDGE_ID_MANAGER, idMaker);
//            put(TinkerGraph.GREMLIN_TINKERGRAPH_VERTEX_PROPERTY_ID_MANAGER, idMaker);
//            if (requiresListCardinalityAsDefault(loadGraphWith, test, testMethodName))
//                put(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
//            if (requiresPersistence(test, testMethodName)) {
//                put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_FORMAT, "gryo");
//                final File tempDir = TestHelper.makeTestDataPath(test, "temp");
//                put(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION,
//                        tempDir.getAbsolutePath() + File.separator + testMethodName + ".kryo");
//            }
//        }};
//  }
//
//  @Override
//  public void clear(final Graph graph, final Configuration configuration) throws Exception {
//    if (graph != null)
//      graph.close();
//
//    // in the even the graph is persisted we need to clean up
//    final String graphLocation = configuration.getString(TinkerGraph.GREMLIN_TINKERGRAPH_GRAPH_LOCATION, null);
//    if (graphLocation != null) {
//      final File f = new File(graphLocation);
//      f.delete();
//    }
//  }
//
//  @Override
//  public Set<Class> getImplementations() {
//        return IMPLEMENTATION;
//    }
//
//  /**
//   * Determines if a test requires TinkerGraph persistence to be configured with graph location and format.
//   */
//  protected static boolean requiresPersistence(final Class<?> test, final String testMethodName) {
//    return test == GraphTest.class && testMethodName.equals("shouldPersistDataOnClose");
//  }
//
//  /**
//   * Determines if a test requires a different cardinality as the default or not.
//   */
//  protected static boolean requiresListCardinalityAsDefault(final LoadGraphWith.GraphData loadGraphWith,
//  final Class<?> test, final String testMethodName) {
//    return loadGraphWith == LoadGraphWith.GraphData.CREW
//    || (test == StarGraphTest.class && testMethodName.equals("shouldAttachWithCreateMethod"))
//    || (test == DetachedGraphTest.class && testMethodName.equals("testAttachableCreateMethod"));
//  }
//
//  /**
//   * Some tests require special configuration for TinkerGraph to properly configure the id manager.
//   */
//  protected TinkerGraph.DefaultIdManager selectIdMakerFromTest(final Class<?> test, final String testMethodName) {
//    if (test.equals(GraphTest.class)) {
//      final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
//                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingDoubleRepresentations");
//                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingIntegerRepresentations");
//                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingFloatRepresentations");
//                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentation");
//                add("shouldIterateVerticesWithNumericIdSupportUsingStringRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingDoubleRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingIntegerRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingFloatRepresentations");
//                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentation");
//                add("shouldIterateEdgesWithNumericIdSupportUsingStringRepresentations");
//            }};
//
//      final Set<String> testsThatNeedUuidIdManager = new HashSet<String>(){{
//                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentation");
//                add("shouldIterateVerticesWithUuidIdSupportUsingStringRepresentations");
//                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentation");
//                add("shouldIterateEdgesWithUuidIdSupportUsingStringRepresentations");
//            }};
//
//      if (testsThatNeedLongIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.LONG;
//      else if (testsThatNeedUuidIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.UUID;
//    }  else if (test.equals(IoEdgeTest.class)) {
//      final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
//                add("shouldReadWriteEdge[graphson-v1]");
//                add("shouldReadWriteDetachedEdgeAsReference[graphson-v1]");
//                add("shouldReadWriteDetachedEdge[graphson-v1]");
//                add("shouldReadWriteEdge[graphson-v2]");
//                add("shouldReadWriteDetachedEdgeAsReference[graphson-v2]");
//                add("shouldReadWriteDetachedEdge[graphson-v2]");
//            }};
//
//      if (testsThatNeedLongIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.LONG;
//    } else if (test.equals(IoVertexTest.class)) {
//      final Set<String> testsThatNeedLongIdManager = new HashSet<String>(){{
//                add("shouldReadWriteVertexWithBOTHEdges[graphson-v1]");
//                add("shouldReadWriteVertexWithINEdges[graphson-v1]");
//                add("shouldReadWriteVertexWithOUTEdges[graphson-v1]");
//                add("shouldReadWriteVertexNoEdges[graphson-v1]");
//                add("shouldReadWriteDetachedVertexNoEdges[graphson-v1]");
//                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v1]");
//                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v1]");
//                add("shouldReadWriteVertexWithBOTHEdges[graphson-v2]");
//                add("shouldReadWriteVertexWithINEdges[graphson-v2]");
//                add("shouldReadWriteVertexWithOUTEdges[graphson-v2]");
//                add("shouldReadWriteVertexNoEdges[graphson-v2]");
//                add("shouldReadWriteDetachedVertexNoEdges[graphson-v2]");
//                add("shouldReadWriteDetachedVertexAsReferenceNoEdges[graphson-v2]");
//                add("shouldReadWriteVertexMultiPropsNoEdges[graphson-v2]");
//            }};
//
//      if (testsThatNeedLongIdManager.contains(testMethodName))
//        return TinkerGraph.DefaultIdManager.LONG;
//    }
//
//    return TinkerGraph.DefaultIdManager.ANY;
//  }
//
//  /**
//   * Test that load with specific graph data can be configured with a specific id manager as the data type to
//   * be used in the test for that graph is known.
//   */
//  protected TinkerGraph.DefaultIdManager selectIdMakerFromGraphData(final LoadGraphWith.GraphData loadGraphWith) {
//    if (null == loadGraphWith) return TinkerGraph.DefaultIdManager.ANY;
//    if (loadGraphWith.equals(LoadGraphWith.GraphData.CLASSIC))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else if (loadGraphWith.equals(LoadGraphWith.GraphData.MODERN))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else if (loadGraphWith.equals(LoadGraphWith.GraphData.CREW))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else if (loadGraphWith.equals(LoadGraphWith.GraphData.GRATEFUL))
//    else if (loadGraphWith.equals(LoadGraphWith.GraphData.GRATEFUL))
//      return TinkerGraph.DefaultIdManager.INTEGER;
//    else
//      throw new IllegalStateException(String.format("Need to define a new %s for %s", TinkerGraph.IdManager.class.getName(), loadGraphWith.name()));
//  }
//}