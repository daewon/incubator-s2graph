package org.apache.s2graph.core.tinkerpop

import java.io.File
import java.util

import org.apache.commons.configuration.Configuration

import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, ServiceColumn}
import org.apache.s2graph.core.types.HBaseType._
import org.apache.s2graph.core.types.{VertexId, HBaseType, InnerVal}
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData
import org.apache.tinkerpop.gremlin.structure.{T, Element, Graph, Edge}
import org.apache.tinkerpop.gremlin.{LoadGraphWith, AbstractGraphProvider}
import sun.security.provider.certpath.Vertex
import scala.collection.JavaConverters._
import scala.util.Random

import com.typesafe.config.ConfigFactory

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
    val dbUrl =
      if (config.hasPath("db.default.url")) config.getString("db.default.url")
      else "jdbc:mysql://default:3306/graph_dev"
    val m = new java.util.HashMap[String, AnyRef]()
    m.put(Graph.GRAPH, classOf[S2Graph].getName)
    m.put("db.default.url", dbUrl)
    m.put("db.default.driver", "com.mysql.jdbc.Driver")
    m
  }

  override def clear(graph: Graph, configuration: Configuration): Unit =
    if (graph != null) {
      val s2Graph = graph.asInstanceOf[S2Graph]
      if (s2Graph.isRunning) {
        val labels = Label.findAll()
        labels.groupBy(_.hbaseTableName).values.foreach { labelsWithSameTable =>
          labelsWithSameTable.headOption.foreach { label =>
            s2Graph.management.deleteStorage(label.label)
          }
        }
        s2Graph.shutdown()
        logger.info(s"S2Graph Shutdown")
      }
    }

  override def getImplementations: util.Set[Class[_]] = S2GraphProvider.Implementation.asJava

  override def loadGraphData(graph: Graph, loadGraphWith: LoadGraphWith, testClass: Class[_], testName: String): Unit = {
    val s2Graph = graph.asInstanceOf[S2Graph]
    val mnt = s2Graph.getManagement()

    val service = s2Graph.DefaultService
    val column = s2Graph.DefaultColumn

    val personColumn = Management.createServiceColumn(service.serviceName, "person", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "0", "integer"), Prop("location", "-", "string")))
    val softwareColumn = Management.createServiceColumn(service.serviceName, "software", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("lang", "-", "string")))
    val productColumn = Management.createServiceColumn(service.serviceName, "product", "integer", Nil)
    val dogColumn = Management.createServiceColumn(service.serviceName, "dog", "integer", Nil)
//    val vertexColumn = Management.createServiceColumn(service.serviceName, "vertex", "integer", Seq(Prop(T.id.toString, "-1", "integer"), Prop("name", "-", "string"), Prop("age", "-1", "integer"), Prop("lang", "scala", "string")))

    val created = mnt.createLabel("created", service.serviceName, "person", "integer", service.serviceName, "software", "integer",
      true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)

    val knows = mnt.createLabel("knows", service.serviceName, "person", "integer", service.serviceName, "person", "integer",
      true, service.serviceName, Nil,
      Seq(
        Prop("weight", "0.0", "double"),
        Prop("data", "-", "string"),
        Prop("year", "-1", "integer"),
        Prop("boolean", "false", "boolean"),
        Prop("float", "0.0", "float"),
        Prop("double", "0.0", "double"),
        Prop("long", "0.0", "long"),
        Prop("string", "-", "string"),
        Prop("integer", "-", "integer"),
        Prop("aKey", "", "string")
      ),
      "strong",
      None,
      None)

    val bought = mnt.createLabel("bought", service.serviceName, "person", "integer", service.serviceName, "product", "integer",
      true, service.serviceName, Nil, Seq(Prop("x", "-", "string"), Prop("y", "-", "string")), "strong", None, None)

    val test = mnt.createLabel("test", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Seq(Prop("xxx", "-", "string")), "weak", None, None)

    val self = mnt.createLabel("self", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil, "weak", None, None)

    val friends = mnt.createLabel("friends", service.serviceName, column.columnName, column.columnType, service.serviceName, column.columnName, column.columnType,
      true, service.serviceName, Nil, Nil, "weak", None, None)

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
  //  override def loadGraphData(graph: Graph, loadGraphWith: LoadGraphWith, testClass: Class[_], testName: String): Unit = {
//    /*
//      -- from 1
//      {"id":1,"label":"vertex",
//          "outE":
//            {"created":[
//                {"id":9,"inV":3,"properties":{"weight":0.4}}],
//             "knows":[
//                {"id":7,"inV":2,"properties":{"weight":0.5}},
//                {"id":8,"inV":4,"properties":{"weight":1.0}}]},
//      "properties":{"name":[{"id":0,"value":"marko"}],"age":[{"id":2,"value":29}]}}
//
//      -- from 2
//      {"id":2,"label":"vertex",
//          "inE":
//            {"knows":[
//                {"id":7,"outV":1,"properties":{"weight":0.5}}]},
//      "properties":{"name":[{"id":3,"value":"vadas"}],"age":[{"id":4,"value":27}]}}
//
//      -- from 3
//      {"id":3,"label":"vertex",
//          "inE":
//            {"created":[
//                {"id":9,"outV":1,"properties":{"weight":0.4}},
//                {"id":11,"outV":4,"properties":{"weight":0.4}},
//                {"id":12,"outV":6,"properties":{"weight":0.2}}]},
//      "properties":{"name":[{"id":5,"value":"lop"}],"lang":[{"id":6,"value":"java"}]}}
//
//      -- from 4
//      {"id":4,"label":"vertex",
//          "inE":
//            {"knows":[
//                {"id":8,"outV":1,"properties":{"weight":1.0}}]},
//          "outE":
//            {"created":[
//                {"id":10,"inV":5,"properties":{"weight":1.0}},
//                {"id":11,"inV":3,"properties":{"weight":0.4}}]},
//      "properties":{"name":[{"id":7,"value":"josh"}],"age":[{"id":8,"value":32}]}}
//
//      -- from 5
//      {"id":5,"label":"vertex",
//          "inE":
//            {"created":[
//                {"id":10,"outV":4,"properties":{"weight":1.0}}]},
//      "properties":{"name":[{"id":9,"value":"ripple"}],"lang":[{"id":10,"value":"java"}]}}
//
//      -- from 6
//      {"id":6,"label":"vertex",
//          "outE":
//            {"created":[
//                {"id":12,"inV":3,"properties":{"weight":0.2}}]},
//      "properties":{"name":[{"id":11,"value":"peter"}],"age":[{"id":12,"value":35}]}}
//    */
////    graph.traversal.V.has("name", outVertexName).outE(edgeLabel).as("e").inV.has("name", inVertexName).select[Edge]("e").next.id;
//    val s2Graph = graph.asInstanceOf[S2Graph]
//    val mnt = s2Graph.getManagement()
//    val service = mnt.createService("s2graph", "localhost", "s2graph", 0, None).get
//    val serviceColumnString = s"${service.serviceName}::vertex"
//    Management.createServiceColumn(service.serviceName, "vertex", "integer", Seq(Prop("name", "-", "string"), Prop("age", "-1", "integer"), Prop("lang", "scala", "string")))
//
//    val created = mnt.createLabel("created", service.serviceName, "vertex", "integer", service.serviceName, "vertex", "integer",
//      true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)
//
//    val knows = mnt.createLabel("knows", service.serviceName, "vertex", "integer", service.serviceName, "vertex", "integer",
//      true, service.serviceName, Nil, Seq(Prop("weight", "0.0", "double")), "strong", None, None)
//
//    val vertex1 = s2Graph.addVertex(T.label, serviceColumnString, T.id, Int.box(1), "name", "marko", "age", Int.box(29))
//    val vertex2 = s2Graph.addVertex(T.label, serviceColumnString, T.id, Int.box(2), "name", "vadas", "age", Int.box(27))
//    val vertex3 = s2Graph.addVertex(T.label, serviceColumnString, T.id, Int.box(3), "name", "lop", "age", Int.box(27), "lang", "java")
//    val vertex4 = s2Graph.addVertex(T.label, serviceColumnString, T.id, Int.box(4), "name", "josh", "age", Int.box(32))
//    val vertex5 = s2Graph.addVertex(T.label, serviceColumnString, T.id, Int.box(5), "name", "ripple", "lang", "java")
//    val vertex6 = s2Graph.addVertex(T.label, serviceColumnString, T.id, Int.box(6), "name", "peter", "age", Int.box(35))
//
//
//    /**
//     * from 1
//     */
//    s2Graph.addEdgeInner(
//      vertex1.asInstanceOf[S2Vertex],
//      vertex3.asInstanceOf[S2Vertex],
//      "created",
//      "out",
//      Map("weight" -> Double.box(0.4))
//    )
////
////    s2Graph.addEdgeInner(
////      vertex1.asInstanceOf[S2Vertex],
////      vertex2.asInstanceOf[S2Vertex],
////      "knows",
////      "out",
////      Map("weight" -> Double.box(0.5))
////    )
////
////    s2Graph.addEdgeInner(
////      vertex1.asInstanceOf[S2Vertex],
////      vertex4.asInstanceOf[S2Vertex],
////      "knows",
////      "out",
////      Map("weight" -> Double.box(1.0))
////    )
////
////    /**
////     * from  2
////     */
////
////    s2Graph.addEdgeInner(
////      vertex2.asInstanceOf[S2Vertex],
////      vertex1.asInstanceOf[S2Vertex],
////      "knows",
////      "in",
////      Map("weight" -> Double.box(0.5))
////    )
////
////    /**
////     * from  3
////     */
////
////    s2Graph.addEdgeInner(
////      vertex3.asInstanceOf[S2Vertex],
////      vertex1.asInstanceOf[S2Vertex],
////      "created",
////      "in",
////      Map("weight" -> Double.box(0.4))
////    )
////
////    s2Graph.addEdgeInner(
////      vertex3.asInstanceOf[S2Vertex],
////      vertex4.asInstanceOf[S2Vertex],
////      "created",
////      "in",
////      Map("weight" -> Double.box(0.4))
////    )
////
////    s2Graph.addEdgeInner(
////      vertex3.asInstanceOf[S2Vertex],
////      vertex6.asInstanceOf[S2Vertex],
////      "created",
////      "in",
////      Map("weight" -> Double.box(0.2))
////    )
////
////    /**
////     * from  4
////     */
////
////    s2Graph.addEdgeInner(
////      vertex4.asInstanceOf[S2Vertex],
////      vertex1.asInstanceOf[S2Vertex],
////      "knows",
////      "in",
////      Map("weight" -> Double.box(1.0))
////    )
////
////    s2Graph.addEdgeInner(
////      vertex4.asInstanceOf[S2Vertex],
////      vertex5.asInstanceOf[S2Vertex],
////      "created",
////      "out",
////      Map("weight" -> Double.box(1.0))
////    )
////
////    s2Graph.addEdgeInner(
////      vertex4.asInstanceOf[S2Vertex],
////      vertex3.asInstanceOf[S2Vertex],
////      "created",
////      "out",
////      Map("weight" -> Double.box(0.4))
////    )
////
////    /**
////     * from 5
////     */
////
////    s2Graph.addEdgeInner(
////      vertex5.asInstanceOf[S2Vertex],
////      vertex4.asInstanceOf[S2Vertex],
////      "created",
////      "in",
////      Map("weight" -> Double.box(1.0))
////    )
////
////    /**
////     * from 6
////     */
////    s2Graph.addEdgeInner(
////      vertex6.asInstanceOf[S2Vertex],
////      vertex3.asInstanceOf[S2Vertex],
////      "created",
////      "out",
////      Map("weight" -> Double.box(0.2))
////    )
//
//    //    super.loadGraphData(graph, loadGraphWith, testClass, testName)
//  }
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