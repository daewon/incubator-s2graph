package org.apache.s2graph

import org.apache.s2graph.S2ManagementType.LabelServiceProp
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.S2Graph
import org.apache.s2graph.core.mysqls.{Label, LabelIndex, Service}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.types.HBaseType
import play.api.libs.json._
import sangria.schema.{Action, Args}

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success, Try}

case class PartialVertex(id: String)

class GraphRepository(val graph: S2Graph) {
  val management = graph.management
  val parser = new RequestParser(graph)

  def insertEdge(args: Args): Future[Boolean] = {
    Future.successful(true)
  }

  def createService(args: Args): Try[Service] = {
    val serviceName = args.arg[String]("name")

    Service.findByName(serviceName) match {
      case Some(_) => Failure(new RuntimeException(s"Service (${serviceName}) already exists"))
      case None =>
        val cluster = args.argOpt[String]("cluster").getOrElse(parser.DefaultCluster)
        val hTableName = args.argOpt[String]("hTableName").getOrElse(s"${serviceName}-${parser.DefaultPhase}")
        val preSplitSize = args.argOpt[Int]("preSplitSize").getOrElse(1)
        val hTableTTL = args.argOpt[Int]("hTableTTL")
        val compressionAlgorithm = args.argOpt[String]("compressionAlgorithm").getOrElse(parser.DefaultCompressionAlgorithm)

        val serviceTry = management
          .createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL, compressionAlgorithm)

        serviceTry
    }
  }

  def createLabel(args: Args): Try[Label] = {
    val labelName = args.arg[String]("name")

    val srcServiceProp = args.arg[LabelServiceProp]("sourceService")
    val tgtServiceProp = args.arg[LabelServiceProp]("targetService")

    val allProps = args.argOpt[Vector[Prop]]("props").getOrElse(Vector.empty)
    val indices = args.argOpt[Vector[Index]]("indices").getOrElse(Vector.empty)
    println(indices)

    val serviceName = args.argOpt[String]("serviceName").getOrElse(tgtServiceProp.name)
    val consistencyLevel = args.argOpt[String]("consistencyLevel").getOrElse("weak")
    val hTableName = args.argOpt[String]("hTableName")
    val hTableTTL = args.argOpt[Int]("hTableTTL")
    val schemaVersion = args.argOpt[String]("schemaVersion").getOrElse(HBaseType.DEFAULT_VERSION)
    val isAsync = args.argOpt("isAsync").getOrElse(false)
    val compressionAlgorithm = args.argOpt[String]("compressionAlgorithm").getOrElse(parser.DefaultCompressionAlgorithm)
    val isDirected = args.argOpt[Boolean]("isDirected").getOrElse(true)
    val options = args.argOpt[String]("options") // TODO: support option type

    val labelTry: scala.util.Try[Label] = management.createLabel(
      labelName,
      srcServiceProp.name, srcServiceProp.columnName, srcServiceProp.dataType,
      tgtServiceProp.name, tgtServiceProp.columnName, tgtServiceProp.dataType,
      isDirected,
      serviceName,
      indices,
      allProps,
      consistencyLevel,
      hTableName,
      hTableTTL,
      schemaVersion,
      isAsync,
      compressionAlgorithm,
      options)

    labelTry match {
      case Success(label) => Option(label)
      case Failure(ex) =>
        println(ex)
        None
    }

    labelTry
  }

  def allServices: List[Service] = Service.findAll()

  def findServiceByName(name: String): Option[Service] = Service.findByName(name)

  def allLabels: List[Label] = Label.findAll()

  def findLabelByName(name: String): Option[Label] = Label.findByName(name)
}


