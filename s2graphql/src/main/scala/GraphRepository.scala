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
import scala.util.{Failure, Success}

case class PartialVertex(id: String)

class GraphRepository(val graph: S2Graph) {
  val management = graph.management
  val rq = new RequestParser(graph)

  def createService(args: Args): Option[Service] = {
    val serviceName = args.arg[String]("name")
    val cluster = args.argOpt[String]("cluster").getOrElse(rq.DefaultCluster)
    val hTableName = args.argOpt[String]("hTableName").getOrElse(s"${serviceName}-${rq.DefaultPhase}")
    val preSplitSize = args.argOpt[Int]("preSplitSize").getOrElse(1)
    val hTableTTL = args.argOpt[Int]("hTableTTL")
    val compressionAlgorithm = args.argOpt[String]("compressionAlgorithm").getOrElse(rq.DefaultCompressionAlgorithm)

    val serviceTry = management
      .createService(serviceName, cluster, hTableName, preSplitSize, hTableTTL, compressionAlgorithm)

    serviceTry.toOption
  }

  def createLabel(args: Args): Option[Label] = {
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
    val compressionAlgorithm = args.argOpt[String]("compressionAlgorithm").getOrElse(rq.DefaultCompressionAlgorithm)
    val isDirected = args.argOpt[Boolean]("isDirected").getOrElse(true)

    // TODO: OptionType
    val options = args.argOpt[String]("options")

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
  }

  def allServices: List[Service] = Service.findAll()

  def findServiceByName(name: String): Option[Service] = Service.findByName(name)

  def allLabels: List[Label] = Label.findAll()

  def findLabelByName(name: String): Option[Label] = Label.findByName(name)
}


