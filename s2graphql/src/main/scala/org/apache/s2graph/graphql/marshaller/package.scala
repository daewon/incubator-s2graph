package org.apache.s2graph.graphql

import org.apache.s2graph.core.Management.JsonModel._
import org.apache.s2graph.graphql.types.S2Type._
import org.apache.s2graph.graphql.types.S2ManagementType._
import sangria.marshalling._

package object marshaller {

  def unwrap(map: Map[String, Any]): Map[String, Any] = map.map { case (k, v: Some[_]) => k -> v.get }

  implicit object IndexFromInput extends FromInput[Index] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, Any]]
      Index(input("name").asInstanceOf[String], input("propNames").asInstanceOf[Seq[String]])
    }
  }

  implicit object PropFromInput extends FromInput[Prop] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, Any]]

      val name = input("name").asInstanceOf[String]
      val defaultValue = input("defaultValue").asInstanceOf[String]
      val dataType = input("dataType").asInstanceOf[String]
      val storeInGlobalIndex = input("storeInGlobalIndex").asInstanceOf[Boolean]

      Prop(name, defaultValue, dataType, storeInGlobalIndex)
    }
  }

  implicit object PartialServiceColumnFromInput extends FromInput[PartialServiceColumn] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = PartialServiceColumnsFromInput.fromResult(node).head
  }

  implicit object PartialServiceColumnsFromInput extends FromInput[Vector[PartialServiceColumn]] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = unwrap(node.asInstanceOf[Map[String, Any]])

      val partialServiceColumns = input.map { case (serviceName, serviceColumnMap) =>
        val innerMap = serviceColumnMap.asInstanceOf[Map[String, Any]]
        val columnName = innerMap("columnName").asInstanceOf[String]
        val props = innerMap.get("props").toSeq.flatMap { case v: Vector[_] =>
          v.map(PropFromInput.fromResult)
        }

        PartialServiceColumn(serviceName, columnName, props)
      }

      partialServiceColumns.toVector
    }
  }

  implicit object PartialServiceVertexParamFromInput extends FromInput[Vector[PartialServiceVertexParam]] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val inputMap = node.asInstanceOf[Map[String, marshaller.Node]]

      val ret = inputMap.toVector.map { case (columnName, node) =>
        val param = PartialVertexFromInput.fromResult(node)
        PartialServiceVertexParam(columnName, param)
      }

      ret
    }
  }

  implicit object PartialVertexFromInput extends FromInput[PartialVertexParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {

      val _inputMap = node.asInstanceOf[Option[Map[String, Any]]]
      val inputMap = _inputMap.get

      val id = inputMap("id")
      val ts = inputMap.get("timestamp") match {
        case Some(Some(v)) => v.asInstanceOf[Long]
        case _ => System.currentTimeMillis()
      }
      val props = inputMap.get("props") match {
        case Some(Some(v)) => v.asInstanceOf[Map[String, Option[Any]]].filter(_._2.isDefined).mapValues(_.get)
        case _ => Map.empty[String, Any]
      }

      PartialVertexParam(ts, id, props)
    }
  }

  implicit object PartialEdgeFromInput extends FromInput[PartialEdgeParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val inputMap = node.asInstanceOf[Map[String, Any]]

      val from = inputMap("from")
      val to = inputMap("to")

      val ts = inputMap.get("timestamp") match {
        case Some(Some(v)) => v.asInstanceOf[Long]
        case _ => System.currentTimeMillis()
      }

      val dir = inputMap.get("direction") match {
        case Some(Some(v)) => v.asInstanceOf[String]
        case _ => "out"
      }

      val props = inputMap.get("props") match {
        case Some(Some(v)) => v.asInstanceOf[Map[String, Option[Any]]].filter(_._2.isDefined).mapValues(_.get)
        case _ => Map.empty[String, Any]
      }

      PartialEdgeParam(ts, from, to, dir, props)
    }
  }

}