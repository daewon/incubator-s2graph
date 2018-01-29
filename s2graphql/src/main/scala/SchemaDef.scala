package org.apache.s2graph

import org.apache.s2graph.SchemaDef.MutationResponse
import org.apache.s2graph.core.types.{InnerVal, LabelWithDirection}
import org.apache.s2graph.core._
import play.api.libs.json.{JsNumber, JsString, JsValue}

import scala.reflect.ClassTag
import scala.util.Try

object SchemaDef {

  case class MutationResponse[T](result: Try[T])

}

/**
  * S2Graph GraphQL schema.
  *
  * When a Label or Service is created, the GraphQL schema is created dynamically.
  */

class SchemaDef(repo: GraphRepository) {

  import sangria.schema._
  import S2ManagementType._

  val serviceField: Field[GraphRepository, Any] = Field(
    "Services",
    ListType(ServiceType),
    arguments = List(ServiceNameArg),
    resolve = { c =>
      c.argOpt[String]("name") match {
        case Some(name) => c.ctx.allServices.filter(_.serviceName == name)
        case None => c.ctx.allServices
      }
    }
  )

  val labelField: Field[GraphRepository, Any] = Field(
    "Labels",
    ListType(LabelType),
    arguments = List(LabelNameArg),
    resolve = { c =>
      c.argOpt[String]("name") match {
        case Some(name) => c.ctx.allLabels.filter(_.label == name)
        case None => c.ctx.allLabels
      }
    }
  )

  val dirArguments = List(Argument("direction", OptionInputType(S2EnumDirectionType), "direction of connected edges", defaultValue = "out"))

  val serviceVertexFields: List[Field[GraphRepository, Any]] = repo.allServices.map { service =>
    val serviceId = service.id.get
    val connectedLabels = repo.allLabels.filter { lb =>
      lb.srcServiceId == serviceId || lb.tgtServiceId == serviceId
    }.distinct

    // label connected on services
    lazy val connectedLabelFields: List[Field[GraphRepository, Any]] = connectedLabels.map { label =>
      val labelColumns = List("from" -> label.srcColumnType, "to" -> label.tgtColumnType)
      val labelProps = label.labelMetas.map { lm => lm.name -> lm.dataType }

      def makeField[A](name: String, tpe: ScalarType[A])(resolver: Any => A): Field[GraphRepository, Any] = {
        Field(name, OptionType(tpe), description = Option("desc here"), resolve = c => resolver(c.value))
      }

      // from, to, props
      val edgeFields: List[Field[GraphRepository, Any]] = (labelColumns ++ labelProps).map { case (cName, cType) =>
        cType match {
          case "string" | "str" | "s" =>
            makeField[String](cName, StringType) { case e: S2EdgeLike =>
              val innerId = if (cName == "from") e.srcForVertex.innerId else if (cName == "to") e.tgtForVertex.innerId else e.propertyValue(cName).get.innerVal
              JSONParser.innerValToAny(innerId, cType).asInstanceOf[String]
            }
          case "double" | "d" | "float64" | "float" | "f" | "float32" | "long" | "l" | "int64" | "integer64" | "int" | "integer" | "i" | "int32" | "integer32" | "short" | "int16" | "integer16" =>
            makeField[BigDecimal](cName, BigDecimalType) { case e: S2EdgeLike =>
              val innerId = if (cName == "from") e.srcForVertex.innerId else if (cName == "to") e.tgtForVertex.innerId else e.propertyValue(cName).get.innerVal
              JSONParser.innerValToAny(innerId, InnerVal.LONG).asInstanceOf[Long]
            }
          case _ => throw new RuntimeException(s"cannot support data type: ${cType}")
        }
      }

      lazy val EdgeType = ObjectType(
        label.label,
        () => fields[GraphRepository, Any](edgeFields ++ connectedLabelFields: _*)
      )

      lazy val edgeTypeField: Field[GraphRepository, Any] = Field(
        label.label,
        ListType(EdgeType),
        arguments = dirArguments,
        description = Some("edges"),
        resolve = { c =>
          val dir = c.argOpt("direction").getOrElse("out")
          val vertex: S2VertexLike = c.value match {
            case v: S2VertexLike => v
            case e: S2Edge => if (dir == "out") e.tgtVertex else e.srcVertex
            case vp: PartialVertexParam =>
              if (dir == "out") {
                c.ctx.partialVertexParamToVertex(label.srcColumn, vp)
              } else {
                c.ctx.partialVertexParamToVertex(label.srcColumn, vp)
              }
          }

          c.ctx.getEdges(vertex, label, dir)
        }
      )
      edgeTypeField
    }

    val vertexIdField: Field[GraphRepository, Any] = Field(
      "id",
      PlayJsonPolyType.PolyType,
      description = Some("desc here"),
      resolve = _.value match {
        case v: PartialVertexParam => v.vid
        case _ => throw new RuntimeException("dead code")
      }
    )

    val VertexType = ObjectType(
      s"${service.serviceName}",
      fields[GraphRepository, Any](vertexIdField +: connectedLabelFields: _*)
    )

    Field(
      service.serviceName,
      ListType(VertexType),
      arguments = List(
        Argument("id", OptionInputType(PlayJsonPolyType.PolyType)),
        Argument("ids", OptionInputType(ListInputType(PlayJsonPolyType.PolyType)))
      ),
      description = Some(s"serviceName: ${service.serviceName}"),
      resolve = { c =>
        val id = c.argOpt[JsValue]("id").toSeq
        val ids = c.argOpt[List[JsValue]]("ids").toList.flatten
        val svc = c.ctx.findServiceByName(service.serviceName).get

        (id ++ ids).map { vid => PartialVertexParam(svc, vid) }
      }
    ): Field[GraphRepository, Any]
  }

  val queryFields = Seq(serviceField, labelField) ++ serviceVertexFields
  val S2QueryType = ObjectType[GraphRepository, Any](
    "Query",
    fields[GraphRepository, Any](queryFields: _*)
  )

  val mutationFields: List[Field[GraphRepository, Any]] = List(
    Field("createService",
      ServiceMutationResponseType,
      arguments = NameArg :: serviceArgOpts,
      resolve = c => MutationResponse(c.ctx.createService(c.args))
    ),
    Field("createLabel",
      LabelMutationResponseType,
      arguments = NameArg :: PropArg :: IndicesArg :: labelArgRequired ::: labelArgOpts,
      resolve = c => MutationResponse(c.ctx.createLabel(c.args))
    ),
    Field("addEdge",
      OptionType(EdgeMutateResponseType),
      arguments = EdgeArg,
      resolve = c => c.ctx.addEdge(c.args)
    ),
    Field("addEdges",
      ListType(EdgeMutateResponseType),
      arguments = EdgesArg,
      resolve = c => c.ctx.addEdges(c.args)
    )
  )

  val S2MutationType = ObjectType("Management", fields[GraphRepository, Any](
    mutationFields: _*
  ))

  val S2GraphSchema = Schema(S2QueryType, Option(S2MutationType))
}
