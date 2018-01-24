package org.apache.s2graph

import org.apache.s2graph.core.mysqls.Label
import sangria.renderer.SchemaRenderer

import scala.util.Try

object SchemaDef {

  import sangria.schema._
  import S2ManagementType._

  val ID = Argument("id", StringType, description = "id of the vertex")
  val Name = Argument("name", StringType, description = "name args")

  val S2QueryType = ObjectType[GraphRepository, Unit](
    "Query",
    fields[GraphRepository, Unit](
      Field("Services", ListType(ServiceType), resolve = c => c.ctx.allServices),
      Field(
        "Service",
        OptionType(ServiceType),
        arguments = List(Name),
        resolve = { c =>
          val name = c.arg[String]("name")
          c.ctx.findServiceByName(name)
        }
      ),
      Field("Labels", ListType(LabelType), resolve = c => c.ctx.allLabels),
      Field(
        "Label",
        OptionType(LabelType),
        arguments = List(Name),
        resolve = { c =>
          val name = c.arg[String]("name")
          c.ctx.findLabelByName(name)
        }
      )
    )
  )

  case class MutationResponse[T](result: Try[T])

  def S2MutationType = ObjectType("Mutation", fields[GraphRepository, Unit](
    Field("createService",
      ServiceMutationResponseType,
      arguments = NameArg :: serviceArgOpts,
      resolve = c => MutationResponse(c.ctx.createService(c.args))
    ),
    Field("createLabel",
      LabelMutationResponseType,
      arguments = NameArg :: PropArg :: IndicesArg :: labelArgRequired ::: labelArgOpts,
      resolve = c => MutationResponse(c.ctx.createLabel(c.args))
    )
  ))

  def S2GraphSchema() = Schema(S2QueryType, Option(S2MutationType))
}
