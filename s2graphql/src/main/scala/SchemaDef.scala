package org.apache.s2graph

import org.apache.s2graph.SchemaDef.MutationResponse
import org.apache.s2graph.core.mysqls.Label
import sangria.renderer.SchemaRenderer

import scala.util.Try


object SchemaDef {

  case class MutationResponse[T](result: Try[T])

}

/**
  * S2Graph GraphQL schema.
  *
  * When a Label or Service is created, the GraphQL schema is created dynamically.
  */

class SchemaDef() {

  import sangria.schema._
  import S2ManagementType._

  val S2QueryType = ObjectType[GraphRepository, Unit](
    "Query",
    fields[GraphRepository, Unit](
      Field("Services",
        ListType(ServiceType),
        arguments = List(ServiceNameArg),
        resolve = { c =>
          c.argOpt[String]("name") match {
            case Some(name) => c.ctx.allServices.filter(_.serviceName == name)
            case None => c.ctx.allServices
          }
        }
      ),
      Field("Labels",
        ListType(LabelType),
        arguments = List(LabelNameArg),
        resolve = { c =>
          c.argOpt[String]("name") match {
            case Some(name) => c.ctx.allLabels.filter(_.label == name)
            case None => c.ctx.allLabels
          }
        }
      )
    )
  )

  val mutationFields: List[Field[GraphRepository, Unit]] = List(
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

  val S2MutationType = ObjectType("Management", fields[GraphRepository, Unit](
    mutationFields: _*
  ))

  val S2GraphSchema = Schema(S2QueryType, Option(S2MutationType))
}
