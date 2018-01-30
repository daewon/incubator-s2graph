package org.apache.s2graph

/**
  * S2Graph GraphQL schema.
  *
  * When a Label or Service is created, the GraphQL schema is created dynamically.
  */
class SchemaDef(s2Type: S2Type) {

  import sangria.schema._

  val S2QueryType = ObjectType[GraphRepository, Any]("Query", fields(s2Type.queryFields: _*))

  val S2MutationType = ObjectType[GraphRepository, Any]("Mutation", fields(s2Type.mutationFields: _*))

  val S2GraphSchema = Schema(S2QueryType, Option(S2MutationType))
}
