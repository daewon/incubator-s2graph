import org.apache.s2graph.core._
import org.apache.s2graph.core.mysqls.{Label, Service, ServiceColumn}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.types.{InnerVal, LabelWithDirection, VertexId}
import org.apache.tinkerpop.gremlin.structure.Graph
import play.api.libs.json._
import sangria.renderer.SchemaRenderer

object SchemaDef {

  import sangria.schema._

  val ID = Argument("id", StringType, description = "id of the vertex")
  val QueryType = ObjectType[GraphRepository, Unit](
    "Query",
    fields[GraphRepository, Unit](
      Field(
        "StartVertex",
        ListType(StringType),
        arguments = List(ID),
        resolve = ctx => Nil)
    )
  )

  def S2GraphSchema = Schema(QueryType, None)

  println("-" * 80)
  println(SchemaRenderer.renderSchema(S2GraphSchema))
}
