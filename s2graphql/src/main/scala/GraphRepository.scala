import org.apache.s2graph.core.S2Graph
import play.api.libs.json._
import scala.concurrent._

import scala.concurrent.ExecutionContext.Implicits.global

case class PartialVertex(id: String)

class GraphRepository(val graph: S2Graph) {
}
