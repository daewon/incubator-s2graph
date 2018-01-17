import java.util.concurrent.Executors

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import com.typesafe.config.ConfigFactory
import org.apache.s2graph.core.S2Graph
import sangria.ast.Document
import sangria.execution._
import sangria.marshalling.sprayJson._
import sangria.parser.QueryParser
import spray.json.{JsObject, JsString, JsValue}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

object GraphQLServer {

  // init s2graph with config
  val numOfThread = Runtime.getRuntime.availableProcessors()
  val threadPool = Executors.newFixedThreadPool(numOfThread * 2)

  val ec = ExecutionContext.fromExecutor(threadPool)
  val config = ConfigFactory.load()

  val s2graph = new S2Graph(config)(ec)
  val s2Repository = new GraphRepository(s2graph)

  def endpoint(requestJSON: spray.json.JsValue)(implicit e: ExecutionContext): Route = {

    val spray.json.JsObject(fields) = requestJSON

    val spray.json.JsString(query) = fields("query")

    val operation = fields.get("operationName") collect {
      case spray.json.JsString(op) => op
    }

    val vars = fields.get("variables") match {
      case Some(obj: spray.json.JsObject) => obj
      case _ => spray.json.JsObject.empty
    }

    QueryParser.parse(query) match {
      case Success(queryAst) => complete(executeGraphQLQuery(queryAst, operation, vars))
      case Failure(error) => complete(BadRequest, spray.json.JsObject("error" -> JsString(error.getMessage)))
    }
  }

  private def executeGraphQLQuery(query: Document, op: Option[String], vars: JsObject)(implicit e: ExecutionContext) = {
    Executor.execute(
      SchemaDef.S2GraphSchema,
      query,
      s2Repository,
      variables = vars,
      operationName = op
    )
      .map((res: spray.json.JsValue) => OK -> res)
      .recover {
        case error: QueryAnalysisError => BadRequest -> error.resolveError
        case error: ErrorWithResolver => InternalServerError -> error.resolveError
      }
  }
}
