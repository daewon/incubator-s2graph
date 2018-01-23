package org.apache.s2graph

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.http.scaladsl.server._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

import Console._
import scala.concurrent.Await
import scala.language.postfixOps

object Server extends App {

  implicit val actorSystem = ActorSystem("s2graphql-server")
  implicit val materializer = ActorMaterializer()

  import actorSystem.dispatcher
  import scala.concurrent.duration._

  logger("Starting GRAPHQL server...")

  val route: Route =
    (post & path("graphql")) {
      entity(as[spray.json.JsValue])(GraphQLServer.endpoint)
    } ~ {
      getFromResource("graphiql.html")
    }

  val port = sys.props.get("http.port").fold(8000)(_.toInt)
  Http().bindAndHandle(route, "0.0.0.0", port)


  def shutdown(): Unit = {
    logger("Terminating...", YELLOW)
    actorSystem.terminate()
    Await.result(actorSystem.whenTerminated, 10 seconds)
    logger("Terminated... Bye", YELLOW)
  }

  private def logger(message: String, color: String = GREEN): Unit = {
    println(color + message)
  }
}
