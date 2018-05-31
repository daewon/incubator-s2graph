/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.s2graph.graphql

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.s2graph
import org.apache.s2graph.core.Management.JsonModel.Prop
import org.apache.s2graph.core.schema.{Label, Schema, Service}
import org.apache.s2graph.core.rest.RequestParser
import org.apache.s2graph.core.{Management, S2Graph}
import org.apache.s2graph.graphql
import org.apache.s2graph.graphql.repository.GraphRepository
import org.apache.s2graph.graphql.types.SchemaDef
import play.api.libs.json._
import sangria.ast.{Document, SchemaDefinition}
import sangria.execution.Executor
import sangria.execution.deferred.DeferredResolver
import sangria.renderer.SchemaRenderer
import sangria.schema.Schema

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util._

object TestGraph {
}

trait TestGraph {

  def open(): Unit

  def cleanup(): Unit

  def repository: GraphRepository

  def schema: Schema[GraphRepository, Any]

  def showSchema: String

  import GraphRepository._

  val resolver: DeferredResolver[GraphRepository] = DeferredResolver.fetchers(vertexFetcher, edgeFetcher)

  def queryAsJs(query: Document, _schema: Schema[GraphRepository, Any] = schema): JsValue = {
    implicit val playJsonMarshaller = sangria.marshalling.playJson.PlayJsonResultMarshaller
    Await.result(
      Executor.execute(_schema, query, repository, deferredResolver = resolver),
      Duration("10 sec")
    )
  }
}

class EmptyGraph(config: Config) extends TestGraph {
  org.apache.s2graph.core.schema.Schema.apply(config)

  lazy val graph = new S2Graph(config)(scala.concurrent.ExecutionContext.Implicits.global)
  lazy val management = new Management(graph)
  lazy val s2Repository = new GraphRepository(graph)

  override def cleanup(): Unit = graph.shutdown(true)

  override def schema: Schema[GraphRepository, Any] = new SchemaDef(s2Repository).S2GraphSchema

  override def showSchema: String = SchemaRenderer.renderSchema(schema)

  override def repository: GraphRepository = s2Repository

  override def open(): Unit = {
    org.apache.s2graph.core.schema.Schema.shutdown(true)
  }
}

class BasicGraph(config: Config) extends EmptyGraph(config) {
  // Init test data
  val serviceName = "kakao"
  val labelName = "friends"
  val columnName = "user"

  Management.deleteService(serviceName)
  val serviceTry: Try[Service] =
    management.createService(
      serviceName,
      "localhost",
      s"${serviceName}_table",
      1,
      None
    )

  val serviceColumnTry = serviceTry.map { _ =>
    management.createServiceColumn(
      serviceName,
      columnName,
      "string",
      List(
        Prop("age", "0", "int"),
        Prop("gender", "", "string")
      )
    )
  }

  Management.deleteLabel(labelName)
  val labelTry: Try[Label] =
    management.createLabel(
      labelName,
      serviceName, columnName, "string",
      serviceName, columnName, "string",
      serviceName,
      Nil,
      Seq(Prop("score", "0", "int")),
      true,
      "strong"
    )
}

class UserActivitySimpleGraph(config: Config) extends EmptyGraph(config) {
  val serviceName = "KAKAO"

  val userColumnName = "User"
  val activityColumnName = "Activity"
  val userActivityLabelName = "UserActivity"
  val friendLabelName = "Friends"

  Management.deleteService(serviceName)
  val serviceTry: Try[Service] =
    management.createService(
      serviceName,
      "localhost",
      s"${serviceName}_table",
      1,
      None
    )

  val userColumn = management.createServiceColumn(
    serviceName,
    userColumnName,
    "string",
    List(
      Prop("age", "0", "int"),
      Prop("gender", "", "string")
    )
  )

  val activityColumn = management.createServiceColumn(
    serviceName,
    userColumnName,
    "string",
    List(
      Prop("type", "0", "string")
    )
  )

  Management.deleteLabel(userActivityLabelName)
  val labelTry: Try[Label] =
    management.createLabel(
      userActivityLabelName,
      serviceName, userColumnName, "string",
      serviceName, activityColumnName, "string",
      serviceName,
      Nil,
      Seq(Prop("score", "0", "int"), Prop("type", "0", "string")),
      true,
      "strong"
    )

  Management.deleteLabel(friendLabelName)
  val friendLabelTry: Try[Label] =
    management.createLabel(
      friendLabelName,
      serviceName, userColumnName, "string",
      serviceName, userColumnName, "string",
      serviceName,
      Nil,
      Seq(Prop("since", "0", "int")),
      true,
      "strong"
    )
}
