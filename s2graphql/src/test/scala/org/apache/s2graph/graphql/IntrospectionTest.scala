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

import com.typesafe.config.ConfigFactory
import org.apache.s2graph.graphql.types.SchemaDef
import org.scalatest._
import play.api.libs.json._

class IntrospectionTest extends FunSuite with Matchers with BeforeAndAfterAll {
  var activityGraph: UserActivitySimpleGraph = _

  override def beforeAll = {
    val config = ConfigFactory.load()
    activityGraph = new UserActivitySimpleGraph(config)
    activityGraph.open()
  }

  override def afterAll(): Unit = {
    activityGraph.cleanup()
  }

  val introspectionQuery = sangria.introspection.introspectionQuery

  test("Introspection for UserActivity Graph") {

    val schema = new SchemaDef(activityGraph.s2Repository).S2GraphSimpleSchema(activityGraph.serviceName)
    val retJs = activityGraph.queryAsJs(introspectionQuery, schema)

    new PrintWriter("schema.json") {
      write(Json.prettyPrint(retJs))
      close()
    }

    true
  }
}
