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

package org.apache.s2graph.graphql.types

import org.apache.s2graph.core.Management.JsonModel._
import org.apache.s2graph.core._
import org.apache.s2graph.core.schema._
import org.apache.s2graph.graphql
import org.apache.s2graph.graphql.repository.GraphRepository
import sangria.schema._
import org.apache.s2graph.graphql.types.StaticTypes._

import scala.language.existentials

object S2SimpleType {
  def makeGraphElementField(cName: String, cType: String): Field[GraphRepository, Any] = {
    def makeField[A](name: String, cType: String, tpe: ScalarType[A]): Field[GraphRepository, Any] =
      Field(name,
        OptionType(tpe),
        description = Option("desc here"),
        resolve = c => FieldResolver.graphElement[A](name, cType, c)
      )

    cType match {
      case "boolean" | "bool" => makeField[Boolean](cName, cType, BooleanType)
      case "string" | "str" | "s" => makeField[String](cName, cType, StringType)
      case "int" | "integer" | "i" | "int32" | "integer32" => makeField[Int](cName, cType, IntType)
      case "long" | "l" | "int64" | "integer64" => makeField[Long](cName, cType, LongType)
      case "double" | "d" => makeField[Double](cName, cType, FloatType)
      case "float64" | "float" | "f" | "float32" => makeField[Double](cName, "double", FloatType)
      case _ => throw new RuntimeException(s"Cannot support data type: ${cType}")
    }
  }

  def makeInputFieldsOnService(service: Service): Seq[InputField[Any]] = {
    val inputFields = service.serviceColumns(false).map { serviceColumn =>
      val idField = InputField("id", toScalarType(serviceColumn.columnType))
      val propFields = serviceColumn.metasWithoutCache.filter(ColumnMeta.isValid).map { lm =>
        InputField(lm.name, OptionInputType(toScalarType(lm.dataType)))
      }

      val vertexMutateType = InputObjectType[Map[String, Any]](
        s"Input_${service.serviceName}_${serviceColumn.columnName}_vertex_mutate",
        description = "desc here",
        () => idField :: propFields
      )

      InputField[Any](serviceColumn.columnName, OptionInputType(ListInputType(vertexMutateType)))
    }

    inputFields
  }

  def makeInputFieldsOnLabel(label: Label): Seq[InputField[Any]] = {
    val propFields = label.labelMetaSet.toList.map { lm =>
      InputField(lm.name, OptionInputType(toScalarType(lm.dataType)))
    }

    val labelFields = List(
      InputField("timestamp", OptionInputType(LongType)),
      InputField("from", toScalarType(label.srcColumnType)),
      InputField("to", toScalarType(label.srcColumnType)),
      InputField("direction", OptionInputType(BothDirectionType))
    )

    labelFields.asInstanceOf[Seq[InputField[Any]]] ++ propFields.asInstanceOf[Seq[InputField[Any]]]
  }

  def makeServiceColumnFields(column: ServiceColumn, allLabels: Seq[Label]): List[Field[GraphRepository, Any]] = {
    val reservedFields = List("id" -> column.columnType)
    val columnMetasKv = column.metasWithoutCache.filter(ColumnMeta.isValid).map { columnMeta => columnMeta.name -> columnMeta.dataType }
    val connectedLabels = allLabels.filter { label =>
      label.srcColumn == column || label.tgtColumn == column
    }.distinct

    lazy val labelFields = connectedLabels.map { label => makeLabelField(label, allLabels) }.toList
    lazy val columnFields = (reservedFields ++ columnMetasKv).map { case (k, v) => makeGraphElementField(k, v) }
    lazy val labelFieldNameSet = connectedLabels.map(_.label).toSet

    columnFields.filterNot(cf => labelFieldNameSet(cf.name)) ++ labelFields
  }

  def makeServiceColumnType(column: ServiceColumn, allLabels: List[Label]): ObjectType[GraphRepository, Any] = {
    lazy val serviceColumnFields = makeServiceColumnFields(column, allLabels)
    lazy val ColumnType = ObjectType(
      s"${column.columnName}",
      () => fields[GraphRepository, Any](serviceColumnFields: _*)
    )

    ColumnType
  }

  def makeServiceField(service: Service, allLabels: List[Label])(implicit repo: GraphRepository): List[Field[GraphRepository, Any]] = {
    val columnsOnService = service.serviceColumns(false).toList.map { column =>
      val ColumnType = makeServiceColumnType(column, allLabels)
      Field(column.columnName,
        OptionType(ColumnType),
        arguments = Nil,
        description = Option("service column desc here"),
        resolve = c => None
      ): Field[GraphRepository, Any]
    }

    columnsOnService
  }

  def makeLabelField(label: Label, allLabels: Seq[Label]): Field[GraphRepository, Any] = {
    import sangria.relay._

    val labelReserved = Nil
    val labelProps = label.labelMetas.map { lm => lm.name -> lm.dataType }

    lazy val labelFields: List[Field[GraphRepository, Any]] =
      (labelReserved ++ labelProps).map { case (k, v) => makeGraphElementField(k, v) }

    val srcColumnType = makeServiceColumnType(label.srcColumn, allLabels.toList)
    val tgtColumnType = makeServiceColumnType(label.tgtColumn, allLabels.toList)

    val columns: List[Field[GraphRepository, Any]] = List(
      Field("source", srcColumnType, arguments = Nil, description = None, resolve = _ => null),
      Field("target", tgtColumnType, arguments = Nil, description = None, resolve = _ => null)
    )

    lazy val EdgeType = ObjectType(
      s"${label.label}",
      () => fields[GraphRepository, Any](
        (columns ++ labelFields): _*
      )
    )

    val ConnectionDefinition(_, labelConnection) =
      Connection.definition[GraphRepository, Connection, Option[_]]("label", OptionType(EdgeType))

    lazy val edgeTypeField: Field[GraphRepository, Any] = Field(
      s"${label.label}",
      labelConnection,
      arguments = Nil,
      description = Some("fetch edges"),
      resolve = { c => null }
    )

    edgeTypeField
  }
}

class S2SimpleType(repo: GraphRepository) {

  import S2SimpleType._

  implicit val graphRepository = repo

  def buildSimpleSchema(serviceName: String) = {
    val service = Service.findByName(serviceName).get
    lazy val serviceFields = makeServiceField(service, repo.labels())

    val ServiceType = ObjectType[GraphRepository, Any](
      service.serviceName,
      fields(serviceFields: _*)
    )

    ServiceType
  }
}
