package org.apache.s2graph

import sangria.ast._
import sangria.schema._
import sangria.validation.ValueCoercionViolation

// https://gist.github.com/OlegIlyenko/5b96f4b54f656aac226d3c4bc33fd2a6

object SangriaPlayJsonInputSerializer {

  import sangria.ast
  import sangria.execution.Executor
  import sangria.marshalling.{InputUnmarshaller, ScalarValueInfo, ArrayMapBuilder, ResultMarshaller}
  import sangria.schema._
  import sangria.validation.{ValueCoercionViolation, IntCoercionViolation, BigIntCoercionViolation}
  //  import spray.json._
  import play.api.libs.json._
  import sangria.macros._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit object CustomSprayJsonResultMarshaller extends ResultMarshaller {
    type Node = JsValue
    type MapBuilder = ArrayMapBuilder[Node]

    def emptyMapNode(keys: Seq[String]) = new ArrayMapBuilder[Node](keys)

    def addMapNodeElem(builder: MapBuilder, key: String, value: Node, optional: Boolean) = builder.add(key, value)

    def mapNode(builder: MapBuilder) = JsObject(builder.toMap)

    def mapNode(keyValues: Seq[(String, JsValue)]) = JsObject(keyValues.toMap)

    def arrayNode(values: Vector[JsValue]) = JsArray(values.toVector)

    def optionalArrayNodeValue(value: Option[JsValue]) = value match {
      case Some(v) ⇒ v
      case None ⇒ nullNode
    }

    def scalarNode(value: Any, typeName: String, info: Set[ScalarValueInfo]) = value match {
      case v: String ⇒ JsString(v)
      case v: Boolean ⇒ JsBoolean(v)
      case v: Int ⇒ JsNumber(v)
      case v: Long ⇒ JsNumber(v)
      case v: Float ⇒ JsNumber(v.toDouble)
      case v: Double ⇒ JsNumber(v)
      case v: BigInt ⇒ JsNumber(BigDecimal(v))
      case v: BigDecimal ⇒ JsNumber(v)
      case v: JsValue ⇒ v
      case v ⇒ throw new IllegalArgumentException("Unsupported scalar value: " + v)
    }

    def enumNode(value: String, typeName: String) = JsString(value)

    def nullNode = JsNull

    def renderCompact(node: JsValue) = Json.stringify(node)

    def renderPretty(node: JsValue) = Json.prettyPrint(node)
  }

  implicit object SprayJsonInputUnmarshaller extends InputUnmarshaller[JsValue] {
    def getRootMapValue(node: JsValue, key: String) = node.asInstanceOf[JsObject].value.get(key)

    def isListNode(node: JsValue) = node.isInstanceOf[JsArray]

    def getListValue(node: JsValue) = node.asInstanceOf[JsArray].value

    def isMapNode(node: JsValue) = node.isInstanceOf[JsObject]

    def getMapValue(node: JsValue, key: String) = node.asInstanceOf[JsObject].value.get(key)

    def getMapKeys(node: JsValue) = node.asInstanceOf[JsObject].fieldSet.map(_._1)

    def isDefined(node: JsValue) = node != JsNull

    def getScalarValue(node: JsValue) = node match {
      case JsBoolean(b) ⇒ b
      case JsNumber(d) ⇒ d.toBigIntExact getOrElse d
      case JsString(s) ⇒ s
      case n ⇒ n
    }

    def getScalaScalarValue(node: JsValue) = getScalarValue(node)

    def isEnumNode(node: JsValue) = node.isInstanceOf[JsString]

    def isScalarNode(node: JsValue) = true

    def isVariableNode(node: JsValue) = false

    def getVariableName(node: JsValue) = throw new IllegalArgumentException("variables are not supported")

    def render(node: JsValue) = Json.stringify(node)
  }

  case object JsonCoercionViolation extends ValueCoercionViolation("Not valid JSON")

  implicit val JsonType = ScalarType[JsValue]("Json",
    description = Some("Raw JSON value"),
    coerceOutput = (value, _) ⇒ value,
    coerceUserInput = {
      case v: String => Right(JsString(v))
      case v: Boolean => Right(JsBoolean(v))
      case v: Int => Right(JsNumber(v))
      case v: Long => Right(JsNumber(v))
      case v: Float => Right(JsNumber(v.toDouble))
      case v: Double => Right(JsNumber(v))
      case v: BigInt => Right(JsNumber(BigDecimal(v)))
      case v: BigDecimal => Right(JsNumber(v))
      case v: JsValue => Right(v)
    },
    coerceInput = {
      case value: ast.StringValue => Right(Json.parse(value.value))
      case _ => Left(JsonCoercionViolation)
    })
}
