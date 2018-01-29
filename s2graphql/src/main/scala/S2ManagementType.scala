package org.apache.s2graph

import org.apache.s2graph.SchemaDef.MutationResponse
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.S2EdgeLike
import org.apache.s2graph.core.mysqls._
import org.apache.s2graph.core.storage.MutateResponse
import play.api.libs.json.JsValue
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput}
import sangria.schema._

import scala.util.{Failure, Success, Try}

object S2ManagementType {

  import PlayJsonPolyType._

  case class PartialVertexParam(service: Service, vid: JsValue)

  case class PartialEdgeParam(ts: Long,
                              from: Any,
                              to: Any,
                              direction: String,
                              props: Map[String, Any])

  implicit object PartialEdgeFromInput extends FromInput[PartialEdgeParam] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val inputMap = node.asInstanceOf[Map[String, Any]]
      println(inputMap)

      val from = inputMap("from")
      val to = inputMap("to")

      val ts = inputMap.get("timestamp") match {
        case Some(Some(v)) => v.asInstanceOf[Long]
        case _ => System.currentTimeMillis()
      }

      val dir = inputMap.get("direction") match {
        case Some(Some(v)) => v.asInstanceOf[String]
        case None => "out"
      }

      val props = inputMap.get("props") match {
        case Some(Some(v)) => v.asInstanceOf[Map[String, Option[Any]]].filter(_._2.isDefined).mapValues(_.get)
        case _ => Map.empty[String, Any]
      }

      PartialEdgeParam(ts, from, to, dir, props)
    }
  }

  implicit object IndexFromInput extends FromInput[Index] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, Any]]
      Index(input("name").asInstanceOf[String], input("propNames").asInstanceOf[Seq[String]])
    }
  }

  implicit object PropFromInput extends FromInput[Prop] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, String]]
      Prop(input("name"), input("defaultValue"), input("dataType"))
    }
  }

  implicit object LabelServiceFromInput extends FromInput[LabelServiceProp] {
    val marshaller = CoercedScalaResultMarshaller.default

    def fromResult(node: marshaller.Node) = {
      val input = node.asInstanceOf[Map[String, String]]
      LabelServiceProp(input("name"), input("columnName"), input("dataType"))
    }
  }

  import sangria.macros.derive._
  import PlayJsonPolyType._

  val ServiceType = deriveObjectType[GraphRepository, Service](
    ObjectTypeName("Service"),
    ObjectTypeDescription("Service"),
    RenameField("serviceName", "name")
  )

  val LabelMetaType = deriveObjectType[GraphRepository, LabelMeta](
    ObjectTypeName("LabelMeta"),
    ExcludeFields("seq", "labelId")
  )

  def S2EnumDataType = EnumType(
    "S2DataType",
    values = List(
      EnumValue("string", value = "string"),
      EnumValue("int", value = "int"),
      EnumValue("long", value = "long"),
      EnumValue("float", value = "float")
    )
  )

  def S2EnumDirectionType = EnumType(
    "S2Direction",
    values = List(
      EnumValue("out", value = "out"),
      EnumValue("in", value = "in")
    )
  )

  def InputIndexType = InputObjectType[Index](
    "Index",
    description = "description here",
    fields = List(
      InputField("name", StringType),
      InputField("propNames", ListInputType(StringType))
    )
  )


  //  lazy val InputPropsListType = InputObjectType[Map[String, JsValue]](
  //    "InputPropType",
  //    "desc",
  //    () => LabelMeta.findAll().map(_.name).distinct.map { name =>
  //      InputField(name, PlayJsonType)
  //    }
  //  )
  //  lazy val InputEdgeType = InputObjectType[PartialEdge](
  //    "Edge",
  //    description = "edge",
  //    fields = List(
  //      InputField("timestamp", LongType),
  //      InputField("from", PlayJsonType),
  //      InputField("to", PlayJsonType),
  //      InputField("label", S2EnumLabelType),
  //      InputField("direction", OptionInputType(S2EnumDirectionType)),
  //      InputField("props", OptionInputType(InputPropsListType))
  //    )
  //  )

  def InputPropType = InputObjectType[Prop](
    "Prop",
    description = "A Property of Label",
    fields = List(
      InputField("name", StringType),
      InputField("dataType", S2EnumDataType),
      InputField("defaultValue", StringType)
    )
  )

  val dummyEnum = EnumValue("_", value = "_")

  case class LabelServiceProp(name: String, columnName: String, dataType: String)

  def S2EnumServiceType = EnumType(
    s"ServiceList",
    values =
      dummyEnum +: Service.findAll().map { service =>
        EnumValue(service.serviceName, value = service.serviceName)
      }
  )

  def S2EnumLabelType = EnumType(
    s"LabelList",
    values =
      dummyEnum +: Label.findAll().map { label =>
        EnumValue(label.label, value = label.label)
      }
  )

  def S2EnumCompressionAlgorithmType = EnumType(
    "compressionAlgorithm",
    values = List(
      EnumValue("gz", description = Option("gzip"), value = "gz"),
      EnumValue("lz4", description = Option("lz4"), value = "lz4")
    )
  )

  def S2EnumConsistencyLevelType = EnumType(
    "consistencyList",
    values = List(
      EnumValue("weak", description = Option("weak consistency"), value = "weak"),
      EnumValue("strong", description = Option("strong consistency"), value = "strong")
    )
  )

  def InputLabelServiceType = InputObjectType[LabelServiceProp](
    "LabelServiceProp",
    fields = List(
      InputField("name", S2EnumServiceType),
      InputField("columnName", StringType),
      InputField("dataType", S2EnumDataType)
    )
  )

  def LabelIndexType = deriveObjectType[GraphRepository, LabelIndex](
    ObjectTypeName("LabelIndex"),
    ExcludeFields("seq", "metaSeqs", "formulars", "labelId")
  )

  def LabelType = deriveObjectType[GraphRepository, Label](
    ObjectTypeName("Label"),
    ObjectTypeDescription("Label"),
    AddFields(
      Field("indexes", ListType(LabelIndexType), resolve = c => Nil),
      Field("props", ListType(LabelMetaType), resolve = c => Nil)
    ),
    RenameField("label", "name")
  )

  def s2TypeToScalarType(from: String): ScalarType[_] = {
    from match {
      case "string" => StringType
      case "int" => IntType
      case "integer" => IntType
      case "long" => LongType
      case "float" => FloatType
      case "boolean" => BooleanType
      case "bool" => BooleanType
    }
  }

  def NameArg = Argument("name", StringType, description = "desc here")

  def ServiceNameArg = Argument("name", OptionInputType(S2EnumServiceType), description = "desc here")

  def LabelNameArg = Argument("name", OptionInputType(S2EnumLabelType), description = "desc here")

  def PropArg = Argument("props", OptionInputType(ListInputType(InputPropType)), description = "desc here")

  def IndicesArg = Argument("indices", OptionInputType(ListInputType(InputIndexType)), description = "desc here")

  def makeInputPartialEdgeParamType(label: Label): InputObjectType[PartialEdgeParam] = {
    lazy val InputPropsType = InputObjectType[Map[String, ScalarType[_]]](
      s"${label.label}_props",
      description = "desc here",
      () => label.labelMetaSet.toList.map(lm =>
        InputField(lm.name, OptionInputType(s2TypeToScalarType(lm.dataType)))
      )
    )

    lazy val labelFields = List(
      InputField("timestamp", OptionInputType(LongType)),
      InputField("from", s2TypeToScalarType(label.srcColumnType)),
      InputField("to", s2TypeToScalarType(label.srcColumnType)),
      InputField("direction", OptionInputType(S2EnumDirectionType))
    )

    InputObjectType[PartialEdgeParam](
      s"${label.label}_mutate",
      description = "desc here",
      () => {
        if (label.labelMetaSet.isEmpty) {
          labelFields
        } else {
          labelFields ++ Seq(InputField("props", OptionInputType(InputPropsType)))
        }
      }
    )
  }

  def EdgeArg = Label.findAll().map { label =>
    val inputPartialEdgeParamType = makeInputPartialEdgeParamType(label)
    Argument(label.label, OptionInputType(inputPartialEdgeParamType))
  }

  def EdgesArg = Label.findAll().map { label =>
    val inputPartialEdgeParamType = makeInputPartialEdgeParamType(label)
    Argument(label.label, OptionInputType(ListInputType(inputPartialEdgeParamType)))
  }

  def serviceArgOpts = List(
    "compressionAlgorithm" -> S2EnumCompressionAlgorithmType,
    "cluster" -> StringType,
    "hTableName" -> StringType,
    "preSplitSize" -> IntType,
    "hTableTTL" -> IntType
  ).map { case (name, _type) => Argument(name, OptionInputType(_type)) }

  def labelArgRequired = List(
    "sourceService" -> InputLabelServiceType,
    "targetService" -> InputLabelServiceType
  ).map { case (name, _type) => Argument(name, _type) }

  def labelArgOpts = List(
    "serviceName" -> S2EnumServiceType,
    "consistencyLevel" -> S2EnumConsistencyLevelType,
    "isDirected" -> BooleanType,
    "isAsync" -> BooleanType,
    "schemaVersion" -> StringType
  ).map { case (name, _type) => Argument(name, OptionInputType(_type)) }

  def ServiceMutationResponseType = createMutationResponseType[Service](
    "CreateService",
    "desc here",
    ServiceType
  )

  def LabelMutationResponseType = createMutationResponseType[Label](
    "CreateLabel",
    "desc here",
    LabelType
  )

  // TODO: Change to EdgeType
  case class BooleanResponse(isSuccess: Boolean)

  def EdgeMutateResponseType = deriveObjectType[GraphRepository, BooleanResponse](
    ObjectTypeName("EdgeMutateResponse"),
    ObjectTypeDescription("desc here")
  )

  def createMutationResponseType[T](name: String, desc: String, tpe: ObjectType[_, T]) = {
    ObjectType(name, desc,
      () => fields[Unit, MutationResponse[T]](
        Field("isSuccess",
          BooleanType,
          resolve = _.value.result.isSuccess
        ),
        Field("message",
          OptionType(StringType),
          resolve = _.value.result match {
            case Success(_) => None
            case Failure(ex) => Option(ex.getMessage)
          }
        ),
        Field("created",
          OptionType(tpe),
          resolve = _.value.result.toOption
        )
      )
    )
  }


}
