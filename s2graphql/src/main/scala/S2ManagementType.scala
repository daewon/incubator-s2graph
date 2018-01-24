package org.apache.s2graph

import org.apache.s2graph.SchemaDef.MutationResponse
import org.apache.s2graph.core.Management.JsonModel.{Index, Prop}
import org.apache.s2graph.core.mysqls._
import sangria.marshalling.{CoercedScalaResultMarshaller, FromInput}
import sangria.schema._

import scala.util.{Failure, Success, Try}

object S2ManagementType {

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

  val ServiceType = deriveObjectType[GraphRepository, Service](
    ObjectTypeName("Service"),
    ObjectTypeDescription("Service"),
    RenameField("serviceName", "name")
  )

  val LabelMetaType = deriveObjectType[GraphRepository, LabelMeta](
    ObjectTypeName("LabelMeta"),
    ExcludeFields("seq", "labelId")
  )

  val S2EnumDataType = EnumType(
    "S2DataType",
    values = List(
      EnumValue("string", value = "string"),
      EnumValue("int", value = "int"),
      EnumValue("long", value = "long"),
      EnumValue("float", value = "float")
    )
  )

  val InputIndexType = InputObjectType[Index](
    "Index",
    description = "description here",
    fields = List(
      InputField("name", StringType),
      InputField("propNames", ListInputType(StringType))
    )
  )

  val InputPropType = InputObjectType[Prop](
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

  val LabelIndexType = deriveObjectType[GraphRepository, LabelIndex](
    ObjectTypeName("LabelIndex"),
    ExcludeFields("seq", "metaSeqs", "formulars", "labelId")
  )


  lazy val LabelType = deriveObjectType[GraphRepository, Label](
    ObjectTypeName("Label"),
    ObjectTypeDescription("Label"),
    AddFields(
      Field("Indexes", ListType(LabelIndexType), resolve = c => Nil),
      Field("Props", ListType(LabelMetaType), resolve = c => Nil)
    ),
    RenameField("label", "name")
  )

  val NameArg = Argument("name", StringType)
  val PropArg = Argument("props", OptionInputType(ListInputType(InputPropType)))
  val IndicesArg = Argument("indices", OptionInputType(ListInputType(InputIndexType)))

  val serviceArgOpts = List(
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
        Field(name,
          OptionType(tpe),
          resolve = _.value.result.toOption
        )
      )
    )
  }

  lazy val ServiceMutationResponseType = createMutationResponseType[Service](
    "CreatedService",
    "desc here",
    ServiceType
  )

  lazy val LabelMutationResponseType = createMutationResponseType[Label](
    "CreatedLabel",
    "desc here",
    LabelType
  )
}
