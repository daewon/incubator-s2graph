package com.daumkakao.s2graph.core.models

import HBaseModel._
import play.api.libs.json.Json

/**
 * Created by shon on 5/15/15.
 */

object ServiceColumn {
  def findById(id: Int, useCache: Boolean = true): ServiceColumn = {
    HBaseModel.find[ServiceColumn](useCache)(Seq(("id" -> id))).get
  }
  def find(serviceId: Int, columnName: String, useCache: Boolean = true): Option[ServiceColumn] = {
    HBaseModel.find[ServiceColumn](useCache)(Seq("serviceId" -> serviceId, "columnName" -> columnName))
  }
  def findsByServiceId(serviceId: Int, useCache: Boolean = true): List[ServiceColumn] = {
    HBaseModel.findsMatch[ServiceColumn](useCache)(Seq("serviceId" -> serviceId))
  }
  def findOrInsert(serviceId: Int, columnName: String, columnType: Option[String]): ServiceColumn = {
    find(serviceId, columnName, useCache = false) match {
      case Some(s) => s
      case None =>
        val id = HBaseModel.getAndIncrSeq[ServiceColumn]
        val model = new ServiceColumn(Map("id" -> id, "serviceId" -> serviceId, "columnName" -> columnName,
          "columnType" -> columnType.getOrElse("string")))
        model.create
        model
    }
  }
}
case class ServiceColumn(kvsParam: Map[KEY, VAL]) extends HBaseModel[ServiceColumn]("HServiceColumn", kvsParam) {
  override val columns = Seq("id", "serviceId", "columnName", "columnType")
  val pk = Seq(("id", kvs("id")))
  val idxServiceIdColumnName = Seq(("serviceId", kvs("serviceId")), ("columnName", kvs("columnName")))
  override val idxs = List(pk, idxServiceIdColumnName)
  override def foreignKeys() = {
    List(
      HBaseModel.findsMatch[ColumnMeta](useCache = false)(Seq("columnId" -> kvs("id")))
    )
  }
  validate(columns)

  val id = Some(kvs("id").toString.toInt)
  val serviceId = kvs("serviceId").toString.toInt
  val columnName = kvs("columnName").toString
  val columnType = kvs("columnType").toString


  lazy val service = Service.findById(serviceId)
  lazy val metas = ColumnMeta.findAllByColumn(id.get)
  lazy val metasInvMap = metas.map { meta => meta.name -> meta} toMap
  lazy val metaNamesMap = (ColumnMeta.lastModifiedAtColumn :: metas).map(x => (x.seq, x.name)) toMap
  lazy val toJson = Json.obj("serviceName" -> service.serviceName, "columnName" -> columnName, "columnType" -> columnType)

  lazy val version = "v1"
}
