package com.kakao.s2graph.core.storage.hbase

import com.kakao.s2graph.core.SnapshotEdge
import com.kakao.s2graph.core.mysqls.LabelIndex
import com.kakao.s2graph.core.storage.{StorageSerializable, SKeyValue}
import com.kakao.s2graph.core.types.VertexId
import org.apache.hadoop.hbase.util.Bytes

class SnapshotEdgeHGStorageSerializable(snapshotEdge: SnapshotEdge) extends HStorageSerializable {
  import StorageSerializable._

  val label = snapshotEdge.label
  val table = label.hbaseTableName.getBytes()
  val cf = HStorageSerializable.edgeCf

  def valueBytes() = Bytes.add(Array.fill(1)(snapshotEdge.op), propsToKeyValuesWithTs(snapshotEdge.props.toList))

  override def toKeyValues: Seq[SKeyValue] = {
    val srcIdBytes = VertexId.toSourceVertexId(snapshotEdge.srcVertex.id).bytes
    val labelWithDirBytes = snapshotEdge.labelWithDir.bytes
    val labelIndexSeqWithIsInvertedBytes = labelOrderSeqWithIsInverted(LabelIndex.DefaultSeq, isInverted = true)

    val row = Bytes.add(srcIdBytes, labelWithDirBytes, labelIndexSeqWithIsInvertedBytes)
    val tgtIdBytes = VertexId.toTargetVertexId(snapshotEdge.tgtVertex.id).bytes

    val qualifier = tgtIdBytes

    val value = snapshotEdge.pendingEdgeOpt match {
      case None => valueBytes()
      case Some(pendingEdge) =>
        val opBytes = Array.fill(1)(snapshotEdge.op)
        val versionBytes = Bytes.toBytes(snapshotEdge.version)
        val propsBytes = propsToKeyValuesWithTs(pendingEdge.propsWithTs.toSeq)
        val dummyBytes = Bytes.toBytes(System.nanoTime())
        val pendingEdgeValueBytes = valueBytes()

        Bytes.add(Bytes.add(pendingEdgeValueBytes, opBytes, versionBytes),
          Bytes.add(propsBytes, dummyBytes))
    }

    val kv = SKeyValue(table, row, cf, qualifier, value, snapshotEdge.version)
    Seq(kv)
  }
}
