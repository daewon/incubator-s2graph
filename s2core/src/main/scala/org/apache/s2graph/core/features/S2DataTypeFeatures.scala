package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

case class S2DataTypeFeatures() extends Features.DataTypeFeatures {
  override def supportsStringValues(): Boolean = true

  override def supportsFloatValues(): Boolean = false

  override def supportsDoubleValues(): Boolean = false

  override def supportsIntegerValues(): Boolean = false

  override def supportsLongValues(): Boolean = false

  override def supportsBooleanValues(): Boolean = false

  override def supportsDoubleArrayValues(): Boolean = false

  override def supportsStringArrayValues(): Boolean = false

  override def supportsIntegerArrayValues(): Boolean = false

  override def supportsByteValues(): Boolean = false

  override def supportsUniformListValues(): Boolean = false

  override def supportsMapValues(): Boolean = false

  override def supportsBooleanArrayValues(): Boolean = false

  override def supportsSerializableValues(): Boolean = false

  override def supportsLongArrayValues(): Boolean = false

  override def supportsMixedListValues(): Boolean = false

  override def supportsFloatArrayValues(): Boolean = false

  override def supportsByteArrayValues(): Boolean = false

}
