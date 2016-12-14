package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features

class S2VertexPropertyFeatures extends S2PropertyFeatures with Features.VertexPropertyFeatures {
  override def supportsStringIds(): Boolean = true

  override def supportsCustomIds(): Boolean = false

  override def supportsUuidIds(): Boolean = false

  override def supportsAddProperty(): Boolean = true

  override def supportsUserSuppliedIds(): Boolean = true

  override def supportsRemoveProperty(): Boolean = false

  override def supportsAnyIds(): Boolean = false

  override def supportsNumericIds(): Boolean = true

  override def willAllowId(id: scala.Any): Boolean = true
}
