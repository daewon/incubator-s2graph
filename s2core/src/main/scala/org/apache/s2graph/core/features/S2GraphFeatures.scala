package org.apache.s2graph.core.features

import org.apache.tinkerpop.gremlin.structure.Graph.Features
import org.apache.tinkerpop.gremlin.structure.Graph.Features.GraphFeatures


class S2GraphFeatures extends GraphFeatures {
  override def supportsComputer(): Boolean = false

  override def supportsThreadedTransactions(): Boolean = true

  override def supportsTransactions(): Boolean = true

  override def supportsPersistence(): Boolean = false

  override def variables(): Features.VariableFeatures = super.variables()

  override def supportsConcurrentAccess(): Boolean = false
}
