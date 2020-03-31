package org.constellation.util

import org.constellation.primitives.Schema.NodeState
import org.constellation.primitives.Schema.NodeType

case class NodeStateInfo(
  nodeState: NodeState,
  addresses: Seq[String] = Seq(),
  nodeType: NodeType = NodeType.Full
) // TODO: Refactor, addresses temp for testing
