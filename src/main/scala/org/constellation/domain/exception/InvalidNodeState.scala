package org.constellation.domain.exception

import org.constellation.schema.NodeState

case class InvalidNodeState(expected: Set[NodeState], actual: NodeState)
    extends Exception(s"Node in invalid state actual: ${actual.toString} expected: ${expected.map(_.toString)}")
