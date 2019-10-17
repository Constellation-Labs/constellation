package org.constellation.domain.consensus

object ConsensusStatus extends Enumeration {
  type ConsensusStatus = Value
  val Pending, InConsensus, Accepted, Unknown = Value
}
