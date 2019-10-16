package org.constellation.domain.consensus

object ConsensusStatus extends Enumeration {
  type ConsensusStatus = Value
  val Pending, Arbitrary, InConsensus, Accepted, Unknown = Value
}
