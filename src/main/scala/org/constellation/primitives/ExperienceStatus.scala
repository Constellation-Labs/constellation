package org.constellation.primitives

object ExperienceStatus extends Enumeration {
  type ExperienceStatus = Value
  val Pending, InConsensus, Accepted, Unknown = Value
}
