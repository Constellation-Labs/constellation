package org.constellation.schema

case class CommonMetadata(
  valid: Boolean = true,
  inDAG: Boolean = false,
  resolved: Boolean = true,
  resolutionInProgress: Boolean = false,
  inMemPool: Boolean = false,
  lastResolveAttempt: Option[Long] = None,
  rxTime: Long = System.currentTimeMillis() // TODO: Unify common metadata like this
)
