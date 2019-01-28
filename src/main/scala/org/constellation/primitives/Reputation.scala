package org.constellation.primitives

import org.constellation.primitives.Schema.Id

/** Reputation trait. */
trait Reputation {

  @volatile var secretReputation: Map[Id, Double] = Map()
  @volatile var publicReputation: Map[Id, Double] = Map()
  @volatile var normalizedDeterministicReputation: Map[Id, Double] = Map()
  @volatile var deterministicReputation: Map[Id, Int] = Map()

  /** Normalizes reputations to unity.
    *
    * @param reps ... Map of reputations.
    * @return Normalized map of reputations given as input.
    */
  def normalizeReputations(reps: Map[String, Long]): Map[String, Double] = {
    val total = reps.values.sum
    reps.map {
      case (id, r) =>
        id -> r.toDouble / total.toDouble
    }
  }

}
