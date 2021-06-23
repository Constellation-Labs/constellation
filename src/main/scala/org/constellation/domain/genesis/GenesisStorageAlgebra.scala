package org.constellation.domain.genesis

import org.constellation.schema.GenesisObservation
import org.constellation.schema.checkpoint.CheckpointBlock

trait GenesisStorageAlgebra[F[_]] {
  def getGenesisObservation: F[Option[GenesisObservation]]
  def setGenesisObservation(go: GenesisObservation): F[Unit]

  def getGenesisBlock: F[Option[CheckpointBlock]]
}
