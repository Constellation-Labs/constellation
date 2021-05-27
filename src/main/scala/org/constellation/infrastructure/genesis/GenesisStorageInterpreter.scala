package org.constellation.infrastructure.genesis

import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.all._
import org.constellation.domain.genesis.GenesisStorageAlgebra
import org.constellation.schema.GenesisObservation
import org.constellation.schema.checkpoint.CheckpointBlock

class GenesisStorageInterpreter[F[_]]()(implicit F: Sync[F]) extends GenesisStorageAlgebra[F] {
  val genesisObservation: Ref[F, Option[GenesisObservation]] = Ref.unsafe(none)

  def getGenesisObservation: F[Option[GenesisObservation]] = genesisObservation.get
  def setGenesisObservation(go: GenesisObservation): F[Unit] = genesisObservation.set(go.some)

  def getGenesisBlock: F[Option[CheckpointBlock]] =
    getGenesisObservation.nested.map(_.genesis).value
}
