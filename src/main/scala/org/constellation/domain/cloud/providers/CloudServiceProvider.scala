package org.constellation.domain.cloud.providers

import better.files.File
import cats.data.EitherT
import org.constellation.schema.v2.GenesisObservation

trait CloudServiceProvider[F[_]] {
  val name: String

  def storeSnapshot(snapshot: File, height: Long, hash: String): EitherT[F, Throwable, Unit]

  def storeSnapshotInfo(snapshotInfo: File, height: Long, hash: String): EitherT[F, Throwable, Unit]

  def storeGenesis(genesisObservation: GenesisObservation): EitherT[F, Throwable, Unit]
}
