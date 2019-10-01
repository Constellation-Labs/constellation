package org.constellation.rollback

import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import org.constellation.DAO
import org.constellation.consensus.{SnapshotInfo, StoredSnapshot}
import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.storage.SnapshotService
import org.constellation.util.AccountBalances.AccountBalances

class RollbackService[F[_]: Concurrent: Logger](
  dao: DAO,
  rollbackBalances: RollbackAccountBalances,
  snapshotService: SnapshotService[F]
)(implicit C: ContextShift[F]) {

  type RollbackData = (Seq[StoredSnapshot], SnapshotInfo, GenesisObservation)

  def validateAndRestore(): EitherT[F, RollbackException, Unit] =
    for {
      rollbackData <- validate()
      _ <- restore(rollbackData)
    } yield ()

  def validate(rollbackLoader: RollbackLoader = new RollbackLoader()): EitherT[F, RollbackException, RollbackData] =
    for {
      snapshots <- EitherT.fromEither[F](rollbackLoader.loadSnapshotsFromFile())
      _ <- EitherT.liftF(Logger[F].info("Snapshots files loaded"))

      snapshotInfo <- EitherT.fromEither[F](rollbackLoader.loadSnapshotInfoFromFile())
      _ <- EitherT.liftF(Logger[F].info("SnapshotInfo file loaded"))

      genesisObservation <- EitherT.fromEither[F](rollbackLoader.loadGenesisObservation())
      _ <- EitherT.liftF(Logger[F].info("GenesisObservation file loaded"))

      balances <- EitherT.fromEither[F](rollbackBalances.calculate(snapshotInfo.snapshot.lastSnapshot, snapshots))
      genesisBalances <- EitherT.fromEither[F](rollbackBalances.calculate(genesisObservation))
      _ <- EitherT.fromEither[F](validateAccountBalance(balances |+| genesisBalances))
      _ <- EitherT.liftF(Logger[F].info("Account balances validated"))
    } yield (snapshots, snapshotInfo, genesisObservation)

  def restore(rollbackData: RollbackData): EitherT[F, RollbackException, Unit] =
    for {
      _ <- acceptSnapshots(rollbackData._1)
      _ <- EitherT.liftF(Logger[F].info("Snapshots restored on disk"))

      _ <- EitherT.liftF(acceptGenesis(rollbackData._3))
      _ <- EitherT.liftF(Logger[F].info("GenesisObservation restored"))

      _ <- EitherT.liftF(acceptSnapshotInfo(rollbackData._2))
      _ <- EitherT.liftF(Logger[F].info("SnapshotInfo restored"))
    } yield ()

  private def acceptGenesis(genesisObservation: GenesisObservation): F[Unit] = Sync[F].delay {
    Genesis.acceptGenesis(genesisObservation)(dao)
  }

  private def acceptSnapshotInfo(snapshotInfo: SnapshotInfo): F[Unit] =
    snapshotService.setSnapshot(snapshotInfo)

  private def acceptSnapshots(
    snapshots: Seq[StoredSnapshot]
  )(implicit C: ContextShift[F]): EitherT[F, RollbackException, Unit] = EitherT {
    snapshots.toList
      .traverse(snapshotService.addSnapshotToDisk)
      .map(_ => ().asRight[RollbackException])
      .handleErrorWith(_ => CannotWriteToDisk.asInstanceOf[RollbackException].asLeft[Unit].pure[F])
  }

  private def validateAccountBalance(accountBalances: AccountBalances): Either[RollbackException, Unit] =
    accountBalances.count(_._2 < 0) match {
      case 0 => Right(())
      case _ => Left(InvalidBalances)
    }
}
