package org.constellation.rollback

import cats.data.EitherT
import cats.effect.{Concurrent, ContextShift, Sync}
import cats.implicits._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.DAO
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.domain.snapshot.{SnapshotInfo, SnapshotStorage}
import org.constellation.primitives.Genesis
import org.constellation.primitives.Schema.GenesisObservation
import org.constellation.rewards.{RewardSnapshot, RewardsManager}
import org.constellation.storage.SnapshotService
import AccountBalances.AccountBalances
import higherkindness.droste.{AlgebraM, CoalgebraM, scheme}
import org.constellation.checkpoint.CheckpointBlockValidator.AddressBalance
import org.constellation.domain.cloud.CloudStorage
import org.constellation.domain.cloud.CloudStorage.StorageName
import org.constellation.rollback.RecursiveStructure.{Done, More, SnapshotOutput, Stack}
import org.constellation.serializer.KryoSerializer

class RollbackService[F[_]](
  snapshotStorage: SnapshotStorage[F],
  cloudStorage: CloudStorage[F]
)(implicit F: Concurrent[F], C: ContextShift[F]) {

  val logger = Slf4jLogger.getLogger[F]

  // -------------

  val loadSnapshotsCoalgebra: CoalgebraM[F, Stack, StoredSnapshot] =
    CoalgebraM(storedSnapshot => {
      val prevHash = storedSnapshot.snapshot.lastSnapshot
      val so = SnapshotOutput(storedSnapshot)

      if (prevHash == Snapshot.snapshotZeroHash)
        F.pure(Done(so))
      else
        loadSnapshot(prevHash).map(More(_, so))
    })

  val balancesAlgebra: AlgebraM[F, Stack, AddressBalance] =
    AlgebraM {
      case Done(output)           => output.balances.pure[F]
      case More(balances, output) => F.pure { balances |+| output.balances }
    }

  val rollbackHylo = scheme.hyloM(balancesAlgebra, loadSnapshotsCoalgebra)

  def loadSnapshot(hash: String): F[StoredSnapshot] =
    if (hash == Snapshot.snapshotZeroHash) {
      StoredSnapshot(Snapshot.snapshotZero, Seq.empty).pure[F]
    } else
      for {
        _ <- logger.info(s"Loading snapshot: $hash")
        exists <- snapshotStorage.exists(hash)
        snapshot <- if (exists) {
          snapshotStorage.readSnapshot(hash).value.flatMap(F.fromEither)
        } else {
          cloudStorage.get(hash, StorageName.Snapshot).flatMap { bytes =>
            F.delay { KryoSerializer.deserializeCast[StoredSnapshot](bytes) }
          }
        }
      } yield snapshot

  def validateAndRestore(): EitherT[F, Throwable, Unit] = ???
}
