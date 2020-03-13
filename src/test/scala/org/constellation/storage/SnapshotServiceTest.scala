package org.constellation.storage

import better.files.File
import cats.data.EitherT
import cats.effect.{ContextShift, IO, Timer}
import org.constellation._
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus.{ConsensusManager, Snapshot, StoredSnapshot}
import org.constellation.domain.cloud.CloudStorageOld
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.rewards.StoredEigenTrust
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.domain.transaction.TransactionService
import org.constellation.primitives.ConcurrentTipService
import org.constellation.rewards.EigenTrust
import org.constellation.trust.TrustManager
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

class SnapshotServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val timer: Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)

  var dao: DAO = _
  var snapshotService: SnapshotService[IO] = _
  var snapshotStorage: LocalFileStorage[IO, StoredSnapshot] = _
  var snapshotInfoStorage: LocalFileStorage[IO, SnapshotInfo] = _

  before {
    dao = mockDAO

    val cts = mock[ConcurrentTipService[IO]]
    val addressService = mock[AddressService[IO]]
    val cloudStorage = mock[CloudStorageOld[IO]]
    val checkpointService = mock[CheckpointService[IO]]
    val messageService = mock[MessageService[IO]]
    val transactionService = mock[TransactionService[IO]]
    val observationService = mock[ObservationService[IO]]
    val rateLimiting = mock[RateLimiting[IO]]
    val consensusManager = mock[ConsensusManager[IO]]
    val trustManager = mock[TrustManager[IO]]
    val soeService = mock[SOEService[IO]]
    val eigenTrustStorage = mock[LocalFileStorage[IO, StoredEigenTrust]]
    val eigenTrust = mock[EigenTrust[IO]]
    snapshotStorage = mock[LocalFileStorage[IO, StoredSnapshot]]
    snapshotInfoStorage = mock[LocalFileStorage[IO, SnapshotInfo]]

    snapshotService = new SnapshotService[IO](
      cts,
      cloudStorage,
      addressService,
      checkpointService,
      messageService,
      transactionService,
      observationService,
      rateLimiting,
      consensusManager,
      trustManager,
      soeService,
      snapshotStorage,
      snapshotInfoStorage,
      eigenTrustStorage,
      eigenTrust,
      dao
    )
  }

  "exists" - {
    "should return true if snapshot hash is the latest snapshot" in {
      val lastSnapshot: Snapshot = snapshotService.storedSnapshot.get.map(_.snapshot).unsafeRunSync

      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { _ =>
          snapshotStorage.list() shouldReturn EitherT.pure(List.empty)

          snapshotService.exists(lastSnapshot.hash).unsafeRunSync shouldBe true
        }
      }
    }
  }

  "should return true if snapshot hash exists" in {
    File.usingTemporaryDirectory() { dir =>
      File.usingTemporaryFile("", "", Some(dir)) { file =>
        snapshotStorage.list() shouldReturn EitherT.pure(List(file.name))

        snapshotService.exists(file.name).unsafeRunSync shouldBe true
      }
    }
  }

  "should return false if snapshot hash does not exist" in {
    File.usingTemporaryDirectory() { dir =>
      File.usingTemporaryFile("", "", Some(dir)) { _ =>
        snapshotStorage.list() shouldReturn EitherT.pure(List("unknown"))

        snapshotService.exists("dontexist").unsafeRunSync shouldBe false
      }
    }
  }

  private def mockDAO: DAO = mock[DAO]
}
