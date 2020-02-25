package org.constellation.storage

import better.files.File
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import org.constellation._
import org.constellation.checkpoint.CheckpointService
import org.constellation.consensus.{ConsensusManager, RandomData, Snapshot, StoredSnapshot}
import org.constellation.domain.observation.ObservationService
import org.constellation.domain.snapshot.{SnapshotInfo, SnapshotInfoStorage, SnapshotStorage}
import org.constellation.domain.transaction.TransactionService
import org.constellation.primitives.ConcurrentTipService
import org.constellation.primitives.Schema.CheckpointCache
import org.constellation.rewards.{EigenTrust, RewardsManager}
import org.constellation.domain.cloud.CloudStorage
import org.constellation.trust.TrustManager
import org.constellation.util.Metrics
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
  var snapshotStorage: SnapshotStorage[IO] = _
  var snapshotInfoStorage: SnapshotInfoStorage[IO] = _

  before {
    dao = mockDAO

    val cts = mock[ConcurrentTipService[IO]]
    val addressService = mock[AddressService[IO]]
    val cloudStorage = mock[CloudStorage[IO]]
    val checkpointService = mock[CheckpointService[IO]]
    val messageService = mock[MessageService[IO]]
    val transactionService = mock[TransactionService[IO]]
    val observationService = mock[ObservationService[IO]]
    val rateLimiting = mock[RateLimiting[IO]]
    val consensusManager = mock[ConsensusManager[IO]]
    val trustManager = mock[TrustManager[IO]]
    val soeService = mock[SOEService[IO]]
    val rewardsManager = mock[RewardsManager[IO]]
    snapshotStorage = mock[SnapshotStorage[IO]]
    snapshotInfoStorage = mock[SnapshotInfoStorage[IO]]

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
      rewardsManager,
      snapshotStorage,
      snapshotInfoStorage,
      dao
    )
  }

  "exists" - {
    "should return true if snapshot hash is the latest snapshot" in {
      val lastSnapshot: Snapshot = snapshotService.storedSnapshot.get.map(_.snapshot).unsafeRunSync

      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { _ =>
          snapshotStorage.getSnapshotHashes shouldReturnF List.empty

          snapshotService.exists(lastSnapshot.hash).unsafeRunSync shouldBe true
        }
      }
    }
  }

  "should return true if snapshot hash exists" in {
    File.usingTemporaryDirectory() { dir =>
      File.usingTemporaryFile("", "", Some(dir)) { file =>
        snapshotStorage.getSnapshotHashes shouldReturnF List(file.name)

        snapshotService.exists(file.name).unsafeRunSync shouldBe true
      }
    }
  }

  "should return false if snapshot hash does not exist" in {
    File.usingTemporaryDirectory() { dir =>
      File.usingTemporaryFile("", "", Some(dir)) { _ =>
        snapshotStorage.getSnapshotHashes shouldReturnF List("unknown")

        snapshotService.exists("dontexist").unsafeRunSync shouldBe false
      }
    }
  }

  private def mockDAO: DAO = mock[DAO]
}
