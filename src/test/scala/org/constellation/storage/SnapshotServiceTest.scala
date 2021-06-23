//package org.constellation.storage
//
//import better.files.File
//import cats.data.EitherT
//import cats.implicits._
//import cats.effect.{ContextShift, IO, Timer}
//import org.constellation._
//import org.constellation.checkpoint.CheckpointService
//import org.constellation.consensus.ConsensusManager
//import org.constellation.domain.cloud.CloudStorageOld
//import org.constellation.domain.observation.ObservationService
//import org.constellation.domain.rewards.StoredRewards
//import org.constellation.domain.storage.LocalFileStorage
//import org.constellation.domain.transaction.TransactionService
//import org.constellation.rewards.EigenTrust
//import org.constellation.schema.snapshot.{Snapshot, SnapshotInfo, StoredSnapshot}
//import org.constellation.serialization.KryoSerializer
//import org.constellation.trust.TrustManager
//import org.mockito.cats.IdiomaticMockitoCats
//import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
//import org.scalatest.BeforeAndAfter
//import org.scalatest.freespec.AnyFreeSpec
//import org.scalatest.matchers.should.Matchers
//
//import scala.concurrent.ExecutionContext
//
//class SnapshotServiceTest
//    extends AnyFreeSpec
//    with IdiomaticMockito
//    with IdiomaticMockitoCats
//    with Matchers
//    with ArgumentMatchersSugar
//    with BeforeAndAfter {
//
//  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
//  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)
//
//  var dao: DAO = _
//  var snapshotService: SnapshotService[IO] = _
//  var snapshotStorage: LocalFileStorage[IO, StoredSnapshot] = _
//  var snapshotInfoStorage: LocalFileStorage[IO, SnapshotInfo] = _
//  KryoSerializer.init[IO].handleError(_ => Unit).unsafeRunSync()
//
//  before {
//    dao = mockDAO
//
//    val cts = mock[ConcurrentTipService[IO]]
//    val addressService = mock[AddressService[IO]]
//    val cloudStorage = mock[CloudStorageOld[IO]]
//    val checkpointService = mock[CheckpointService[IO]]
//    val messageService = mock[MessageService[IO]]
//    val transactionService = mock[TransactionService[IO]]
//    val observationService = mock[ObservationService[IO]]
//    val rateLimiting = mock[RateLimiting[IO]]
//    val consensusManager = mock[ConsensusManager[IO]]
//    val trustManager = mock[TrustManager[IO]]
//    val soeService = mock[SOEService[IO]]
//    val eigenTrustStorage = mock[LocalFileStorage[IO, StoredRewards]]
//    val eigenTrust = mock[EigenTrust[IO]]
//    snapshotStorage = mock[LocalFileStorage[IO, StoredSnapshot]]
//    snapshotInfoStorage = mock[LocalFileStorage[IO, SnapshotInfo]]
//
//    snapshotService = new SnapshotService[IO](
//      cts,
//      cloudStorage,
//      addressService,
//      checkpointService,
//      messageService,
//      transactionService,
//      observationService,
//      rateLimiting,
//      consensusManager,
//      trustManager,
//      soeService,
//      snapshotStorage,
//      snapshotInfoStorage,
//      eigenTrustStorage,
//      eigenTrust,
//      dao,
//      ExecutionContext.global,
//      ExecutionContext.global
//    )
//  }
//
//  "exists" - {
//    "should return true if snapshot hash is the latest snapshot" in {
//      val lastSnapshot: Snapshot = snapshotService.storedSnapshot.get.map(_.snapshot).unsafeRunSync
//
//      File.usingTemporaryDirectory() { dir =>
//        File.usingTemporaryFile("", "", Some(dir)) { _ =>
//          snapshotStorage.list() shouldReturn EitherT.pure(List.empty)
//
//          snapshotService.exists(lastSnapshot.hash).unsafeRunSync shouldBe true
//        }
//      }
//    }
//  }
//
//  "should return true if snapshot hash exists" in {
//    File.usingTemporaryDirectory() { dir =>
//      File.usingTemporaryFile("", "", Some(dir)) { file =>
//        snapshotStorage.list() shouldReturn EitherT.pure(List(file.name))
//
//        snapshotService.exists(file.name).unsafeRunSync shouldBe true
//      }
//    }
//  }
//
//  "should return false if snapshot hash does not exist" in {
//    File.usingTemporaryDirectory() { dir =>
//      File.usingTemporaryFile("", "", Some(dir)) { _ =>
//        snapshotStorage.list() shouldReturn EitherT.pure(List("unknown"))
//
//        snapshotService.exists("dontexist").unsafeRunSync shouldBe false
//      }
//    }
//  }
//
//  private def mockDAO: DAO = mock[DAO]
//}
