package org.constellation.storage

import better.files.File
import cats.effect.{ContextShift, IO, Timer}
import org.constellation.DAO
import org.constellation.consensus.Snapshot
import org.constellation.primitives.ConcurrentTipService
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{BeforeAndAfter, FreeSpec, Matchers}

import scala.concurrent.ExecutionContext

class SnapshotServiceTest
    extends FreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(ExecutionContext.global)

  var dao: DAO = _
  var snapshotService: SnapshotService[IO] = _

  before {
    dao = mockDAO

    val cts = mock[ConcurrentTipService]
    val addressService = mock[AddressService[IO]]
    val checkpointService = mock[CheckpointService[IO]]
    val messageService = mock[MessageService[IO]]
    val transactionService = mock[TransactionService[IO]]

    snapshotService = new SnapshotService[IO](
      cts,
      addressService,
      checkpointService,
      messageService,
      transactionService,
      dao
    )
  }

  "exists" - {
    "should return true if snapshot hash is the latest snapshot" in {
      val lastSnapshot: Snapshot = snapshotService.snapshot.get.unsafeRunSync

      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { _ =>
          dao.snapshotPath shouldReturn dir

          snapshotService.exists(lastSnapshot.hash).unsafeRunSync shouldBe true
        }
      }
    }

    "should return true if snapshot hash exists" in {
      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { file =>
          dao.snapshotPath shouldReturn dir

          snapshotService.exists(file.name).unsafeRunSync shouldBe true
        }
      }
    }

    "should return false if snapshot hash does not exist" in {
      File.usingTemporaryDirectory() { dir =>
        File.usingTemporaryFile("", "", Some(dir)) { _ =>
          dao.snapshotPath shouldReturn dir

          snapshotService.exists("dontexist").unsafeRunSync shouldBe false
        }
      }
    }
  }

  private def mockDAO: DAO = mock[DAO]
}
