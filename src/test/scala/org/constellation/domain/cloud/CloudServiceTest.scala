package org.constellation.domain.cloud

import better.files.File
import cats.effect.IO
import cats.syntax.all._
import fs2.concurrent.Queue
import org.constellation.domain.cloud.CloudService.{CloudServiceEnqueue, DataToSend, PrecedingSnapshotIsNotThePreviousOne, SnapshotSent}
import org.constellation.domain.cloud.config.CloudConfig
import org.constellation.util.Metrics
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.BeforeAndAfter
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class CloudServiceTest extends AnyFreeSpec with Matchers with BeforeAndAfter with IdiomaticMockito with ArgumentMatchersSugar with IdiomaticMockitoCats {

  implicit val ce = IO.ioConcurrentEffect(IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global))

  var metrics: Metrics = _
  var cloudService: CloudService[IO] = _
  var cloudServiceEnqueue: CloudServiceEnqueue[IO] = _

  before {
    metrics = mock[Metrics]
    metrics.updateMetricAsync(*, any[Long]) shouldReturnF (())
    cloudService = CloudService(CloudConfig.empty, metrics)
    cloudServiceEnqueue = cloudService.cloudSendingQueue(Queue.unbounded[IO, DataToSend].unsafeRunSync()).unsafeRunSync()
  }

  "verifyAndSend" - {
    "verify the previous sent snapshot correctly when there is no snapshot" in {
      val result = cloudService.verifyAndSend(2L, "somehash", IO.delay(List(().asRight[Throwable]))).unsafeRunSync()

      result shouldBe List(Right(()))
    }

    "verify the previous sent snapshot correctly when the chain is preserved" in {
      val previousSnapshot = SnapshotSent(10L, "somehash", true, true).some
      cloudService.lastSentSnapshot.set(previousSnapshot).unsafeRunSync()

      val result = cloudService.verifyAndSend(12L, "otherHash", IO.delay(List(().asRight[Throwable]))).unsafeRunSync()
      val expected = List(Right(()))

      result shouldBe expected
    }

    "verify the previous sent snapshot correctly when we are sending data for the same snapshot" in {
      val previousSnapshot = SnapshotSent(10L, "somehash", true, true).some
      cloudService.lastSentSnapshot.set(previousSnapshot).unsafeRunSync()

      val result = cloudService.verifyAndSend(10L, "somehash", IO.delay(List(().asRight[Throwable]))).unsafeRunSync()
      val expected = List(Right(()))

      result shouldBe expected
    }

    "fail verification if previously sent snapshot is not directly preceding the one we are sending" in {
      val previousSnapshot = SnapshotSent(10L, "somehash", true, true).some
      cloudService.lastSentSnapshot.set(previousSnapshot).unsafeRunSync()

      val result = cloudService.verifyAndSend(14L, "otherHash", IO.delay(List(().asRight[Throwable]))).unsafeRunSync()
      val expected = List(Left(PrecedingSnapshotIsNotThePreviousOne(previousSnapshot, 14L, "otherHash")))

      result shouldBe expected
    }

    "fail verification if previously sent snapshot is on the same height but the hash is different" in {
      val previousSnapshot = SnapshotSent(10L, "somehash", true, true).some
      cloudService.lastSentSnapshot.set(previousSnapshot).unsafeRunSync()

      val result = cloudService.verifyAndSend(10L, "otherHash", IO.delay(List(().asRight[Throwable]))).unsafeRunSync()
      val expected = List(Left(PrecedingSnapshotIsNotThePreviousOne(previousSnapshot, 10L, "otherHash")))

      result shouldBe expected
    }
  }

  "enqueueSnapshot" - {
    "should enqueue, process snapshot and the last sent snapshot should be set correctly" in {
      cloudServiceEnqueue.enqueueSnapshot(File(""), 10L, "somehash").unsafeRunSync()
      cloudServiceEnqueue.enqueueSnapshotInfo(File(""), 10L, "somehash").unsafeRunSync()
      cloudServiceEnqueue.enqueueSnapshotInfo(File(""), 12L, "otherHash").unsafeRunSync()
      Thread.sleep(50) //to let the stream finish
      val expected = SnapshotSent(12L, "otherHash", false, true).some
      val result = cloudService.lastSentSnapshot.get.unsafeRunSync()

      result shouldBe expected
    }

    "should enqueue, and not allow to mark snapshot as sent if the previous snapshot wasn't sent completely" in {
      cloudServiceEnqueue.enqueueSnapshot(File(""), 10L, "somehash").unsafeRunSync()
      cloudServiceEnqueue.enqueueSnapshotInfo(File(""), 12L, "otherHash").unsafeRunSync()
      Thread.sleep(50) //to let the stream finish
      val expected = SnapshotSent(10L, "somehash", true, false).some
      val result = cloudService.lastSentSnapshot.get.unsafeRunSync()

      result shouldBe expected
    }
  }
}
