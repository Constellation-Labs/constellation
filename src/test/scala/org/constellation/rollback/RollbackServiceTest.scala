package org.constellation.rollback

import cats.data.{EitherT, NonEmptyList}
import cats.effect.IO
import cats.implicits.catsSyntaxEitherId
import org.constellation.domain.cloud.HeightHashFileStorage
import org.constellation.domain.redownload.RedownloadService
import org.constellation.domain.storage.LocalFileStorage
import org.constellation.genesis.GenesisObservationS3Storage
import org.constellation.p2p.Cluster
import org.constellation.schema.snapshot.{SnapshotInfo, StoredSnapshot}
import org.constellation.storage.SnapshotService
import org.constellation.DAO
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.mockito.cats.IdiomaticMockitoCats
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.BeforeAndAfter
import org.scalatest.matchers.should.Matchers

class RollbackServiceTest
    extends AnyFreeSpec
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with Matchers
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val contextShift = IO.contextShift(scala.concurrent.ExecutionContext.global)

  var dao: DAO = _
  var snapshotService: SnapshotService[IO] = _
  var snapshotLocalStorage: LocalFileStorage[IO, StoredSnapshot] = _
  var snapshotInfoLocalStorage: LocalFileStorage[IO, SnapshotInfo] = _
  var snapshotCloudStorage: NonEmptyList[HeightHashFileStorage[IO, StoredSnapshot]] = _
  var snapshotInfoCloudStorage: NonEmptyList[HeightHashFileStorage[IO, SnapshotInfo]] = _
  var genesisObservationCloudStorage: NonEmptyList[GenesisObservationS3Storage[IO]] = _
  var redownloadService: RedownloadService[IO] = _
  var cluster: Cluster[IO] = _
  var rollbackService: RollbackService[IO] = _

  before {
    rollbackService = new RollbackService[IO](
      dao,
      snapshotService,
      snapshotLocalStorage,
      snapshotInfoLocalStorage,
      snapshotCloudStorage,
      snapshotInfoCloudStorage,
      genesisObservationCloudStorage,
      redownloadService,
      cluster
    )
  }

  "executeWithFallback" - {
    sealed trait Storage[F[_]] { def read: EitherT[F, Throwable, String] }
    val first = new Storage[IO] { def read = EitherT.fromEither(new Throwable("firstError").asLeft[String]) }
    val second = new Storage[IO] { def read = EitherT.fromEither(new Throwable("secondError").asLeft[String]) }
    val third = new Storage[IO] { def read = EitherT.fromEither("third".asRight[Throwable]) }

    "should fallback to first successfully computed value" in {
      val buckets = NonEmptyList(first, List(second, third))
      val result = rollbackService.executeWithFallback(buckets)(_.read).value.unsafeRunSync()

      result shouldBe Right("third")
    }

    "should return last error when none of the backups successfully returned value" in {
      val buckets = NonEmptyList(first, List(second))
      val result = rollbackService.executeWithFallback(buckets)(_.read).value.unsafeRunSync().left.get.getMessage

      result shouldBe "secondError"
    }
  }
}
