package org.constellation

import cats.effect.{ExitCode, IO, IOApp}
import org.constellation.consensus.{Snapshot, StoredSnapshot}
import org.constellation.domain.cloud.CloudStorage.StorageName
import org.constellation.infrastructure.cloud.AWSStorage
import org.constellation.infrastructure.snapshot.SnapshotFileStorage
import org.constellation.rollback.RollbackService
import org.constellation.serializer.KryoSerializer

object TestApp extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val accessKey = ""
    val secretKey = ""
    val region = "us-west-1"
    val bucketName = "constellationlabs-block-explorer-test"

    val highestSnapshotToRollback = "8477a21b0ff4f219ab487c472bcb3cd2171f05c1cf9bffa40076f831faf3aca3"

    // 9a5057a4183ec0fa17768b1ae8f51173813a93ed9025b42554e0ec9a823ce160
    // 3184092827dc622b016256f68c1f0e512e1f73f3e7423304dae5783048b220cb
    // 8477a21b0ff4f219ab487c472bcb3cd2171f05c1cf9bffa40076f831faf3aca3

    for {
      _ <- IO.unit

      sfs = SnapshotFileStorage[IO](s"tmp/snapshots")
      aws = new AWSStorage[IO](accessKey, secretKey, region, bucketName)
      rs = new RollbackService[IO](sfs, aws)

      snapshot <- aws.get(highestSnapshotToRollback, StorageName.Snapshot).flatMap { bytes =>
        IO { KryoSerializer.deserializeCast[StoredSnapshot](bytes) }
      }
      balances <- rs.rollbackHylo.apply(snapshot)

      _ <- IO {
        println(balances)
      }
    } yield ExitCode.Success
  }

}
