package org.constellation

import cats.syntax.all._
import cats.effect.{ExitCode, IO, IOApp, Resource}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.util.IOUtils
import fs2.Stream
import org.constellation.migrations.SnapshotInfoV1Migration
import org.constellation.schema.snapshot.{SnapshotInfo, SnapshotInfoV1}
import org.constellation.serialization.KryoSerializer

object Statistics extends IOApp {

  val s3BucketName = "constellationlabs-block-explorer-mainnet"
  val s3Region = "us-west-1"
  val height = "1114830"
  val hash = "6160db2d7582e0a6fd20e0a1593cbb28ffea4b8af00ecf3b1788b7f35cd5c665"

  val s3Client = Stream.eval {
    IO {
      AmazonS3ClientBuilder
        .standard()
        .withRegion(s3Region)
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .build()
    }
  }

  val fetchSnapshotInfo = {
    for {
      client <- s3Client
      obj <- Stream.eval {
        IO {
          client.getObject(s3BucketName, s"snapshots/${height}-${hash}/${hash}-snapshot_info")
        }
      }
      is <- Stream
        .resource(Resource.make(IO {
          obj.getObjectContent
        })(o => IO(o.close())))
      bytes <- Stream.eval { IO { IOUtils.toByteArray(is) } }
      snapshotInfoV1 <- Stream.eval { IO { KryoSerializer.deserializeCast[SnapshotInfoV1](bytes) } }
      snapshotInfo = SnapshotInfoV1Migration.convert(snapshotInfoV1)
    } yield snapshotInfo
  }

  val main: Stream[IO, Unit] = {
    Stream.eval { KryoSerializer.init[IO] } >>
      fetchSnapshotInfo.flatMap { snapshotInfo =>
        Stream.eval {
          IO {
            println(s"Total addresses: ${calculateTotalAddresses(snapshotInfo)}")
            println(s"Total supply: ${calculateTotalSupply(snapshotInfo)}")
            val topN = getAddresses(snapshotInfo).sortBy(-_._2).take(100)
            topN.foreach { a =>
              println(s"Address: ${a._1} | Balance: ${a._2}")
            }
          }
        }.handleErrorWith(err => Stream.eval_(IO { println(err.getMessage) }))
      }
  }

  override def run(args: List[String]): IO[ExitCode] =
    main.compile.drain.as(ExitCode.Success)

  def calculateTotalAddresses(snapshotInfo: SnapshotInfo): Long =
    snapshotInfo.addressCacheData.size

  def calculateTotalSupply(snapshotInfo: SnapshotInfo): Long =
    snapshotInfo.addressCacheData.mapValues(_.balance).values.toList.sum

  def getAddresses(snapshotInfo: SnapshotInfo): List[(String, Long)] =
    snapshotInfo.addressCacheData
      .mapValues(_.balance)
      .toList

}
