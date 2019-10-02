package org.constellation

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.softwaremill.sttp.SttpBackend
import com.softwaremill.sttp.okhttp.OkHttpFutureBackend
import org.constellation.storage.RecentSnapshot
import org.scalatest.{BeforeAndAfterAll, FreeSpecLike}

import scala.concurrent.Future

class ClusterSnapshotsTest
    extends TestKit(ActorSystem("ClusterSnapshotsTest"))
    with FreeSpecLike
    with BeforeAndAfterAll {

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materialize: ActorMaterializer = ActorMaterializer()
  implicit val C: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  implicit val backend: SttpBackend[Future, Nothing] =
    OkHttpFutureBackend()(ConstellationExecutionContext.unbounded)

  "should check cluster snapshots heights" in {
    val (ignoreIPs, auxAPIs) = ComputeTestUtil.getAuxiliaryNodes()
    val apis = ComputeTestUtil.createApisFromIpFile(ignoreIPs).toList

    val response = apis
      .traverse(api => api.getNonBlockingIO[List[RecentSnapshot]]("snapshot/recent")(C).map((api.hostName, _)))
      .unsafeRunSync

    val mapped = response
      .flatMap(a => a._2.map(s => (s.height -> (s.hash -> a._1))))
      .groupBy(_._1)
      .mapValues(_.map(_._2).groupBy(_._1).mapValues(_.map(_._2).size))
      .toList
      .sortBy(_._1)

    println(mapped)

  }
}
