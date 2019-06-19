package org.constellation.p2p

import java.util.concurrent.Executors

import cats.effect.IO
import org.constellation.DAO
import org.constellation.primitives.Schema.Id
import org.constellation.util.{APIClient, PeerApiClient}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.{ExecutionContext, Future, TimeoutException}

class DataResolverTest extends FunSuite with BeforeAndAfter with Matchers {

  implicit val dao: DAO = mock(classOf[DAO])

  val storageMock: StorageMock = mock(classOf[StorageMock])
  val badNode: APIClient = mock(classOf[APIClient])
  val goodNode: APIClient = mock(classOf[APIClient])
  val hashes: List[String] = List("hash1", "hash2")
  val endpoint: String = "endpoint"

  before {
    when(dao.id)
      .thenReturn(Id("node1"))
    when(dao.edgeExecutionContext)
      .thenReturn(ExecutionContext.fromExecutor(Executors.newWorkStealingPool(8)))

    when(badNode.id)
      .thenReturn(Id("unresponsiveNode"))

    when(goodNode.id)
      .thenReturn(Id("responsiveNode"))

    when(
      goodNode.getNonBlockingIO[Option[String]](ArgumentMatchers.eq(s"$endpoint/hash1"), any(), any())(any(), any())
    ).thenReturn(IO { Some("resolved hash1") })

    when(
      goodNode.getNonBlockingIO[Option[String]](ArgumentMatchers.eq(s"$endpoint/hash2"), any(), any())(any(), any())
    ).thenReturn(IO { Some("resolved hash2") })

    when(badNode.getNonBlockingIO(anyString(), any(), any())(any(), any()))
      .thenReturn(IO.fromFuture(IO {
        Future.failed(new TimeoutException("Testing timeout, case just ignore this message."))
      }))
    reset(storageMock)
  }

  test("it should resolve and store data from responsive node") {

    val result = DataResolver
      .resolveDataByDistanceFlat[String](
        hashes,
        endpoint,
        List(PeerApiClient(badNode.id, badNode), PeerApiClient(goodNode.id, goodNode)),
        storageMock.loopbackStore
      )
      .unsafeRunSync()
    result shouldBe List(Some("resolved hash1"), Some("resolved hash2"))

    verify(storageMock, times(1)).loopbackStore("resolved hash1")
    verify(storageMock, times(1)).loopbackStore("resolved hash2")
  }

  test("it should throw an exception when bad node is unresponsive") {

    val resolverIO = DataResolver
      .resolveDataByDistanceFlat[String](
        hashes,
        endpoint,
        List(PeerApiClient(badNode.id, badNode)),
        storageMock.loopbackStore
      )

    resolverIO.attempt.unsafeRunSync() should matchPattern {
      case Left(_: TimeoutException) => ()
    }

    verify(storageMock, never()).loopbackStore(anyString())
  }

  test(
    "it should throw an exception when bad node returns empty response"
  ) {
    when(badNode.getNonBlockingIO[Option[String]](anyString(), any(), any())(any(), any()))
      .thenReturn(IO { None })

    val resolverIO = DataResolver
      .resolveDataByDistanceFlat[String](
        hashes,
        endpoint,
        List(PeerApiClient(badNode.id, badNode)),
        storageMock.loopbackStore
      )

    resolverIO.attempt.unsafeRunSync() should matchPattern {
      case Left(DataResolutionOutOfPeers("node1", "endpoint", _, _)) => ()
    }
    verify(storageMock, never()).loopbackStore(anyString())
  }

  test(
    "it should throw an exception when max error count reached"
  ) {

    val resolverIO = DataResolver
      .resolveData[String](
        "hash1",
        endpoint,
        List(PeerApiClient(badNode.id, badNode), PeerApiClient(badNode.id, badNode)),
        storageMock.loopbackStore,
        1
      )

    resolverIO.attempt.unsafeRunSync() should matchPattern {
      case Left(DataResolutionMaxErrors("endpoint", "hash1")) => ()
    }
    verify(storageMock, never()).loopbackStore(anyString())
  }

}

class StorageMock {

  def loopbackStore[T](item: T): Any =
    item
}
