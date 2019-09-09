package org.constellation.p2p

import cats.effect.{ContextShift, IO}
import org.constellation.primitives.Schema.Id
import org.constellation.util.{APIClient, PeerApiClient}
import org.constellation.{DAO, Fixtures}
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

import scala.concurrent.{ExecutionContext, Future, TimeoutException}

class DataResolverTest extends FunSuite with BeforeAndAfter with Matchers {

  implicit val dao: DAO = mock(classOf[DAO])
  implicit val contextShift: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val badNode: APIClient = mock(classOf[APIClient])
  val goodNode: APIClient = mock(classOf[APIClient])
  val hashes: List[String] = List("hash1", "hash2")
  val endpoint: String = "endpoint"

  before {
    when(dao.id)
      .thenReturn(Id("node1"))

    when(badNode.id)
      .thenReturn(Fixtures.id)

    when(goodNode.id)
      .thenReturn(Fixtures.id2)

    when(
      goodNode
        .getNonBlockingIO[Option[String]](ArgumentMatchers.eq(s"$endpoint/hash1"), any(), any())(any())(any(), any())
    ).thenReturn(IO { Some("resolved hash1") })

    when(
      goodNode
        .getNonBlockingIO[Option[String]](ArgumentMatchers.eq(s"$endpoint/hash2"), any(), any())(any())(any(), any())
    ).thenReturn(IO { Some("resolved hash2") })

    when(badNode.getNonBlockingIO(anyString(), any(), any())(any())(any(), any()))
      .thenReturn(IO.fromFuture(IO {
        Future.failed(new TimeoutException("Testing timeout, case just ignore this message."))
      }))
  }

  test("it should resolve and store data from responsive node") {

    val result = DataResolver
      .resolveDataByDistanceFlat[String](
        hashes,
        endpoint,
        List(PeerApiClient(badNode.id, badNode), PeerApiClient(goodNode.id, goodNode))
      )(contextShift)
      .unsafeRunSync()
    result shouldBe List("resolved hash1", "resolved hash2")

  }

  test("it should throw an exception when bad node is unresponsive") {

    val resolverIO = DataResolver
      .resolveDataByDistanceFlat[String](
        hashes,
        endpoint,
        List(PeerApiClient(badNode.id, badNode))
      )(contextShift)

    assertThrows[TimeoutException] {
      resolverIO.unsafeRunSync()
    }
  }

  test(
    "it should throw an exception when bad node returns empty response"
  ) {
    when(badNode.getNonBlockingIO[Option[String]](anyString(), any(), any())(any())(any(), any()))
      .thenReturn(IO { None })

    val resolverIO = DataResolver
      .resolveDataByDistanceFlat[String](
        hashes,
        endpoint,
        List(PeerApiClient(badNode.id, badNode))
      )(contextShift)

    resolverIO.attempt.unsafeRunSync() should matchPattern {
      case Left(DataResolutionOutOfPeers("node1", "endpoint", _, _)) => ()
    }
  }

  test(
    "it should throw an exception when max error count reached"
  ) {

    val resolverIO = DataResolver
      .resolveData[String](
        "hash1",
        endpoint,
        List(PeerApiClient(badNode.id, badNode), PeerApiClient(badNode.id, badNode)),
        1
      )(contextShift)

    resolverIO.attempt.unsafeRunSync() should matchPattern {
      case Left(DataResolutionMaxErrors("endpoint", "hash1")) => ()
    }
  }

}
