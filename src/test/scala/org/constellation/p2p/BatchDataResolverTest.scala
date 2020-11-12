//package org.constellation.p2p
//
//import cats.effect.{ContextShift, IO}
//import org.constellation.schema.schema.Id
//import org.constellation.util.PeerApiClient
//import org.constellation.{ConstellationExecutionContext, DAO, Fixtures, ProcessingConfig}
//import org.mockito.IdiomaticMockitoBase.Times
//import org.mockito.cats.IdiomaticMockitoCats
//import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
//import org.scalatest.{BeforeAndAfter, FunSpecLike, Matchers}
//
//class BatchDataResolverTest
//    extends FunSpecLike
//    with ArgumentMatchersSugar
//    with BeforeAndAfter
//    with IdiomaticMockito
//    with IdiomaticMockitoCats
//    with Matchers {
//
//  implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
//  implicit val dao: DAO = mock[DAO]
//
//  dao.id shouldReturn Fixtures.id
//  dao.processingConfig shouldReturn ProcessingConfig()
//
//  describe("resolving batch data") {
//    it("should return data from first peer") {
//      val hashes = List("x01", "x02", "x03")
//      val firstMockApiClient: APIClient = mock[APIClient]
//      firstMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        hashes.map(h => (h, "resolved"))
//      val peers = List(PeerApiClient(Id("first"), firstMockApiClient))
//
//      val resolves = DataResolver.resolveBatchData[String](hashes, "endpoint", peers)(contextShift).unsafeRunSync()
//
//      resolves.size shouldBe 3
//    }
//
//    it("should return data collecting from two peers") {
//      val hashes = List("x01", "x02", "x03")
//      val firstMockApiClient: APIClient = mock[APIClient]
//      firstMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        List(("x01", "resolved"), ("x03", "resolved"))
//      val secondMockApiClient: APIClient = mock[APIClient]
//      secondMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        List(("x02", "resolved"))
//      val peers =
//        List(PeerApiClient(Id("first"), firstMockApiClient), PeerApiClient(Id("second"), secondMockApiClient))
//
//      val resolves = DataResolver.resolveBatchData[String](hashes, "endpoint", peers)(contextShift).unsafeRunSync()
//
//      resolves.size shouldBe 3
//    }
//
//    it("should raise error if data is not available") {
//      val hashes = List("x01", "x02", "x03")
//      val firstMockApiClient: APIClient = mock[APIClient]
//      firstMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        List(("x01", "resolved"))
//      val secondMockApiClient: APIClient = mock[APIClient]
//      secondMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        List(("x02", "resolved"))
//      val peers =
//        List(PeerApiClient(Id("first"), firstMockApiClient), PeerApiClient(Id("second"), secondMockApiClient))
//
//      assertThrows[DataResolutionOutOfPeers] {
//        DataResolver.resolveBatchData[String](hashes, "endpoint", peers)(contextShift).unsafeRunSync()
//      }
//    }
//
//    it("should stop collecting data if has all responses") {
//      val hashes = List("x01", "x02", "x03")
//      val firstMockApiClient: APIClient = mock[APIClient]
//      firstMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        List(("x01", "resolved"))
//      val secondMockApiClient: APIClient = mock[APIClient]
//      secondMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        List(("x02", "resolved"), ("x03", "resolved"))
//      val thirdMockApiClient: APIClient = mock[APIClient]
//      thirdMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *) shouldReturnF
//        List()
//      val peers = List(
//        PeerApiClient(Id("first"), firstMockApiClient),
//        PeerApiClient(Id("second"), secondMockApiClient),
//        PeerApiClient(Id("third"), thirdMockApiClient)
//      )
//
//      DataResolver.resolveBatchData[String](hashes, "endpoint", peers)(contextShift).unsafeRunSync()
//
//      firstMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *).wasCalled(once)
//      secondMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *).wasCalled(once)
//      thirdMockApiClient.postNonBlockingIO[List[(String, String)]](*, *, *)(*)(*, *).wasCalled(Times(0))
//    }
//  }
//}
