package org.constellation.primitives

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import com.typesafe.scalalogging.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.checkpoint.{CheckpointParentService, CheckpointService}
import org.constellation.consensus.RandomData
import org.constellation.domain.configuration.NodeConfig
import org.constellation.primitives.Schema.{CheckpointCacheMetadata, EdgeHashType, Height, TypedEdgeHash}
import org.constellation.storage.SOEService
import org.constellation.util.Metrics
import org.constellation.{ConstellationExecutionContext, DAO, Fixtures}
import org.mockito.cats.IdiomaticMockitoCats
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito}
import org.scalatest.{FunSpecLike, Matchers}

class TipServiceTest
    extends FunSpecLike
    with IdiomaticMockito
    with IdiomaticMockitoCats
    with ArgumentMatchersSugar
    with Matchers {

  implicit val dao: DAO = prepareDAO()
  implicit val ioContextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)
  implicit val timer: Timer[IO] = IO.timer(ConstellationExecutionContext.unbounded)
  implicit val unsafeLogger = Slf4jLogger.getLogger[IO]

  val facilitatorFilter: FacilitatorFilter[IO] = mock[FacilitatorFilter[IO]]

  def prepareDAO(): DAO = {
    val d = new DAO()
    d.initialize(NodeConfig(primaryKeyPair = Fixtures.tempKey5))
    d.metrics = new Metrics(200)(d)
    d
  }

  describe("TrieBasedTipService") {

    it("limits maximum number of tips") {
      val limit = 6
      val concurrentTipService =
        new ConcurrentTipService[IO](limit, 10, 2, 2, 30, dao.checkpointParentService, dao, facilitatorFilter)

      val cbs = createIndexedCBmocks(limit * 3, { i =>
        createCBMock(i.toString)
      })

      val tasks = createShiftedTasks(cbs.toList, { cb =>
        concurrentTipService.update(cb, Height(1, 1))
      })
      tasks.par.foreach(_.unsafeRunAsyncAndForget)
      Thread.sleep(2000)
      concurrentTipService.size.unsafeRunSync() shouldBe limit
    }

    it("removes the tip ") {
      val maxTipUsage = 500
      val concurrentTipService =
        new ConcurrentTipService[IO](6, 10, maxTipUsage, 2, 30, dao.checkpointParentService, dao, facilitatorFilter)

      RandomData.go.initialDistribution
        .storeSOE()
        .flatMap(_ => RandomData.go.initialDistribution2.storeSOE())
        .unsafeRunSync()

      List(RandomData.go.initialDistribution, RandomData.go.initialDistribution2)
        .map(concurrentTipService.update(_, Height(1, 1)))
        .sequence
        .unsafeRunSync()

      concurrentTipService
        .remove(RandomData.go.initialDistribution.baseHash)(dao.metrics)
        .flatMap(_ => concurrentTipService.remove(RandomData.go.initialDistribution2.baseHash)(dao.metrics))
        .unsafeRunSync()

      concurrentTipService.toMap.unsafeRunSync() shouldBe Map()

    }
    it("safely updates a tip ") {
      val maxTipUsage = 10
      val concurrentTipService =
        new ConcurrentTipService[IO](6, 4, maxTipUsage, 0, 30, dao.checkpointParentService, dao, facilitatorFilter)

      RandomData.go.initialDistribution
        .storeSOE()
        .flatMap(_ => RandomData.go.initialDistribution2.storeSOE())
        .unsafeRunSync()

      List(RandomData.go.initialDistribution, RandomData.go.initialDistribution2)
        .map(concurrentTipService.update(_, Height(1, 1)))
        .sequence
        .unsafeRunSync()

      val cb = CheckpointBlock.createCheckpointBlock(Seq(Fixtures.tx), RandomData.startingTips.map { s =>
        TypedEdgeHash(s.hash, EdgeHashType.CheckpointHash)
      })(Fixtures.tempKey)

      val tasks = createShiftedTasks(List.fill(maxTipUsage + 1)(cb), { cb =>
        concurrentTipService.update(cb, Height(1, 1))
      })
      tasks.par.foreach(_.unsafeToFuture())
      Thread.sleep(2000)
      val tips = concurrentTipService.toMap
        .unsafeRunSync()
//      tips shouldBe Map.empty
      tips.get(RandomData.go.initialDistribution.baseHash) shouldBe None
      tips.get(RandomData.go.initialDistribution2.baseHash) shouldBe None
    }

  }

  private def prepareTransactions(): Seq[Transaction] = {
    val tx1 = mock[Transaction]
    tx1.hash shouldReturn "tx1"
    val tx2 = mock[Transaction]
    tx2.hash shouldReturn "tx2"
    Seq(tx1, tx2)
  }

  private def createCBMock(hash: String) = {
    val cb = mock[CheckpointBlock]
    cb.parentSOEHashes shouldReturn Seq.empty
//    cb.transactions shouldReturn Seq.empty
    cb.baseHash shouldReturn hash
    cb
  }

  def createIndexedCBmocks(size: Int, func: Int => CheckpointBlock) =
    (1 to size).map(func)

  def createShiftedTasks(
    cbs: List[CheckpointBlock],
    func: CheckpointBlock => IO[Any]
  ) =
    cbs.map(IO.shift *> func(_))

}
