package org.constellation.consensus

import java.security.KeyPair
import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{TestActor, TestKit, TestProbe}
import akka.util.Timeout
import org.constellation.crypto.KeyUtils
import org.constellation.{Data, LevelDBActor}
import org.constellation.Fixtures._
import org.constellation.LevelDB.DBGet
import org.constellation.primitives.Schema._
import org.constellation.util.Signed
import org.scalatest.{FlatSpec, FlatSpecLike}
import org.constellation.util.SignHelp._

class EdgeProcessorTest extends TestKit(ActorSystem("EdgeProcessorTest")) with FlatSpecLike {

  "The handleConflictingCheckpoint method" should "handle conflicting checkpointBlocks" in {

    val metricsManager = TestProbe()

    val dao = new Data()

    implicit val timeout: Timeout = Timeout(30, TimeUnit.SECONDS)

    val dbActor = TestProbe()


    dao.dbActor = dbActor.ref

    dao.metricsManager = metricsManager.ref

    implicit val keyPair: KeyPair = KeyUtils.makeKeyPair()

    dao.keyPair = keyPair

    val CoinBaseHash = "coinbase"

    val debtAddress = KeyUtils.makeKeyPair().getPublic
    val selfAddress = KeyUtils.makeKeyPair().getPublic

    val dumTx = dummyTx(dao)

    val genTXHash = dumTx.edge.signedObservationEdge.signatureBatch.hash

    val cb = CheckpointEdgeData(Seq(genTXHash))

    val obe = ObservationEdge(
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      TypedEdgeHash(CoinBaseHash, EdgeHashType.CheckpointHash),
      data = Some(TypedEdgeHash(cb.hash, EdgeHashType.CheckpointDataHash))
    )

    val soed = signedObservationEdge(obe)

    val tips = Seq(soed, soed)

    val transactions = Seq(dummyTx(dao), dummyTx(dao))

    val existingCheckpointBlock = createCheckpointBlock(transactions, tips)

    val newKeyPair = KeyUtils.makeKeyPair()

    val conflictingCheckpointBlock = existingCheckpointBlock.plus(newKeyPair)

    val cpcd = CheckpointCacheData(existingCheckpointBlock)

    val cpHash = cpcd.checkpointBlock.soeHash
    val ccHash = conflictingCheckpointBlock.soeHash

    dbActor.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any): TestActor.AutoPilot = msg match {
        case DBGet(`cpHash`) =>
          sender ! Some(cpcd.checkpointBlock.resolvedOE)
          TestActor.KeepRunning
        case DBGet(`ccHash`) =>
          sender ! Some(CheckpointCacheData(conflictingCheckpointBlock, true))
          TestActor.KeepRunning
        case _ =>
          sender ! None
          TestActor.KeepRunning
      }
    })

    val mostValidCheckpointBlock = EdgeProcessor.handleConflictingCheckpoint(cpcd, conflictingCheckpointBlock, dao)

    assert(mostValidCheckpointBlock == conflictingCheckpointBlock)
  }

}
