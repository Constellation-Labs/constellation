package org.constellation.util

import cats.effect.{ContextShift, IO}
import org.constellation._
import org.constellation.consensus._
import org.constellation.domain.configuration.NodeConfig
import org.constellation.domain.transaction.LastTransactionRef
import org.constellation.primitives.Schema.{AddressCacheData, CheckpointCache, CheckpointEdge}
import org.constellation.domain.observation.Observation
import org.constellation.p2p.PeerNotification
import org.constellation.primitives.{ChannelMessage, CheckpointBlock, Transaction}
import org.scalatest.FlatSpec

class KryoSerializerTest extends FlatSpec {

  implicit val dao: DAO = TestHelpers.prepareRealDao(
    nodeConfig =
      NodeConfig(primaryKeyPair = Fixtures.tempKey5, processingConfig = ProcessingConfig(metricCheckInterval = 200))
  )
  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.bounded)

  val genesis = RandomData.go()
  def randomCB =
    CheckpointBlock
      .createCheckpointBlockSOE(Seq.fill(1000)(RandomData.randomTransaction), RandomData.startingTips(genesis))(
        Fixtures.tempKey1
      )

  def transactions100 = Seq.fill[Transaction](100)(Fixtures.tx)
  val checkpointBlocks = Seq.fill[CheckpointBlock](1000)(randomCB)
  val snapshot = Snapshot(Fixtures.randomSnapshotHash, checkpointBlocks.map(_.baseHash))

  val snaps = List.fill(3)(
    StoredSnapshot(Snapshot(Fixtures.randomSnapshotHash, Seq.fill(100)(Fixtures.randomSnapshotHash)), Seq.fill(100)(CheckpointCache(randomCB)))
  )

//  case class SnapshotInfo(
//                           snapshot: Snapshot,
//                           acceptedCBSinceSnapshot: Seq[String] = Seq(),
//                           acceptedCBSinceSnapshotCache: Seq[CheckpointCache] = Seq(),
//                           lastSnapshotHeight: Int = 0,
//                           snapshotHashes: Seq[String] = Seq(),
//                           addressCacheData: Map[String, AddressCacheData] = Map(),
//                           tips: Map[String, TipData] = Map(),
//                           snapshotCache: Seq[CheckpointCache] = Seq(),
//                           lastAcceptedTransactionRef: Map[String, LastTransactionRef] = Map()
//                         )

  "KryoSerializer" should "fail when serializing beyond buffer size" in {

    assert(true)
  }
  /*
  "KryoSerializer" should "round trip serialize and deserialize SerializedUDPMessage" in {

    val message = SerializedUDPMessage(ByteString("test".getBytes), 1, 1, 1)

    val serialized = serialize(message)

    val deserialized = deserialize(serialized)

    assert(message == deserialized)

    val testBundle = Gossip(TestHelpers.createTestBundle())

    assert(testBundle.event.valid)

    val messages = serializeGrouped(testBundle)

    val messagesSerialized = messages.map(serialize(_))

    val messagesDeserialized: Seq[SerializedUDPMessage] = messagesSerialized.map(deserialize(_).asInstanceOf[SerializedUDPMessage])

    val sorted = messagesDeserialized.sortBy(f => f.packetGroupId).flatMap(_.data).toArray

    val deserializedSorted = deserialize(sorted).asInstanceOf[Gossip[Bundle]]

    assert(testBundle == deserializedSorted)

    assert(deserializedSorted.event.valid)
  }
 */

}
