package org.constellation.util

import org.constellation.p2p.SerializedUDPMessage
import org.scalatest.FlatSpec
import org.constellation.serializer.KryoSerializer._
import akka.util.ByteString
import org.constellation.TestHelpers
import org.constellation.consensus.Consensus.RemoteMessage
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema._

class KryoSerializerTest extends FlatSpec {

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
