package org.constellation.util

import org.constellation.p2p.SerializedUDPMessage
import org.scalatest.FlatSpec
import org.constellation.serializer.KryoSerializer._
import akka.util.ByteString
import org.constellation.consensus.Consensus.RemoteMessage

case class TestMessage(a: String, b: Int) extends ProductHash with RemoteMessage

class KryoSerializerTest extends FlatSpec {

  "KryoSerializer" should "round trip serialize and deserialize SerializedUDPMessage" in {
    val message = SerializedUDPMessage(ByteString("test".getBytes), 1, 1, 1)

    val serialized = serialize(message)

    val deserialized = deserialize(serialized)

    assert(message == deserialized)

    val testMessage = TestMessage("test", 3)

    val messages = serializeGrouped(testMessage)

    val messagesSerialized = messages.map(serialize(_))

    val messagesDeserialized: Seq[SerializedUDPMessage] = messagesSerialized.map(deserialize(_).asInstanceOf[SerializedUDPMessage])

    val sorted = messagesDeserialized.sortBy(f => f.packetGroupId).flatMap(_.data).toArray

    val deserializedSorted = deserialize(sorted)

    assert(testMessage == deserializedSorted)
  }

}
