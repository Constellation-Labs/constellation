package org.constellation

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}

import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.{Id, Peer}
import org.constellation.util.Signed

object Fixtures2 {

  val tempKey: KeyPair = KeyUtils.makeKeyPair()
  val tempKey1: KeyPair = KeyUtils.makeKeyPair()
  val tempKey2: KeyPair = KeyUtils.makeKeyPair()
  val tempKey3: KeyPair = KeyUtils.makeKeyPair()
  val tempKey4: KeyPair = KeyUtils.makeKeyPair()
  val tempKey5: KeyPair = KeyUtils.makeKeyPair()
  val publicKey: PublicKey = tempKey.getPublic
  val publicKey1: PublicKey = tempKey1.getPublic
  val publicKey2: PublicKey = tempKey2.getPublic
  val publicKey3: PublicKey = tempKey3.getPublic
  val publicKey4: PublicKey = tempKey4.getPublic
  val publicKey5: PublicKey = tempKey5.getPublic
  val address = new InetSocketAddress("127.0.0.1", 16180)
  val id = Id(publicKey.encoded)
  val id1 = Id(publicKey1.encoded)
  val id2 = Id(publicKey2.encoded)
  val id3 = Id(publicKey3.encoded)
  val id4 = Id(publicKey4.encoded)
  val id5 = Id(publicKey5.encoded)
  val signedPeer: Signed[Peer] = Peer(id, Some(address), Some(address), Seq(), "").signed()(tempKey)

  val address1: InetSocketAddress = constellation.addressToSocket("localhost:16181")
  val address2: InetSocketAddress = constellation.addressToSocket("localhost:16182")
  val address3: InetSocketAddress = constellation.addressToSocket("localhost:16183")
  val address4: InetSocketAddress = constellation.addressToSocket("localhost:16184")
  val address5: InetSocketAddress = constellation.addressToSocket("localhost:16185")

  val idSet4 = Set(id1, id2, id3, id4)
  val idSet4B = Set(id1, id2, id3, id5)
  val idSet5 = Set(id1, id2, id3, id4, id5)


}
