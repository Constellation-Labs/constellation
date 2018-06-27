package org.constellation

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}

import constellation._
import org.constellation.crypto.KeyUtils
import org.constellation.primitives.Schema.{Id, Peer}
import org.constellation.primitives.Transaction
import org.constellation.util.Signed

object Fixtures2 {

  val tempKey: KeyPair = KeyUtils.makeKeyPair()
  val tempKey1: KeyPair = KeyUtils.makeKeyPair()
  val tempKey2: KeyPair = KeyUtils.makeKeyPair()
  val tempKey3: KeyPair = KeyUtils.makeKeyPair()
  val tempKey4: KeyPair = KeyUtils.makeKeyPair()
  val tempKey5: KeyPair = KeyUtils.makeKeyPair()
  val tx = Transaction(0L, tempKey.getPublic, tempKey.getPublic, 1L)
  val publicKey: PublicKey = tempKey.getPublic
  val publicKey1: PublicKey = tempKey1.getPublic
  val publicKey2: PublicKey = tempKey2.getPublic
  val publicKey3: PublicKey = tempKey3.getPublic
  val publicKey4: PublicKey = tempKey4.getPublic
  val publicKey5: PublicKey = tempKey5.getPublic
  val address = new InetSocketAddress("127.0.0.1", 16180)
  val id = Id(publicKey)
  val id1 = Id(publicKey1)
  val id2 = Id(publicKey2)
  val id3 = Id(publicKey3)
  val id4 = Id(publicKey4)
  val id5 = Id(publicKey5)
  val signedPeer: Signed[Peer] = Peer(id, address, Set()).signed()(tempKey)

  val address1: InetSocketAddress = constellation.addressToSocket("localhost:16181")
  val address2: InetSocketAddress = constellation.addressToSocket("localhost:16182")
  val address3: InetSocketAddress = constellation.addressToSocket("localhost:16183")
  val address4: InetSocketAddress = constellation.addressToSocket("localhost:16184")
  val address5: InetSocketAddress = constellation.addressToSocket("localhost:16185")

  val idSet4 = Set(id1, id2, id3, id4)
  val idSet4B = Set(id1, id2, id3, id5)
  val idSet5 = Set(id1, id2, id3, id4, id5)

  val prevBlock4 = Block("sig", 0, "", idSet4, 0L, Seq())
  val latestBlock4B = Block("sig", 0, "", idSet4B, 1L, Seq())

  val transaction1: Transaction =
    Transaction.senderSign(Transaction(0L, tempKey1.getPublic, tempKey4.getPublic, 33L), tempKey1.getPrivate)

  val transaction2: Transaction =
    Transaction.senderSign(Transaction(1L, tempKey2.getPublic, tempKey3.getPublic, 14L), tempKey2.getPrivate)

  val transaction3: Transaction =
    Transaction.senderSign(Transaction(2L, tempKey3.getPublic, tempKey2.getPublic, 2L), tempKey3.getPrivate)

  val transaction4: Transaction =
    Transaction.senderSign(Transaction(3L, tempKey4.getPublic, tempKey1.getPublic, 20L), tempKey4.getPrivate)


}
