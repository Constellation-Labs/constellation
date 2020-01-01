package org.constellation

import java.net.InetSocketAddress
import java.security.{KeyPair, PublicKey}
import java.util.{Random, UUID}

import cats.effect.{ContextShift, IO}
import com.google.common.hash.Hashing
import constellation._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.constellation.consensus.RandomData
import org.constellation.keytool.KeyUtils
import org.constellation.primitives.Schema._
import org.constellation.domain.transaction.{TransactionChainService, TransactionService}
import org.constellation.primitives.{CheckpointBlock, Transaction}
import org.constellation.schema.Id
import org.constellation.util.{APIClient, SignHelp}

import scala.concurrent.ExecutionContext

object Fixtures {

  implicit val cs: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)
  implicit val logger = Slf4jLogger.getLogger[IO]

  val kp: KeyPair = KeyUtils.makeKeyPair()
  val kp1: KeyPair = KeyUtils.makeKeyPair()

  def tx: Transaction =
    TransactionService.createTransaction[IO](kp.address, kp1.address, 1L, kp)(TransactionChainService[IO]).unsafeRunSync

  def randomSnapshotHash = Hashing.sha256.hashBytes(UUID.randomUUID().toString.getBytes).toString

  def transactions100 = Seq.fill[Transaction](1000)(Fixtures.tx)

  def getRandomTip: SignedObservationEdge = {
    val oe = ObservationEdge(Seq.fill[TypedEdgeHash](2)
      (TypedEdgeHash(Fixtures.randomSnapshotHash, EdgeHashType.CheckpointHash)),
      TypedEdgeHash(Fixtures.randomSnapshotHash, EdgeHashType.CheckpointHash))
    signedObservationEdge(oe)(Fixtures.kp)
  }

  def randomCB(numTx: Int = 100) = {
    val cb = CheckpointBlock
      .createCheckpointBlockSOE(Seq.fill(numTx)(RandomData.randomTransaction),
        Seq.fill(2)(getRandomTip))(
        Fixtures.tempKey1
      )
    println(cb)
    cb
  }

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
  val id = publicKey.toId
  val id1 = publicKey1.toId
  val id2 = publicKey2.toId
  val id3 = publicKey3.toId
  val id4 = publicKey4.toId
  val id5 = publicKey5.toId

  val address1: InetSocketAddress = constellation.addressToSocket("localhost:16181")
  val address2: InetSocketAddress = constellation.addressToSocket("localhost:16182")
  val address3: InetSocketAddress = constellation.addressToSocket("localhost:16183")
  val address4: InetSocketAddress = constellation.addressToSocket("localhost:16184")
  val address5: InetSocketAddress = constellation.addressToSocket("localhost:16185")

  val addPeerRequest = PeerMetadata("host:", 1, id: Id, resourceInfo = ResourceInfo(diskUsableBytes = 1073741824))

  val tempKeySet = Seq(tempKey, tempKey2, tempKey3, tempKey4, tempKey5)

  val idSet4 = Set(id1, id2, id3, id4)
  val idSet4B = Set(id1, id2, id3, id5)
  val idSet5 = Set(id1, id2, id3, id4, id5)

  def getRandomElement[T](list: Seq[T], random: Random): T = list(random.nextInt(list.length))

  def dummyTx(data: DAO, amt: Long = 1L, src: Id = id): Transaction =
    TransactionService
      .createTransaction[IO](data.selfAddressStr, src.address, amt, data.keyPair)(
        TransactionChainService[IO]
      )
      .unsafeRunSync

  def makeTransaction(srcAddressString: String, destinationAddressString: String, amt: Long, keyPair: KeyPair) =
    TransactionService
      .createTransaction[IO](srcAddressString, destinationAddressString, amt, keyPair)(
        TransactionChainService[IO]
      )
      .unsafeRunSync

  def makeDummyTransaction(src: String, dst: String, keyPair: KeyPair) =
    TransactionService.createDummyTransaction(src, dst, keyPair)(TransactionChainService[IO]).unsafeRunSync

  def getAPIClient(hostName: String, httpPort: Int)(implicit dao: DAO, executionContext: ExecutionContext) = {
    val api = APIClient(host = hostName, port = httpPort)(dao.backend, dao)
    api.id = id
    api
  }

}
