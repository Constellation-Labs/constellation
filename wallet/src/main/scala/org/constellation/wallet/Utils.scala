package org.constellation.wallet

import java.security.{KeyPair, PrivateKey, PublicKey}

import cats.Monoid
import com.google.common.hash.Hashing
import com.twitter.chill.{IKryoRegistrar, Kryo, KryoPool, ScalaKryoInstantiator}
import enumeratum._
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.{bytes2hex, hexToPublicKey, signData, verifySignature}

import scala.util.Random

case class Id(hex: String) {

  @transient
  val short: String = hex.toString.slice(0, 5)

  @transient
  val medium: String = hex.toString.slice(0, 10)

  @transient
  lazy val address: String = KeyUtils.publicKeyToAddressString(toPublicKey)

  @transient
  lazy val toPublicKey: PublicKey = hexToPublicKey(hex)

  @transient
  lazy val bytes: Array[Byte] = KeyUtils.hex2bytes(hex)

  @transient
  lazy val bigInt: BigInt = BigInt(bytes)

  @transient
  lazy val distance: BigInt = BigInt(Hashing.sha256.hashBytes(toPublicKey.getEncoded).asBytes())
}

class ConstellationKryoRegistrar extends IKryoRegistrar {
  override def apply(kryo: Kryo): Unit =
    this.registerClasses(kryo)

  def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Transaction], 1001)
    kryo.register(classOf[ObservationEdge], 1002)
    kryo.register(classOf[SignedObservationEdge], 1003)
    kryo.register(classOf[TypedEdgeHash], 1004)
    kryo.register(classOf[Edge[TransactionEdgeData]], 1005)
    kryo.register(classOf[SignatureBatch], 1006)
    kryo.register(classOf[HashSignature], 1007)
    kryo.register(classOf[Enumeration#Value], 1008)
    kryo.register(classOf[TransactionEdgeData], 1009)
    kryo.register(classOf[LastTransactionRef], 1010)

    kryo.register(classOf[Id], 1011)

    kryo.register(classOf[Array[Byte]], 1012)
    kryo.register(classOf[Option[Long]], 1013)
    kryo.register(classOf[String], 1014)
    kryo.register(classOf[Boolean], 1015)

    kryo.register(EdgeHashType.AddressHash.getClass, 1024)
    kryo.register(EdgeHashType.CheckpointDataHash.getClass, 1025)
    kryo.register(EdgeHashType.CheckpointHash.getClass, 1026)
    kryo.register(EdgeHashType.TransactionDataHash.getClass, 1027)
    kryo.register(EdgeHashType.TransactionHash.getClass, 1028)
    kryo.register(EdgeHashType.ValidationHash.getClass, 1029)
    kryo.register(EdgeHashType.BundleDataHash.getClass, 1030)
    kryo.register(EdgeHashType.ChannelMessageDataHash.getClass, 1031)

    kryo.register(Class.forName("scala.Enumeration$Val"), 1017)

    kryo.register(Class.forName("scala.collection.immutable.HashSet$HashSet1"), 1018)
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"), 1019)
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"), 1020)
    kryo.register(Class.forName("scala.None$"), 1021)
    kryo.register(Class.forName("scala.collection.immutable.Nil$"), 1022)
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"), 1023)
  }
}

object KryoSerializer {

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  val kryoPool = KryoPool.withByteArrayOutputStream(
    10,
    new ScalaKryoInstantiator()
      .setRegistrationRequired(true)
      .withRegistrar(new ConstellationKryoRegistrar())
  )

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)

  def deserializeCast[T](bytes: Array[Byte]): T =
    kryoPool.fromBytes(bytes).asInstanceOf[T]
}

object Hashable {
  def hash(a: AnyRef): String = Hashing.sha256().hashBytes(KryoSerializer.serializeAnyRef(a)).toString
}

case class LastTransactionRef(hash: String, ordinal: Long)

object LastTransactionRef {
  val empty = LastTransactionRef("", 0L)
}

case class TransactionEdgeData(
  amount: Long,
  lastTxRef: LastTransactionRef,
  fee: Option[Long] = None,
  salt: Long = Random.nextLong()
)

sealed trait EdgeHashType extends EnumEntry

object EdgeHashType extends Enum[EdgeHashType] with CirceEnum[EdgeHashType] {

  case object AddressHash extends EdgeHashType
  case object CheckpointDataHash extends EdgeHashType
  case object CheckpointHash extends EdgeHashType
  case object TransactionDataHash extends EdgeHashType
  case object TransactionHash extends EdgeHashType
  case object ValidationHash extends EdgeHashType
  case object BundleDataHash extends EdgeHashType
  case object ChannelMessageDataHash extends EdgeHashType

  val values = findValues
}

case class TypedEdgeHash(hash: String, hashType: EdgeHashType, baseHash: Option[String] = None)

case class ObservationEdge(
  parents: Seq[TypedEdgeHash],
  data: TypedEdgeHash
)

case class HashSignature(
  signature: String,
  id: Id
) extends Ordered[HashSignature] {

  def address: String = KeyUtils.publicKeyToAddressString(publicKey)

  def valid(hash: String): Boolean =
    verifySignature(hash.getBytes(), KeyUtils.hex2bytes(signature))(publicKey)

  def publicKey: PublicKey = id.toPublicKey

  override def compare(that: HashSignature): Int =
    signature.compare(that.signature)
}

case class SignatureBatch(
  hash: String,
  signatures: Seq[HashSignature]
) extends Monoid[SignatureBatch] {
  override def empty: SignatureBatch = SignatureBatch(hash, Seq())

  override def combine(x: SignatureBatch, y: SignatureBatch): SignatureBatch =
    x.copy(signatures = (x.signatures ++ y.signatures).distinct.sorted)
}

case class SignedObservationEdge(signatureBatch: SignatureBatch) {
  def baseHash: String = signatureBatch.hash
}

object SignHelp {

  def signHashWithKey(hash: String, privateKey: PrivateKey): String =
    bytes2hex(signData(hash.getBytes())(privateKey))

  def signedObservationEdge(oe: ObservationEdge)(implicit kp: KeyPair): SignedObservationEdge =
    SignedObservationEdge(hashSignBatchZeroTyped(Hashable.hash(oe), kp))

  def hashSignBatchZeroTyped(hash: String, keyPair: KeyPair): SignatureBatch =
    SignatureBatch(hash, Seq(hashSign(hash, keyPair)))

  def hashSign(hash: String, keyPair: KeyPair): HashSignature =
    HashSignature(
      signHashWithKey(hash, keyPair.getPrivate),
      Id(KeyUtils.publicKeyToHex(keyPair.getPublic))
    )

}

case class Edge[D](
  observationEdge: ObservationEdge,
  signedObservationEdge: SignedObservationEdge,
  data: D
) {
  def baseHash: String = signedObservationEdge.signatureBatch.hash

  def parents: Seq[TypedEdgeHash] = observationEdge.parents
}

case class TransactionEdge()

object TransactionEdge {

  def createTransactionEdge(
    src: String,
    dst: String,
    lastTxRef: LastTransactionRef,
    amount: Double,
    keyPair: KeyPair,
    fee: Option[Double] = None
  ): Edge[TransactionEdgeData] = {
    val amountToUse = amount * 1e8.toLong
    val feeToUse = fee.map(_ * 1e8.toLong).map(_.toLong)
    val txData = TransactionEdgeData(amountToUse.toLong, lastTxRef, feeToUse)
    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(Hashable.hash(txData), EdgeHashType.TransactionDataHash)
    )
    val soe = SignHelp.signedObservationEdge(oe)(keyPair)
    Edge(oe, soe, txData)
  }
}
