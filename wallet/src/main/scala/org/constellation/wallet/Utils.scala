package org.constellation.wallet

import java.security.{KeyPair, PrivateKey, PublicKey}

import cats.Monoid
import com.google.common.hash.Hashing
import com.twitter.chill.{IKryoRegistrar, Kryo, KryoPool, ScalaKryoInstantiator}
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.{bytes2hex, hexToPublicKey, signData, verifySignature}
import org.constellation.wallet.EdgeHashType.EdgeHashType

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
    kryo.register(classOf[Transaction])
    kryo.register(classOf[ObservationEdge])
    kryo.register(classOf[SignedObservationEdge])
    kryo.register(classOf[TypedEdgeHash])
    kryo.register(classOf[Edge[TransactionEdgeData]])
    kryo.register(classOf[SignatureBatch])
    kryo.register(classOf[HashSignature])
    kryo.register(classOf[Enumeration#Value])
    kryo.register(classOf[TransactionEdgeData])
    kryo.register(classOf[LastTransactionRef])

    kryo.register(classOf[Id])

    kryo.register(classOf[Array[Byte]])
    kryo.register(classOf[Option[Long]])
    kryo.register(classOf[String])
    kryo.register(classOf[Boolean])

//    kryo.register(Class.forName("org.constellation.wallet.EdgeHashType$EdgeHashType$"))
    kryo.register(EdgeHashType.getClass)
    kryo.register(Class.forName("scala.Enumeration$Val"))

    kryo.register(Class.forName("scala.collection.immutable.HashSet$HashSet1"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(Class.forName("scala.collection.immutable.$colon$colon"))
    kryo.register(Class.forName("scala.None$"))
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
  }
}

object KryoSerializer {

  def guessThreads: Int = {
    val cores = Runtime.getRuntime.availableProcessors
    val GUESS_THREADS_PER_CORE = 4
    GUESS_THREADS_PER_CORE * cores
  }

  val kryoPool: KryoPool = KryoPool.withBuffer(
    guessThreads,
    new ScalaKryoInstantiator()
      .setRegistrationRequired(true)
      .withRegistrar(new ConstellationKryoRegistrar()),
    32,
    -1
  )

  def serializeAnyRef(anyRef: AnyRef): Array[Byte] =
    kryoPool.toBytesWithClass(anyRef)
}

object Hashable {
  def hash(a: AnyRef): String = Hashing.sha256().hashBytes(KryoSerializer.serializeAnyRef(a)).toString
}

trait Signable {

  def signInput: Array[Byte] = hash.getBytes()

  def hash: String = Hashable.hash(getRunLengthEncoding)

  def short: String = hash.slice(0, 5)

  def getRunLengthEncoding: String = Hashable.hash(this)

}

case class LastTransactionRef(prevHash: String, ordinal: Long) extends Signable {
  override def getRunLengthEncoding = {
    val hashLengthString = prevHash.length.toString
    val ordinalLengthString = prevHash.length.toString
    hashLengthString :: prevHash :: ordinalLengthString :: ordinal.toString :: Nil mkString ""
  }
}

object LastTransactionRef {
  val empty = LastTransactionRef("", 0L)
}

case class TransactionEdgeData(//todo need to duplicate with Schema
  amount: Long,
  lastTxRef: LastTransactionRef,
  fee: Option[Long] = None,
  salt: Long = Random.nextLong()
) extends Signable  {
  override def getRunLengthEncoding = {
    val amountLengthString = amount.toString.length.toString
    val feeLengthString = fee.getOrElse(0L).toString.length
    val saltLengthString = amount.toString.length.toString
    amountLengthString :: amount.toString :: lastTxRef.getRunLengthEncoding :: feeLengthString ::
      fee.getOrElse(0L).toString :: saltLengthString :: salt.toString :: Nil mkString ""
  }
}

object EdgeHashType extends Enumeration {
  type EdgeHashType = Value

  val AddressHash, TransactionDataHash, TransactionHash = Value
}

case class TypedEdgeHash(hash: String, hashType: EdgeHashType, baseHash: Option[String] = None)

case class ObservationEdge(
  parents: Seq[TypedEdgeHash],
  data: TypedEdgeHash
) extends Signable {
  override def getRunLengthEncoding = {
    val numParentsLengthString = parents.length.toString
    val parentValuesString = parents.flatMap(parent => parent.hash.length :: parent.hash :: Nil).mkString("")
    val runEncodedTxData = data.hash //todo call this something else? Payload?
    (numParentsLengthString :: parentValuesString :: runEncodedTxData :: Nil).mkString("")
  }
}

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
      TypedEdgeHash(txData.getRunLengthEncoding, EdgeHashType.TransactionDataHash)
    )
    val soe = SignHelp.signedObservationEdge(oe)(keyPair)
    Edge(oe, soe, txData)
  }
}
