package org.constellation.wallet

import cats.syntax.all._
import cats.effect.{ContextShift, IO}
import org.constellation.keytool.KeyUtils
import org.constellation.schema.edge.{Edge, EdgeHashType, ObservationEdge, SignedObservationEdge, TypedEdgeHash}
import org.constellation.schema.serialization.{Kryo, SchemaKryoRegistrar}
import org.constellation.schema.signature.SignHelp
import org.constellation.schema.transaction.{LastTransactionRef, Transaction, TransactionEdgeData}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.ExecutionContext

class TransactionDataConciseFormatTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val src = "DAG4EqbfJNSYZDDfs7AUzofotJzZXeRYgHaGZ6jQ"
  val dst = "DAG48nmxgpKhZzEzyc86y9oHotxxG57G8sBBwj56"
  val amount = 100000000L
  val prevHash = "08e6f0c3d65ed0b393604ffe282374bf501956ba447bc1c5ac49bcd2e8cc44fd"
  val ordinal = 567L
  val fee = 123456L
  val salt = 7370566588033602435L
  val lastTxRef = LastTransactionRef(prevHash, ordinal)
  val txData = TransactionEdgeData(amount = amount, lastTxRef = lastTxRef, fee = Option(fee), salt = salt)

  val oe = ObservationEdge(
    Seq(
      TypedEdgeHash(src, EdgeHashType.AddressHash),
      TypedEdgeHash(dst, EdgeHashType.AddressHash)
    ),
    TypedEdgeHash(txData.getEncoding, EdgeHashType.TransactionDataHash)
  )
  var soe: SignedObservationEdge = _
  var tx: Transaction = _

  before {
    Kryo.init[IO](SchemaKryoRegistrar).handleError(_ => Unit).unsafeRunSync()
    soe = SignHelp.signedObservationEdge(oe)(KeyUtils.makeKeyPair())
    tx = Transaction(edge = Edge(oe, soe, txData), lastTxRef = lastTxRef, isDummy = false, isTest = false)
  }

  "generate" should "correctly encode transaction data" in {
    val expected =
      "02" + //number of parents
        "28" + //length of parent 0 in bytes in hex
        "44414734457162664a4e53595a444466733741557a6f666f744a7a5a58655259674861475a366a51" + //value of parent 0 in hex
        "28" + //length of parent 1 in bytes in hex
        "44414734386e6d7867704b685a7a457a7963383679396f486f7478784735374738734242776a3536" + //value of parent 1 in hex
        "04" + //length of amount in bytes in hex
        "05f5e100" + //value of amount in hex
        "40" + //last tx ref length in bytes, in hex
        "30386536663063336436356564306233393336303466666532383233373462663530313935366261343437626331633561633439626364326538636334346664" + //last tx ref in hex
        "02" + //length of last tx ref ordinal in bytes, in hex
        "0237" + //last tx ordinal, in hex
        "03" + //length of fee in bytes, in hex
        "01e240" + //value of fee, in hex
        "08" + //length of salt in bytes, in hex
        "66498342c91b1383" //value of salt in hex
    val result = TransactionDataConciseFormat.generate(tx)

    result shouldBe expected
  }

  "generate" should "correctly encode transaction data for corner cases" in {
    val src = ""
    val dst = "DAG48nmxgpKhZzEzyc86y9oHotxxG57G8sBBwj56"
    val amount = 0L
    val prevHash = ""
    val ordinal = 0L
    val fee = None
    val salt = 0L
    val lastTxRef = LastTransactionRef(prevHash, ordinal)
    val txData = TransactionEdgeData(amount = amount, lastTxRef = lastTxRef, fee = fee, salt = salt)
    val oe = ObservationEdge(
      Seq(
        TypedEdgeHash(src, EdgeHashType.AddressHash),
        TypedEdgeHash(dst, EdgeHashType.AddressHash)
      ),
      TypedEdgeHash(txData.getEncoding, EdgeHashType.TransactionDataHash)
    )
    val soe = SignHelp.signedObservationEdge(oe)(KeyUtils.makeKeyPair())
    val tx = Transaction(edge = Edge(oe, soe, txData), lastTxRef = lastTxRef, isDummy = false, isTest = false)
    val expected =
      "02" + //number of parents
        "00" + //length of parent 0 in bytes in hex
        "" + //value of parent 0 in hex
        "28" + //length of parent 1 in bytes in hex
        "44414734386e6d7867704b685a7a457a7963383679396f486f7478784735374738734242776a3536" + //value of parent 1 in hex
        "01" + //length of amount in bytes in hex
        "00" + //value of amount in hex
        "00" + //last tx ref length in bytes, in hex
        "" + //last tx ref in hex
        "01" + //length of last tx ref ordinal in bytes, in hex
        "00" + //last tx ordinal, in hex
        "01" + //length of fee in bytes, in hex
        "00" + //value of fee, in hex
        "01" + //length of salt in bytes, in hex
        "00" //value of salt in hex
    val result = TransactionDataConciseFormat.generate(tx)

    result shouldBe expected
  }
}
