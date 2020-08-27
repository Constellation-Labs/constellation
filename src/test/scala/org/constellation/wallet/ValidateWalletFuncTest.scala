package org.constellation.wallet

import java.security.{KeyPair, PrivateKey, PublicKey}

import cats.effect.IO
import org.scalatest.{FlatSpec, Matchers}
import constellation._
import org.constellation.Fixtures
import org.constellation.keytool.KeyStoreUtils
import org.constellation.keytool.KeyUtils._
import org.constellation.wallet.Transaction.pickHashForLastTransactionRef
import org.constellation.wallet.{Transaction => WalletTransaction}

case class SetSerialize(s: Set[String])

class ValidateWalletFuncTest extends FlatSpec with Matchers {

  val kp: KeyPair = makeKeyPair()

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
  val soe = SignHelp.signedObservationEdge(oe)(Fixtures.tempKey)
  val tx = Transaction(edge = Edge(oe, soe, txData), lastTxRef = lastTxRef, isDummy = false, isTest = false)

  /*  "Wallet KeyStore" should "build a keystore properly" in {

    val file = new File("keystoretest.p12")
    val file2 = new File("keystoretest.bks")
    val pass = "fakepassword".toCharArray
    val (p12A, bksA) = makeWalletKeyStore(
      saveCertTo = Some(file),
      savePairsTo = Some(file2),
      password = pass,
      numECDSAKeys = 10
    )

    val p12 = KeyStore.getInstance("PKCS12", "BC")
    p12.load(new FileInputStream(file), pass)

    val bks = KeyStore.getInstance("BKS", "BC")
    bks.load(new FileInputStream(file2), pass)

    assert(p12A.getCertificate("test_cert") == p12.getCertificate("test_cert"))

    file.delete()
    file2.delete()

    // Put more tests in here.

  }*/

  "getEncoding" should "correctly encode transaction data" in {
    val expectedEncoded =
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
    val result = tx.edge.observationEdge.getEncoding

    result shouldBe expectedEncoded
  }

  "getEncoding" should "correctly encode transaction data for corner cases" in {
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
    val soe = SignHelp.signedObservationEdge(oe)(Fixtures.tempKey)
    val tx = Transaction(edge = Edge(oe, soe, txData), lastTxRef = lastTxRef, isDummy = false, isTest = false)
    val expectedEncoded =
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
    val result = tx.edge.observationEdge.getEncoding

    result shouldBe expectedEncoded
  }

  "KeyGen" should "make proper keys" in {
    val privK = kp.getPrivate.toString
    val pubK = kp.getPublic.toString
    Seq(privK, pubK).foreach { pk =>
      assert(pk.length > 50)
      assert(pk.contains(":"))
      assert(pk.contains("X:"))
      assert(pk.split("\n").length > 2)
    }
  }

  "Signature" should "sign and verify output" in {

    val text = "Yo this some text"
    val inputBytes = text.getBytes()

    val signedOutput = signData(inputBytes)(kp.getPrivate)
    val isLegit = verifySignature(inputBytes, signedOutput)(kp.getPublic)

    assert(isLegit)

  }

  "Key Size" should "verify byte array lengths for encoded keys" in {

    def fill(thunk: => Array[Byte]) =
      Seq.fill(50) { thunk }.map { _.length }.distinct

    assert(fill(makeKeyPair().getPrivate.getEncoded) == List(144))
    assert(fill(makeKeyPair().getPublic.getEncoded) == List(88))

  }

  "Address maker" should "create valid address deterministically" in {
    val addr = publicKeyToAddressString(kp.getPublic)
    assert(addr.length > 10)
    assert(addr.toCharArray.distinct.length > 5)
    val addr2 = publicKeyToAddressString(kp.getPublic)
    assert(addr == addr2)
  }

  "Key Encoding" should "verify keys can be encoded and decoded with X509/PKCS8 spec" in {
    val pub1 = kp.getPublic
    val priv1 = kp.getPrivate

    val encodedBytesPub = pub1.getEncoded
    val pub2 = bytesToPublicKey(encodedBytesPub)
    assert(pub1 == pub2)
    assert(pub1.getEncoded.sameElements(pub2.getEncoded))

    val encodedBytesPriv = priv1.getEncoded
    val priv2 = bytesToPrivateKey(encodedBytesPriv)
    assert(priv1 == priv2)
    assert(priv1.getEncoded.sameElements(priv2.getEncoded))
  }

  "pickHashForLastTransactionRef" should "pick historical hash for previous transaction if it was generated with old encoding" in {
    val txPath = "src/test/resources/valid-historical-tx.txt"
    val readTx =
      KeyStoreUtils
        .readFromFileStream[IO, Option[WalletTransaction]](txPath, WalletTransaction.transactionParser[IO])
        .value
        .unsafeRunSync()
        .right
        .get
        .get

    val expected = LastTransactionRef(
      "b8fb3a08a8dd4a0642e391ff7b39d4d9ac04957fe683286c6b41bd3ac92f28fa",
      1
    )

    pickHashForLastTransactionRef(readTx) shouldBe expected
  }

  "pickHashForLastTransactionRef" should "pick new hash for previous transaction if it was generated with new encoding" in {
    val txPath = "src/test/resources/valid-tx.txt"
    val readTx =
      KeyStoreUtils
        .readFromFileStream[IO, Option[WalletTransaction]](txPath, WalletTransaction.transactionParser[IO])
        .value
        .unsafeRunSync()
        .right
        .get
        .get

    val expected = LastTransactionRef(
      "44ea30a6b57e888ca739fb1cc1e76ea5a03d5510a626b50b893a61fdf104ecbf",
      2
    )

    pickHashForLastTransactionRef(readTx) shouldBe expected
  }

  "historicalDataRunLengthEncoding" should "generate correct historical runLengthEncoding for data field" in {
    val txPath = "src/test/resources/valid-historical-tx.txt"
    val readTx =
      KeyStoreUtils
        .readFromFileStream[IO, Option[WalletTransaction]](txPath, WalletTransaction.transactionParser[IO])
        .value
        .unsafeRunSync()
        .right
        .get
        .get

    readTx.historicalDataRunLengthEncoding shouldBe "10010101696c815a42ff2bd02"
  }

  "historicalHash" should "generate correct historical hash for a transaction" in {
    val txPath = "src/test/resources/valid-historical-tx.txt"
    val readTx =
      KeyStoreUtils
        .readFromFileStream[IO, Option[WalletTransaction]](txPath, WalletTransaction.transactionParser[IO])
        .value
        .unsafeRunSync()
        .right
        .get
        .get

    readTx.historicalHash shouldBe "b8fb3a08a8dd4a0642e391ff7b39d4d9ac04957fe683286c6b41bd3ac92f28fa"
  }

  "hash" should "generate correct hash for a transaction" in {
    val expected = "8987e92a61b7e38e82361ccaa62772801654afa20065c2a5a6d873fe23cccc49"
    val result = tx.edge.observationEdge.hash

    result shouldBe expected
  }

}
