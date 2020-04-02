package org.constellation

import cats.effect.{ContextShift, IO}
import cats.implicits._
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.Transaction
import org.constellation.serializer.KryoSerializer
import org.constellation.wallet.{KryoSerializer => WalletKryoSerializer}
import org.constellation.wallet.{Hashable, Transaction => WalletTransaction}
import org.scalatest.{FreeSpec, Matchers}
import io.circe.generic.auto._
import io.circe.parser.parse
import org.constellation.schema.Id
import org.http4s.{Header, Headers, Request, Uri}
import org.http4s.client.dsl.io._
import org.http4s.client.dsl._
import org.http4s.Method._
import pl.abankowski.httpsigner.http4s.Http4sRequestVerifier
import pl.abankowski.httpsigner.signature.generic.GenericVerifier

class TransactionHashIntegrityTest extends FreeSpec with Matchers {
  implicit val cc: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)

  "check" in {
    val id = Id(
      "b6fb191d9d053de138fec00ac1edf7e615769f6b687a831dafb653f9b1e6a0d4637cfe4380ff78d2763e8072a7d75021193de78d4325d7a701e8db6352edd39f"
    )
    val req = Request[IO](
      method = GET,
      uri = Uri.unsafeFromString("http://54.215.210.171:9001/registration/request"),
      headers = Headers.of(
        Header("Host", "54.215.210.171:9001"),
        Header(
          "Request-Signature",
          "MEUCIGT2oKxufSbAd88UpqmEeHS1Fj0VVDl90pqmVRxAsMWuAiEA6VWn5ScU5oduFHw4N0mcyavs2+fdMm/azb8UwbXOPrk="
        ),
        Header("Accept", "application/json"),
        Header("User-Agent", "http4s-blaze/0.21.2"),
        Header("Content-Length", "0")
      )
    )
    val c = GenericVerifier(KeyUtils.DefaultSignFunc, KeyUtils.provider, id.toPublicKey)
    val h = new Http4sRequestVerifier[IO](c)
    println(h.verify(req).unsafeRunSync())
  }

  "transaction read by wallet and node should have consistent hash" in {
    val txPath = "src/test/resources/valid-tx.txt"
    val readTx =
      KeyStoreUtils
        .readFromFileStream[IO, Option[WalletTransaction]](txPath, WalletTransaction.transactionParser[IO])
        .value
        .unsafeRunSync()
        .right
        .get
        .get

    val readTx2 = KeyStoreUtils
      .readFromFileStream[IO, Option[Transaction]](
        txPath,
        KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](parse(_).map(_.as[Transaction]).toOption.flatMap(_.toOption))
      )
      .value
      .unsafeRunSync()
      .right
      .get
      .get

    val walletHash1 = Hashable.hash(readTx.edge.signedObservationEdge)
    val nodeHash1 = readTx2.hash

    val serializedWalletTx = WalletKryoSerializer.serializeAnyRef(readTx)
    val serializedNodeTx = KryoSerializer.serializeAnyRef(readTx2)

    val deserializedWalletTx = WalletKryoSerializer.deserializeCast[WalletTransaction](serializedWalletTx)
    val deserializedNodeTx = KryoSerializer.deserializeCast[Transaction](serializedNodeTx)

    val walletHash2 = Hashable.hash(deserializedWalletTx.edge.signedObservationEdge)
    val nodeHash2 = deserializedNodeTx.hash

    walletHash1 shouldBe nodeHash1
    walletHash2 shouldBe nodeHash2

    walletHash1 shouldBe walletHash2
    nodeHash1 shouldBe nodeHash2
  }
}
