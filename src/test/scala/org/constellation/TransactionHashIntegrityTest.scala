package org.constellation

import cats.effect.IO
import cats.implicits._
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.{Edge, Transaction}
import org.constellation.serializer.KryoSerializer
import org.constellation.wallet.{KryoSerializer => WalletKryoSerializer}
import org.constellation.wallet.{Hashable, Transaction => WalletTransaction}
import org.scalatest.{FreeSpec, Matchers}
import constellation._
import org.constellation.domain.snapshot.SnapshotInfo
import org.constellation.infrastructure.p2p.ClientInterpreter
import org.constellation.infrastructure.p2p.PeerResponse.PeerClientMetadata
import org.constellation.schema.Id
import org.http4s.Uri
import org.http4s.implicits._
import org.http4s.Uri.{Authority, RegName, Scheme}
import org.http4s.client.blaze.BlazeClientBuilder
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.parser.parse

class TransactionHashIntegrityTest extends FreeSpec with Matchers {

  "check" in {
    val url = "http://54.215.210.171:9000/registration/request"
    val uri = Uri.unsafeFromString(url)

    implicit val cs = IO.contextShift(ConstellationExecutionContext.unbounded)

    val prr = BlazeClientBuilder[IO](ConstellationExecutionContext.unbounded).resource.use { client =>
      val api = ClientInterpreter[IO](client)
      val pm = PeerClientMetadata("54.215.210.171", 9001, Id("foo"))
      api.snapshot.getLatestMajorityHeight().run(pm)
    }.unsafeRunSync()

    println(prr)

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
