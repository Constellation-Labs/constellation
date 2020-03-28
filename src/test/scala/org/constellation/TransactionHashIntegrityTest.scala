package org.constellation

import cats.effect.IO
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.{Edge, Transaction}
import org.constellation.serializer.KryoSerializer
import org.constellation.wallet.{KryoSerializer => WalletKryoSerializer}
import org.constellation.wallet.{Hashable, Transaction => WalletTransaction}
import org.scalatest.{FreeSpec, Matchers}
import constellation._

class TransactionHashIntegrityTest extends FreeSpec with Matchers {

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
        KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](_.x[Transaction])
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
