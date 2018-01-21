package org.constellation.tx

import java.security.KeyPair

import org.constellation.tx.AtomicTransaction._
import org.constellation.tx.FakeNodeResponseUtils.FakePriorTransaction
import org.constellation.wallet.KeyUtils
import org.constellation.wallet.KeyUtils._
import org.scalatest.FlatSpec


object FakeNodeResponseUtils {

  // This is a mock-up for a transaction stored by the node network
  // which gets used during processing of a new transaction
  case class FakePriorTransaction(
                            address: String, // This is the 'destination address' of this transaction,
                            // or source address of next transaction
                            quantity: Long,
                            isValid: Boolean
                            )


}

class AtomicTransactionTest extends FlatSpec {

  val kp: KeyPair = makeKeyPair()

  val sampleSelfTXInput = TransactionInputData(kp.getPublic, publicKeyToAddress(kp.getPublic), 1L)

  "Transaction Input Data" should "render to json" in {
    val r = sampleSelfTXInput.encode.rendered
    assert(r.contains('['))
    assert(r.length > 50)
    assert(r.toCharArray.distinct.length > 5)
    assert(r.contains('"'))
  }

  "Transaction Encoding" should "encode and decode transactions" in {
    val enc = sampleSelfTXInput.encode
    assert(enc.decode == sampleSelfTXInput)
    val rendParse = txFromString(enc.rendered)
    assert(rendParse == enc)
    assert(rendParse.decode == sampleSelfTXInput)
  }

  val kp2: KeyPair = makeKeyPair()

  "Sample transaction" should "sign and verify a transaction using mocked history" in {

    val sampleTX = TransactionInputData(kp.getPublic, publicKeyToAddress(kp2.getPublic), 1L)

    // To run this test, first we need a way to find the source public key's address
    // And look up all transactions ( groupBy address ) find those that were verified to go there.
    // Then we need to prove that this public key is legit by signing with the private key, etc.

    // So the order of operations is as follows
    // First the person who wishes to send a transaction (the source,) needs to broadcast their public key
    // to the network so we can figure out what their address is (it's the same as broadcasting their address,
    // and we need to know their public key anyways for signing this transaction, so we just use the minimal
    // representation of sourcePubKey (which generates the address deterministically)

    val sourceAddress = sampleTX.sourceAddress

    // Below data is a mock which includes both valid and invalid transactions,
    // i.e. we expect some nodes to lie about the transaction history when the lookup occurs.
    // The 'isValid' flag is a mockup assuming that there was some previous step where this node
    // either verified the transactions leading up to this one or we trusted the network to do that
    // according to reputation / consensus / whatever mechanism.

    // The purpose here is to mock all previous logic leading up to isValid
    val fakeTransactions = Seq.fill(10)(FakePriorTransaction(sourceAddress, 5L, isValid = true)) ++
      Seq.fill(5)(FakePriorTransaction(sourceAddress, 10L, isValid = false)) ++
      Seq.fill(5)(FakePriorTransaction("anotherAddressNotUsedByThisTransactor", 10L, isValid = true))
    Seq.fill(5)(FakePriorTransaction("anotherAddressNotUsedByThisTransactor", 10L, isValid = false))

    // This is a mock for a lookup that needs to be performed against other nodes / the ledger history.
    val addressToTransactionLookup = fakeTransactions.groupBy { t => t.address }

    val previousTransactions = addressToTransactionLookup(sourceAddress).filter{_.isValid}

    val previousSourceAddressSum = previousTransactions.map {_.quantity}.sum
    val canPerformTransaction = previousSourceAddressSum > sampleTX.quantity
    // This verifies that the address has enough funds to send some to new address.
    assert(canPerformTransaction)

    // Note: we will enforce the generation of many transactions to ensure KeyPairs get fully rotated every
    // transaction at the wallet level. We should not force users to rotate keys, because there may be a
    // situation where they wish to use an alternate wallet / key mechanism, or they may just transfer the entire
    // amount at once. This is in line with the BTC whitepaper. We could consider forcing key rotations, but
    // that means shutting down 3rd party transaction generators unless they enforce that protocol, which is
    // unnecessarily strict. We would rather anyone be able to send a valid transaction. The security of revealing
    // the PubKey for more than one transaction is on the implementation provider.

    // This is to remove the quantity from the previous address
    val inverseTransaction = TransactionInputData(sampleTX.sourcePubKey, sourceAddress, -1*sampleTX.quantity)
    // Assume that this will get broadcast to the network also; so subsequent lookups will find that the quantity
    // For the address has decreased. This is where anti-double-spending must be enforced by some other mechanism

    val inverseBytes = inverseTransaction.encode.rendered.getBytes
    // This is not necessary for the test but included for completeness to demonstrate transaction steps.
    val inverseSign = KeyUtils.signData(inverseBytes)(kp.getPrivate)

    val inverseValid = KeyUtils.verifySignature(inverseBytes, inverseSign)(inverseTransaction.sourcePubKey)
    assert(inverseValid)
    // In real usage, this transaction would then update the network and the lookup to prevent double spend.
    // Here, we'll skip that step and continue verifying the actual sampleTX -- and then mock the update below

    val txBytes = sampleTX.encode.rendered.getBytes

    val txSign = KeyUtils.signData(txBytes)(kp.getPrivate)

    val txValid = KeyUtils.verifySignature(txBytes, txSign)(sampleTX.sourcePubKey)
    assert(txValid)

    val updatedFakeTransactions = fakeTransactions ++ Seq(
      FakePriorTransaction(inverseTransaction.destinationAddress, inverseTransaction.quantity, isValid = inverseValid),
      FakePriorTransaction(sampleTX.destinationAddress, sampleTX.quantity, isValid = txValid)
    )

    val updatedAddressToTransactionLookup = updatedFakeTransactions.groupBy { t => t.address }

    val updatedTXSource = updatedAddressToTransactionLookup(sourceAddress).filter{_.isValid}

    assert({updatedTXSource.map{_.quantity}.sum == (previousSourceAddressSum - sampleTX.quantity)})
    assert(
      updatedAddressToTransactionLookup(sampleTX.destinationAddress)
        .filter{_.isValid}
        .map{_.quantity}.sum == sampleTX.quantity
    )

  }


}
