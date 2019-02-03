package org.constellation.primitives

import java.security.KeyPair

import org.constellation.DAO
import org.constellation.primitives.Schema.{Address, AddressCacheData, TransactionCacheData, TransactionEdgeData}
import org.constellation.util.HashSignature
import constellation._

/** Documentation. */
case class Transaction(edge: Edge[TransactionEdgeData]) {

  /** Documentation. */
  def store(cache: TransactionCacheData)(implicit dao: DAO): Unit = {
    dao.transactionService.put(this.hash, cache)
  }

  /** Documentation. */
  def ledgerApply()(implicit dao: DAO): Unit = {
    dao.addressService.update(
      src.hash,
      { a: AddressCacheData => a.copy(balance = a.balance - amount)},
      AddressCacheData(0L, 0L) // unused since this address should already exist here
    )
    dao.addressService.update(
      dst.hash,
      { a: AddressCacheData => a.copy(balance = a.balance + amount)},
      AddressCacheData(amount, 0L) // unused since this address should already exist here
    )
  }

  /** Documentation. */
  def ledgerApplySnapshot()(implicit dao: DAO): Unit = {
    dao.addressService.update(
      src.hash,
      { a: AddressCacheData => a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot - amount)},
      AddressCacheData(0L, 0L) // unused since this address should already exist here
    )
    dao.addressService.update(
      dst.hash,
      { a: AddressCacheData => a.copy(balanceByLatestSnapshot = a.balanceByLatestSnapshot + amount)},
      AddressCacheData(amount, 0L) // unused since this address should already exist here
    )
  }

  // Unsafe

  /** Documentation. */
  def src: Address = Address(edge.parents.head.hash)

  /** Documentation. */
  def dst: Address = Address(edge.parents.last.hash)

  /** Documentation. */
  def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

  // TODO: Add proper exception on empty option

  /** Documentation. */
  def amount : Long = edge.data.amount

  /** Documentation. */
  def baseHash: String = edge.signedObservationEdge.baseHash

  /** Documentation. */
  def hash: String = edge.signedObservationEdge.hash

  /** Documentation. */
  def withSignatureFrom(keyPair: KeyPair): Transaction = this.copy(
    edge = edge.withSignatureFrom(keyPair)
  )

  /** Documentation. */
  def valid: Boolean = validSrcSignature &&
    dst.address.nonEmpty &&
    dst.address.length > 30 &&
    dst.address.startsWith("DAG") &&
    amount > 0

  /** Documentation. */
  def validSrcSignature: Boolean = {
    edge.signedObservationEdge.signatureBatch.signatures.exists{ hs =>
      hs.publicKey.address == src.address && hs.valid(edge.signedObservationEdge.signatureBatch.hash)
    }
  }

}

