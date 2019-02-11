package org.constellation.primitives

import java.security.KeyPair

import constellation._
import org.constellation.DAO
import org.constellation.primitives.Schema.{
  Address,
  AddressCacheData,
  TransactionCacheData,
  TransactionEdgeData
}
import org.constellation.util.HashSignature

case class Transaction(edge: Edge[TransactionEdgeData]) {

  def store(cache: TransactionCacheData)(implicit dao: DAO): Unit = {
    dao.transactionService.put(this.hash, cache)
  }

  def ledgerApply()(implicit dao: DAO): Unit = {
    dao.addressService.transfer(src, dst, amount).unsafeRunSync()
  }

  def ledgerApplySnapshot()(implicit dao: DAO): Unit = {
    dao.addressService.transferSnapshot(src, dst, amount).unsafeRunSync()
  }

  // Unsafe

  def src: Address = Address(edge.parents.head.hash)

  def dst: Address = Address(edge.parents.last.hash)

  def signatures: Seq[HashSignature] = edge.signedObservationEdge.signatureBatch.signatures

  // TODO: Add proper exception on empty option

  def amount: Long = edge.data.amount

  def baseHash: String = edge.signedObservationEdge.baseHash

  def hash: String = edge.signedObservationEdge.hash

  def withSignatureFrom(keyPair: KeyPair): Transaction = this.copy(
    edge = edge.withSignatureFrom(keyPair)
  )

  def valid: Boolean =
    validSrcSignature &&
      dst.address.nonEmpty &&
      dst.address.length > 30 &&
      dst.address.startsWith("DAG") &&
      amount > 0

  def validSrcSignature: Boolean = {
    edge.signedObservationEdge.signatureBatch.signatures.exists { hs =>
      hs.publicKey.address == src.address && hs.valid(
        edge.signedObservationEdge.signatureBatch.hash
      )
    }
  }

}
