package org.constellation.storage.algebra

trait MerkleStorageAlgebra[F[_], K, V] extends LookupAlgebra[F, K, V] {
  def findHashesByMerkleRoot(merkleRoot: String): F[Option[Seq[K]]] // Set?
}
