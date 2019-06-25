package org.constellation.storage
import cats.effect.{IO, Sync}
import org.constellation.primitives.PeerNotification
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}

class NotificationService[F[_]: Sync]() extends MerkleStorageAlgebra[F, String, PeerNotification] {
  val merklePool = new StorageService[F, Seq[String]]()
  val memPool = new StorageService[F, PeerNotification]()

  def lookup(key: String): F[Option[PeerNotification]] =
    Lookup.extendedLookup[F, String, PeerNotification](List(memPool))(key)

  def contains(key: String): F[Boolean] =
    Lookup.extendedContains[F, String, PeerNotification](List(memPool))(key)

  def findHashesByMerkleRoot(merkleRoot: String): F[Option[Seq[String]]] =
    merklePool.lookup(merkleRoot)
}
