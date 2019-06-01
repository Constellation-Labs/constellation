package org.constellation.storage
import cats.effect.IO
import org.constellation.primitives.PeerNotification
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}

class NotificationService(size: Int = 2000)  extends MerkleStorageAlgebra[IO, String, PeerNotification]{
  val merklePool = new StorageService[IO, Seq[String]]()
  val memPool = new StorageService[IO, PeerNotification]()

  def lookup(key: String): IO[Option[PeerNotification]] =
    Lookup.extendedLookup[IO, String, PeerNotification](List(memPool))(key)

  def contains(key: String): IO[Boolean] =
    Lookup.extendedContains[IO, String, PeerNotification](List(memPool))(key)

  override def findHashesByMerkleRoot(
    merkleRoot: String
  ): IO[Option[Seq[String]]] = merklePool.lookup(merkleRoot)
}
