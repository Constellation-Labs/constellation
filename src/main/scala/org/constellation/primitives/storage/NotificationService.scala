package org.constellation.primitives.storage
import cats.effect.IO
import org.constellation.primitives.PeerNotification

class NotificationService(size: Int = 2000)  extends MerkleService[String, PeerNotification]{
  val merklePool = new StorageService[Seq[String]]()
  val memPool = new StorageService[PeerNotification]()

  def lookup: String => IO[Option[PeerNotification]] =
    DbStorage.extendedLookup[String, PeerNotification](List(memPool))
  override def findHashesByMerkleRoot(
    merkleRoot: String
  ): IO[Option[Seq[String]]] = merklePool.get(merkleRoot)
}
