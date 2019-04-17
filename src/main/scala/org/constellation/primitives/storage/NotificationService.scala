package org.constellation.primitives.storage

import cats.effect.IO
import org.constellation.primitives.PeerNotification

class NotificationService(size: Int = 2000)  extends MerkleService[PeerNotification]{
  val merklePool = new StorageService[Seq[String]](size)
  val memPool = new StorageService[PeerNotification](size)

  def lookup: String => IO[Option[PeerNotification]] =
    DbStorage.extendedLookup[String, PeerNotification](List(memPool))

}
