package org.constellation.storage
import cats.syntax.all._
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.constellation.p2p.PeerNotification
import org.constellation.storage.algebra.{Lookup, MerkleStorageAlgebra}

class NotificationService[F[_]: Concurrent]() extends MerkleStorageAlgebra[F, String, PeerNotification] {

  val merklePool =
    new ConcurrentStorageService[F, Seq[String]](
      ConstellationExecutionContext.createSemaphore(),
      "notification_merkle_pool".some
    )

  val memPool =
    new ConcurrentStorageService[F, PeerNotification](
      ConstellationExecutionContext.createSemaphore(),
      "notification_mem_pool".some
    )

  def lookup(key: String): F[Option[PeerNotification]] =
    Lookup.extendedLookup[F, String, PeerNotification](List(memPool))(key)

  def contains(key: String): F[Boolean] =
    Lookup.extendedContains[F, String, PeerNotification](List(memPool))(key)

  def findHashesByMerkleRoot(merkleRoot: String): F[Option[Seq[String]]] =
    merklePool.lookup(merkleRoot)

  override def addMerkle(merkleRoot: String, keys: Seq[String]): F[Seq[String]] = merklePool.put(merkleRoot, keys)
}
