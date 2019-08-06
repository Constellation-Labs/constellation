package org.constellation.storage

import cats.effect.Concurrent
import cats.implicits._
import org.constellation.DAO
import org.constellation.primitives.{CheckpointBlock, CheckpointBlockMetadata}
import org.constellation.primitives.Schema.{CheckpointCache, CheckpointCacheMetadata}
import org.constellation.util.MerkleTree

class CheckpointBlocksMemPool[F[_]: Concurrent](
  dao: DAO,
  transactionsMerklePool: StorageService[F, Seq[String]],
  messagesMerklePool: StorageService[F, Seq[String]],
  notificationsMerklePool: StorageService[F, Seq[String]],
  experiencesMerklePool: StorageService[F, Seq[String]]
) extends StorageService[F, CheckpointCacheMetadata]() {

  def put(
    key: String,
    value: CheckpointCache
  ): F[CheckpointCacheMetadata] =
    value.checkpointBlock
      .map(cb => incrementChildrenCount(cb.parentSOEBaseHashes()(dao)))
      .sequence *>
      storeMerkleRoots(value.checkpointBlock.get)
        .flatMap(ccm => {
          super.put(key, CheckpointCacheMetadata(ccm, value.children, value.height))
        })

  private def storeMerkleRoots(data: CheckpointBlock): F[CheckpointBlockMetadata] =
    for {
      t <- store(data.transactions.map(_.hash), transactionsMerklePool)
      m <- store(data.messages.map(_.signedMessageData.hash), messagesMerklePool)
      n <- store(data.notifications.map(_.hash), notificationsMerklePool)
      e <- store(data.experiences.map(_.hash), experiencesMerklePool)
    } yield CheckpointBlockMetadata(t, data.checkpoint, m, n, e)

  private def store(data: Seq[String], ss: StorageService[F, Seq[String]]): F[Option[String]] =
    data match {
      case Seq() => none[String].pure[F]
      case _ =>
        val rootHash = MerkleTree(data).rootHash
        ss.put(rootHash, data).map(_ => rootHash.some)
    }

  def incrementChildrenCount(hashes: Seq[String]): F[Unit] =
    hashes.toList.traverse { hash =>
      update(hash, (cd: CheckpointCacheMetadata) => cd.copy(children = cd.children + 1))
    }.void
}
