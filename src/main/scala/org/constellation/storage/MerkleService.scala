package org.constellation.storage
import cats.effect.IO

trait MerkleService[H, T] {
  def lookup: H => IO[Option[T]]
  def findHashesByMerkleRoot(merkleRoot: String): IO[Option[Seq[H]]]
}