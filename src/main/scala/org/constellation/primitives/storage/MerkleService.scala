package org.constellation.primitives.storage
import cats.effect.IO

trait MerkleService[T] {
  def lookup: String => IO[Option[T]]
  def findHashesByMerkleRoot(merkleRoot: String): IO[Option[Seq[String]]]
}