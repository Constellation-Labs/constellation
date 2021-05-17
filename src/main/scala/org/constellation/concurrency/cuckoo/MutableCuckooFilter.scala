package org.constellation.concurrency.cuckoo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import cats.effect.Sync
import net.cinnom.nanocuckoo.NanoCuckooFilter
import org.constellation.schema.snapshot.FilterData
import cats.syntax.all._

class MutableCuckooFilter[F[_], T](filter: NanoCuckooFilter)(implicit F: Sync[F]) {

  def insert(item: T)(implicit convert: T => String): F[Boolean] = {
    val converted = convert(item)
    for {
      insertResult <- tryInsert(converted)
      finalResult <- if (insertResult) F.pure(true)
      else expand >> tryInsert(converted)

    } yield finalResult
  }

  private def tryInsert(convertedItem: String): F[Boolean] = F.delay {
    filter.insert(convertedItem)
  }

  private def expand: F[Unit] = F.delay {
    filter.expand()
  }

  def delete(item: T)(implicit convert: T => String): F[Boolean] = F.delay {
    filter.delete(convert(item))
  }

  def contains(item: T)(implicit convert: T => String): F[Boolean] = F.delay {
    filter.contains(convert(item))
  }

  def getFilterData: F[FilterData] =
    for {
      capacity <- getCapacity
      contents <- getFilterContents
    } yield FilterData(contents, capacity)

  def clear: F[Unit] =
    for {
      capacity <- getCapacity
      contents <- MutableCuckooFilter(capacity).getFilterContents
      _ <- setFilterContents(contents)
    } yield ()

  private def getCapacity: F[Long] = F.delay {
    filter.getCapacity
  }

  private def getFilterContents: F[Array[Byte]] = F.delay {
    val stream = new ByteArrayOutputStream()
    try filter.writeMemory(stream)
    finally stream.close()
    stream.toByteArray
  }

  private def setFilterContents(contents: Array[Byte]): F[Unit] = F.delay {
    val stream = new ByteArrayInputStream(contents)
    try filter.readMemory(stream)
    finally stream.close()
  }

}

object MutableCuckooFilter {

  def apply[F[_]: Sync, T](capacity: Long = 512): MutableCuckooFilter[F, T] =
    new MutableCuckooFilter[F, T](new NanoCuckooFilter.Builder(capacity).build())

}
