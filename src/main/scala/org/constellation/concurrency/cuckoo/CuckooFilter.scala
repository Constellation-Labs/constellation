package org.constellation.concurrency.cuckoo

import java.io.ByteArrayInputStream

import net.cinnom.nanocuckoo.NanoCuckooFilter
import org.constellation.schema.snapshot.FilterData

case class CuckooFilter(filterData: FilterData) {

  private lazy val filter: NanoCuckooFilter = {
    val stream = new ByteArrayInputStream(filterData.contents)
    try {
      val resultingFilter = new NanoCuckooFilter.Builder(8192).build()
      while (resultingFilter.getCapacity < filterData.capacity) resultingFilter.expand()
      resultingFilter.readMemory(stream)
      resultingFilter
    } finally stream.close()
  }

  def contains[T](item: T)(implicit convert: T => String): Boolean = convert.andThen(filter.contains)(item)

}
