package org.constellation.storage

import cats.effect.IO
import cats.implicits._
import org.mockito.IdiomaticMockito
import org.scalatest.{FunSuite, Matchers}

class StorageServiceTest extends FunSuite with IdiomaticMockito with Matchers {

  test("it should dequeue old values when maxQueueSize would be reached") {
    val storage = new StorageService[IO, Int]()

    (1 to 30).toList.traverse(i => storage.put(i.toString, i)).unsafeRunSync

    val start = storage.getLast20().unsafeRunSync()
    start shouldBe (11 to 30).reverse.toList
  }
}
