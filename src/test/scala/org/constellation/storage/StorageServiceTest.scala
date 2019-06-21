package org.constellation.storage

import cats.effect.IO
import cats.implicits._
import org.mockito.IdiomaticMockito
import org.scalatest.{FunSuite, Matchers}

class StorageServiceTest extends FunSuite with IdiomaticMockito with Matchers {

  test("it should allow to put a new value") {
    val storage = new StorageService[IO, Int]()

    storage.put("lorem", 1).unsafeRunSync

    storage.lookup("lorem").unsafeRunSync shouldBe Some(1)
  }

  ignore("it should put a value to the queue during addition") {}

  test("it should allow to remove a value") {
    val storage = new StorageService[IO, Int]()

    storage.put("lorem", 2).unsafeRunSync

    storage.lookup("lorem").unsafeRunSync shouldBe Some(2)

    storage.remove("lorem").unsafeRunSync

    storage.lookup("lorem").unsafeRunSync shouldBe None
  }

  ignore("it should remove a value from the queue during removal") {}

  test("it should allow to update a value") {
    val storage = new StorageService[IO, Int]()

    storage.put("lorem", 1).unsafeRunSync

    storage.lookup("lorem").unsafeRunSync shouldBe Some(1)

    storage.update("lorem", _ => 2).unsafeRunSync shouldBe Some(2)

    storage.lookup("lorem").unsafeRunSync shouldBe Some(2)
  }

  ignore("it should update a value in the queue during update") {}

  test("it should put a new value in the cache during update if the value doesn't exist") {
    val storage = new StorageService[IO, Int]()

    storage.lookup("lorem").unsafeRunSync shouldBe None

    storage.update("lorem", _ => 2, 3).unsafeRunSync shouldBe 3

    storage.lookup("lorem").unsafeRunSync shouldBe Some(3)
  }

  test("it should allow to lookup") {
    val storage = new StorageService[IO, Int]()

    storage.put("lorem", 2).unsafeRunSync

    storage.lookup("lorem").unsafeRunSync shouldBe Some(2)
  }

  test("it should return None if value doesn't exist") {
    val storage = new StorageService[IO, Int]()

    storage.lookup("lorem").unsafeRunSync shouldBe None
  }

  test("it should allow to return the size") {
    val storage = new StorageService[IO, Int]()

    storage.size.unsafeRunSync shouldBe 0

    storage.put("lorem", 2).unsafeRunSync

    storage.size.unsafeRunSync shouldBe 1
  }

  test("it should allow to return all values as Map") {
    val storage = new StorageService[IO, Int]()

    storage.put("lorem", 2).unsafeRunSync

    storage.toMap().unsafeRunSync shouldBe Map("lorem" -> 2)
  }

  test("it should allow to return last 20 values") {
    val storage = new StorageService[IO, Int]()

    (1 to 30).toList.traverse(i => storage.put(i.toString, i)).unsafeRunSync

    val start = storage.getLast20().unsafeRunSync()
    start shouldBe (11 to 30).reverse.toList
  }
}
