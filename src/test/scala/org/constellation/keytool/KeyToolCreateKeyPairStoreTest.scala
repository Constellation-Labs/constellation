package org.constellation.keytool

import java.security.KeyPair

import better.files.File
import cats.effect.{ContextShift, IO}
import org.constellation.ConstellationExecutionContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class KeyToolCreateKeyPairStoreTest extends AnyFunSuite with BeforeAndAfterAll with Matchers {

  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ConstellationExecutionContext.unbounded)

  private val keyPath: String = "src/test/resources/testKey.p12"
  private val alias: String = "alias"
  private val storePassword: String = "storePassword"
  private val keyPassword: String = "keyPassword"

  override def afterAll: Unit =
    File(keyPath).delete()

  test("key pair should be the same after reading from the key store") {
    val keyPair: KeyPair = KeyUtils.makeKeyPair()

    KeyStoreUtils
      .putKeyPairToStorePath[IO](keyPair, keyPath, alias, storePassword.toCharArray, keyPassword.toCharArray)
      .value
      .unsafeRunSync()
      .toTry
      .get

    val keyPairFromStore: KeyPair = KeyStoreUtils
      .keyPairFromStorePath[IO](keyPath, alias, storePassword.toCharArray, keyPassword.toCharArray)
      .value
      .unsafeRunSync()
      .toTry
      .get

    keyPair.getPublic.getEncoded shouldBe keyPairFromStore.getPublic.getEncoded
    keyPair.getPrivate.getEncoded shouldBe keyPairFromStore.getPrivate.getEncoded
  }
}
