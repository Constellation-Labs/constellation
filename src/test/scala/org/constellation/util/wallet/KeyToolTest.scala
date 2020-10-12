package org.constellation.util.wallet

import java.security.{KeyPair, PrivateKey, PublicKey}

import better.files.File
import cats.effect.IO
import org.constellation.Fixtures
import org.constellation.Fixtures._
import org.constellation.keytool.{KeyStoreUtils, KeyTool, KeyUtils}
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class KeyToolTest extends AsyncFlatSpecLike with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  val keyTool = KeyTool
  val savedKeystorePath = getClass.getResource("/wallet-client-test-save-kp.p12").getPath

  val keyToolArgs = List(
    s"--keystore=${savedKeystorePath}",
    s"--alias=${Fixtures.alias}",
    s"--storepass=${Fixtures.storepass}",
    s"--keypass=${Fixtures.keypass}"
  )

  override protected def beforeEach(): Unit = {
    File(savedKeystorePath).delete(true)
    super.beforeEach()
  }

  "KeyStoreUtils" should "load keypair successfully" ignore {
    val loadKp =
      for {
        lkp <- KeyStoreUtils
          .keyPairFromStorePath[IO](
            savedKeystorePath,
            Fixtures.alias,
            Fixtures.storepass.toCharArray,
            Fixtures.keypass.toCharArray
          )
      } yield lkp
    val kpEither = loadKp.value.unsafeRunSync()
    assert(kpEither.isRight)
    val kp = kpEither.right.get
    assert(kp.getPublic.isInstanceOf[PublicKey] && kp.getPrivate.isInstanceOf[PrivateKey])
  }

  "KeyTool" should "create new keypair and save to disk" in {
    val genKeyLoop = for {
      kp <- keyTool.run(keyToolArgs)
    } yield kp
    val genKeyLoopF = genKeyLoop.unsafeToFuture()
    genKeyLoopF.map(_ => assert(File(savedKeystorePath).nonEmpty))
  }

  "KeyTool" should "create new keypair and save to disk with env args" in {
    val args = List(
      s"--keystore=${savedKeystorePath}",
      s"--alias=${Fixtures.alias}",
      s"--storepass=${Fixtures.storepass}",
      s"--keypass=${Fixtures.keypass}",
      s"--env_args=${Fixtures.envArgs}"
    )
    val genKeyLoop = for {
      kp <- keyTool.run(args)
    } yield kp
    val genKeyLoopF = genKeyLoop.unsafeToFuture()
    genKeyLoopF.map(_ => assert(File(savedKeystorePath).nonEmpty))
  }

  "keyPairFromPemStr" should "generate keypair from PEM strings" in {
    assert(KeyUtils.keyPairFromPemStr(Fixtures.privateKeyStr, Fixtures.pubKeyStr).isInstanceOf[KeyPair])
  }

  "KeyTool" should "convert private key to hex and back from hex" in {
    val keypair = KeyUtils.makeKeyPair()
    val privateKey = keypair.getPrivate
    val hex = KeyUtils.privateKeyToFullHex(privateKey)
    val fromHex: PrivateKey = KeyUtils.hexToPrivateKey(hex)
    assert(fromHex.equals(privateKey))
  }
}
