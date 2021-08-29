package org.constellation.wallet

import org.bitcoinj.crypto.{ChildNumber, HDUtils}
import org.bitcoinj.wallet.{DeterministicKeyChain, DeterministicSeed}
import org.constellation.keytool.KeyUtils
import org.constellation.wallet.MnemonicGenerator.{dhash, dhashRounds, stringUTF8ToMnemonic}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.Charset
import java.util

class MnemonicGeneratorTest extends AnyFreeSpec with Matchers {

  val testString = "asdf";
  val bytesUTF8: Array[Byte] = "asdf".getBytes(Charset.forName("UTF-8"));

  "UTF-8 string rendering" in {
    KeyUtils.bytes2hex(bytesUTF8) shouldBe "61736466"
  }

  "Double hashing function" in {
    val bytes = dhash(bytesUTF8)
    KeyUtils.bytes2hex(bytes) shouldBe "fe9a32f5b565da46af951e4aab23c24b8c1565eb0b6603a03118b7d225a21e8c"
  }

  "Double hashing function at 10k rounds" in {
    val bytes = dhashRounds(bytesUTF8)
    KeyUtils.bytes2hex(bytes) shouldBe "2a627a4bddc2ef8ad771eaa4f1969a479f167ad7a45fbb269de57d02117d083b"
  }

  "Mnemonic generation all entropy" in {
    stringUTF8ToMnemonic(testString) shouldBe "clerk because napkin romance confirm shell fruit diary pilot million pledge monkey vapor dice future message robot crunch junk wheel canal salon can gas"
      .split(" ").toList
  }

  // NOTE: There is no checksum check here! If you don't use sha256 or exactly u8;32 / 256 bit byte array
  // then this will not work in other languages!
  "Private key gen" in {
    val seedPhrase = stringUTF8ToMnemonic(testString).slice(0, 24).mkString(" ");
    println(seedPhrase)
    val seed = new DeterministicSeed(seedPhrase, null, "", 0L)
    val chain: DeterministicKeyChain = DeterministicKeyChain.builder.seed(seed).build
    val chainPath: String = "M/44H/0H/0H/0/0"
    val keyPath: util.List[ChildNumber] = HDUtils.parsePath(chainPath)
    val key = chain.getKeyByPath(keyPath, true)
    val kp = BIP44.getECKeyPairFromBip44Key(key)
    val hex = KeyUtils.privateKeyToFullHex(kp.getPrivate)
    // This hex was checked against code in other languages for same input.
    assert(hex.startsWith("4f24eed68ea40d165049e80717ab82ea507bb15b478e93233e809601d4cd6bec"))
  }
}
