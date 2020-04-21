package org.constellation.wallet

import java.math.BigInteger
import java.security.{KeyFactory, KeyPair, PrivateKey, PublicKey}
import java.util
import java.util.Date

import org.bitcoinj.crypto.{ChildNumber, DeterministicKey, HDUtils}
import org.bitcoinj.wallet.{DeterministicKeyChain, DeterministicSeed}
import org.bouncycastle.asn1.sec.SECNamedCurves
import org.bouncycastle.asn1.x9.X9ECParameters
import org.bouncycastle.jce.spec.{ECParameterSpec, ECPrivateKeySpec, _}
import org.bouncycastle.math.ec
import org.constellation.keytool.KeyUtils
import org.constellation.keytool.KeyUtils.insertProvider
import org.constellation.wallet.BIP44.{getPrivateKeyFromECBigIntAndCurve, getPublicKeyFromECPoint}

class BIP44(seedPrase: String, childIndex: Int = 0, passphrase: String = "", creationTime: Long = new Date().getTime) {
  val chainPathPrefix: String = "M/44H/137036H/0H/0/" //137.036 is the fine structure constant ...
  val seed = new DeterministicSeed(seedPrase, null, passphrase, creationTime)
  val chain: DeterministicKeyChain = DeterministicKeyChain.builder.seed(seed).build

  def signData(data: Array[Byte])(implicit privateKey: PrivateKey = getChildKeyPairOfDepth().getPrivate): Array[Byte] =
    KeyUtils.signData(data)(privateKey)

  def getDeterministicKeyOfDepth(depth: Int = childIndex) = {
    val chainPath: String = chainPathPrefix + childIndex.toString()
    val keyPath: util.List[ChildNumber] = HDUtils.parsePath(chainPath)
    chain.getKeyByPath(keyPath, true)
  }

  def getChildKeyPairOfDepth(depth: Int = childIndex) = {
    val key: DeterministicKey = getDeterministicKeyOfDepth(depth)
    val rawPrivate: BigInteger = key.getPrivKey
    val privateKey: PrivateKey = getPrivateKeyFromECBigIntAndCurve(rawPrivate, "secp256k1")
    val publicKey: PublicKey = getPublicKeyFromECPoint(key.getPubKeyPoint)
    new KeyPair(publicKey, privateKey)
  }
}

object BIP44 extends App {
  import org.bouncycastle.jce.ECNamedCurveTable

  def getPrivateKeyFromECBigIntAndCurve(s: BigInteger, curveName: String) = {
    val ecParameterSpec: ECParameterSpec = ECNamedCurveTable.getParameterSpec(curveName)
    val privateKeySpec: ECPrivateKeySpec = new ECPrivateKeySpec(s, ecParameterSpec)
    val keyFactory = KeyFactory.getInstance("ECDSA")
    val priv = keyFactory.generatePrivate(privateKeySpec)
    priv
  }

  def getPublicKeyFromECPoint(point: ec.ECPoint) = {
    val kf = KeyFactory.getInstance(KeyUtils.ECDSA, insertProvider)
    val curve: X9ECParameters = SECNamedCurves.getByName("secp256k1")
    val params = new ECParameterSpec(curve.getCurve(), curve.getG(), curve.getN(), curve.getH())
    kf.generatePublic(new ECPublicKeySpec(point, params))
  }
}
