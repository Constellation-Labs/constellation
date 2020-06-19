package org.constellation.wallet

import java.security.{KeyPair, PrivateKey}
import java.util
import java.util.Date

import org.bitcoinj.crypto.{ChildNumber, DeterministicKey, HDUtils}
import org.bitcoinj.params.MainNetParams
import org.bitcoinj.wallet.{DeterministicKeyChain, DeterministicSeed}
import org.bouncycastle.crypto.params.{ECDomainParameters, ECPrivateKeyParameters, ECPublicKeyParameters}
import org.bouncycastle.jcajce.provider.asymmetric.ec.{BCECPrivateKey, BCECPublicKey}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.constellation.keytool.KeyUtils
import org.constellation.wallet.BIP44.getECKeyPairFromBip44Key

class BIP44(seedPrase: String, childIndex: Int = 0, passphrase: String = "", creationTime: Long = new Date().getTime) {
  //1137 is DAG coin type taken from https://github.com/satoshilabs/slips/blob/master/slip-0044.md
  val chainPathPrefix: String = "M/44H/1137H/0H/0/"
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

    // For debugging
    val params = new MainNetParams
    val mainKey = chain.getKeyByPath(HDUtils.parsePath(chainPathPrefix), true)
    println(s"seed:             ${chain.getSeed.toHexString}")
    println(s"mainKeyPriv:      ${mainKey.serializePrivB58(params)}")
    println(s"mainKeyPublic:    ${mainKey.serializePubB58(params)}")
    println(s"privChildBase58:  ${key.serializePrivB58(params)}")
    println(s"pubChildBase58:   ${key.serializePubB58(params)}")
    println(s"privChildKeyWiF:  ${key.getPrivateKeyAsWiF(params)}")
    println(s"privChildKey hex: ${KeyUtils.bytes2hex(key.getPrivKeyBytes)}")
    println(s"pubChildKey hex:  ${KeyUtils.bytes2hex(key.getPubKey)}")
    println("RAW PRIVATE:      " + KeyUtils.bytes2hex(key.getPrivKey.toByteArray))
    // end

    getECKeyPairFromBip44Key(key)
  }
}

object BIP44 extends App {
  import org.bouncycastle.jce.ECNamedCurveTable

  def getECKeyPairFromBip44Key(bip44Key: DeterministicKey): KeyPair = {
    val rawPrivate = bip44Key.getPrivKey
    val ecPoint = bip44Key.getPubKeyPoint
    val bcConf = BouncyCastleProvider.CONFIGURATION
    val curveParams = ECNamedCurveTable.getParameterSpec("secp256k1")
    val n = curveParams.getN
    val curve = curveParams.getCurve
    val domainParams = new ECDomainParameters(curve, ecPoint, n)
    val privKeyParams = new ECPrivateKeyParameters(rawPrivate, domainParams)
    val publicKeyParams = new ECPublicKeyParameters(ecPoint, domainParams)
    val publicKey = new BCECPublicKey(KeyUtils.ECDSA, publicKeyParams, curveParams, bcConf)
    val privateKey = new BCECPrivateKey(KeyUtils.ECDSA, privKeyParams, publicKey, curveParams, bcConf)
    new KeyPair(publicKey, privateKey)
  }
}
