package org.constellation.wallet

import java.security.{KeyPair, PrivateKey}
import java.util
import java.util.Date

import org.bitcoinj.crypto.{ChildNumber, DeterministicKey, HDUtils}
import org.bitcoinj.wallet.{DeterministicKeyChain, DeterministicSeed}
import org.bouncycastle.crypto.params.{ECDomainParameters, ECPrivateKeyParameters, ECPublicKeyParameters}
import org.bouncycastle.jcajce.provider.asymmetric.ec.{BCECPrivateKey, BCECPublicKey}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.constellation.keytool.KeyUtils
import org.constellation.wallet.BIP44.getECKeyPairFromBip44Key

/**
  * Only a reference implementation. It can't be used at this point without fixing the assembly issues.
  * Bitcoinj pulls in earlier version of bouncycastle -> 1.63 and we are using 1.65 across the project.
  * It wouldn't be a problem if not the case that Bitcoinj uses functionality which is removed in 1.65.
  * I think we don't want to go back with the version of bouncycastle used in the project until this point.
  * Currently we are removing that bouncycastle 1.63 jar file during wallet assembly.
  * When it's needed the Bitcoinj may be already released with BC upgraded to 1.65, which is the current version used
  * on Bitcoinj snapshot version, so in that case updating library may solve the issues. If it's not the case
  * some shading may be used, which I tried but couldn't achieve a 100% working implementation - some bouncycastle's
  * classes used in Bitcoinj still didn't reference the renamed/shaded packages and returned e.g. NoSuchMethod errors.
  */
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
    getECKeyPairFromBip44Key(key)
  }
}

object BIP44 {
  import org.bouncycastle.jce.ECNamedCurveTable

  def getECKeyPairFromBip44Key(bip44Key: DeterministicKey): KeyPair = {
    val rawPrivate = bip44Key.getPrivKey
    val ecPoint = bip44Key.getPubKeyPoint
    val bcConf = BouncyCastleProvider.CONFIGURATION
    val curveParams = ECNamedCurveTable.getParameterSpec("secp256k1")
    val g = curveParams.getG
    val n = curveParams.getN
    val h = curveParams.getH
    val curve = curveParams.getCurve
    val domainParams = new ECDomainParameters(curve, g, n, h)
    val privKeyParams = new ECPrivateKeyParameters(rawPrivate, domainParams)
    val publicKeyParams = new ECPublicKeyParameters(ecPoint, domainParams)
    val publicKey = new BCECPublicKey(KeyUtils.ECDSA, publicKeyParams, curveParams, bcConf)
    val privateKey = new BCECPrivateKey(KeyUtils.ECDSA, privKeyParams, publicKey, curveParams, bcConf)
    new KeyPair(publicKey, privateKey)
  }
}
