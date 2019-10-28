package org.constellation.keytool.cert

import java.math.BigInteger
import java.security.cert.X509Certificate
import java.security.{KeyPair, SecureRandom, Security}
import java.util.{Calendar, Date}

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.spongycastle.asn1.x500.X500Name
import org.spongycastle.asn1.x509.SubjectPublicKeyInfo
import org.spongycastle.cert.X509v3CertificateBuilder
import org.spongycastle.cert.jcajce.JcaX509CertificateConverter
import org.spongycastle.operator.jcajce.JcaContentSignerBuilder

/**
  * KeyStore does not store PublicKeys directly but forces PublicKeys to be wrapped by Certificate.
  * The utility below creates a self-signed certificate.
  */
object SelfSignedCertificate {

  def generate(dn: String, pair: KeyPair, days: Int, algorithm: String): X509Certificate = {
    val bcProvider = new BouncyCastleProvider()
    Security.addProvider(bcProvider)

    val owner = new X500Name(dn)
    val serial = new BigInteger(64, new SecureRandom())

    val (notBefore, notAfter) = periodFromDays(days)

    val publicKeyInfo: SubjectPublicKeyInfo = SubjectPublicKeyInfo.getInstance(pair.getPublic.getEncoded)

    val builder = new X509v3CertificateBuilder(owner, serial, notBefore, notAfter, owner, publicKeyInfo)

    val signer = new JcaContentSignerBuilder(algorithm).setProvider(bcProvider).build(pair.getPrivate)

    val certificateHolder = builder.build(signer)

    val selfSignedCertificate = new JcaX509CertificateConverter().getCertificate(certificateHolder)

    selfSignedCertificate
  }

  private def periodFromDays(days: Int): (Date, Date) = {
    val notBefore = new Date()
    val calendar = Calendar.getInstance
    calendar.setTime(notBefore)
    calendar.add(Calendar.DAY_OF_MONTH, days)
    val notAfter = calendar.getTime
    (notBefore, notAfter)
  }
}
