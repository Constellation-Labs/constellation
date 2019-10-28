package org.constellation.keytool

import java.io.{File, FileInputStream, FileOutputStream}
import java.security.cert.Certificate
import java.security.{KeyPair, KeyStore, PrivateKey}

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import org.constellation.keytool.cert.{DistinguishedName, SelfSignedCertificate}

object KeyStoreUtils {
  val storeType = "JKS"

  private def reader[F[_]: Sync](keyStorePath: String): Resource[F, FileInputStream] =
    Resource.fromAutoCloseable(Sync[F].delay {
      new FileInputStream(keyStorePath)
    })

  private def writer[F[_]: Sync](keyStorePath: String): Resource[F, FileOutputStream] =
    Resource.fromAutoCloseable(Sync[F].delay {
      val file = new File(keyStorePath)
      // TODO: Check if file exists
      new FileOutputStream(file)
    })

  private def generateCertificateChain[F[_]: Sync](keyPair: KeyPair): F[Array[Certificate]] =
    Sync[F].delay {
      val keyPair = KeyUtils.makeKeyPair()
      // TODO: Maybe move to config
      val dn = DistinguishedName(
        commonName = "constellationnetwork.io",
        organization = "Constellation Labs"
      )

      val validity = 365 * 1000 // // 1000 years of validity should be enough I guess

      val certificate = SelfSignedCertificate.generate(dn.toString, keyPair, validity, KeyUtils.DefaultSignFunc)

      Array(certificate)
    }

  private def unlockKeyStore[F[_]: Sync](
    password: Array[Char]
  )(stream: FileInputStream): F[KeyStore] = Sync[F].delay {
    val keyStore = KeyStore.getInstance(storeType)
    keyStore.load(stream, password)
    keyStore
  }

  private def createEmptyKeyStore[F[_]: Sync](password: Array[Char]): F[KeyStore] = Sync[F].delay {
    val keyStore = KeyStore.getInstance(storeType)
    keyStore.load(null, password)
    keyStore
  }

  private def unlockKeyPair[F[_]: Sync](alias: String, keyPassword: Array[Char])(keyStore: KeyStore): F[KeyPair] =
    Sync[F].delay {
      val privateKey = keyStore.getKey(alias, keyPassword).asInstanceOf[PrivateKey]
      val publicKey = keyStore.getCertificate(alias).getPublicKey
      new KeyPair(publicKey, privateKey)
    }

  private def setKeyEntry[F[_]: Sync](
    alias: String,
    keyPair: KeyPair,
    keyPassword: Array[Char],
    chain: Array[Certificate]
  )(keyStore: KeyStore): F[KeyStore] =
    Sync[F].delay {
      keyStore.setKeyEntry(alias, keyPair.getPrivate, keyPassword, chain)
      keyStore
    }

  private def store[F[_]: Sync](stream: FileOutputStream, storePassword: Array[Char])(
    keyStore: KeyStore
  ): F[KeyStore] = Sync[F].delay {
    keyStore.store(stream, storePassword)
    keyStore
  }

  private def loadEnvPasswords[F[_]: Sync]: EitherT[F, Throwable, EnvPasswords] =
    EitherT.fromEither[F] {
      for {
        storepass <- sys.env.get("CL_STOREPASS").toRight(new RuntimeException("CL_STOREPASS is missing in environment"))
        keypass <- sys.env.get("CL_KEYPASS").toRight(new RuntimeException("CL_KEYPASS is missing in environment"))
      } yield EnvPasswords(storepass = storepass.toCharArray, keypass = keypass.toCharArray)
    }

  def keyPairToStorePath[F[_]: Sync](
    path: String,
    alias: String,
    storePassword: Array[Char],
    keyPassword: Array[Char]
  ): EitherT[F, Throwable, KeyStore] =
    writer(path)
      .use(
        stream =>
          for {
            keyStore <- createEmptyKeyStore(storePassword)
            keyPair <- KeyUtils.makeKeyPair().pure[F]
            chain <- generateCertificateChain(keyPair)
            _ <- setKeyEntry(alias, keyPair, keyPassword, chain)(keyStore)
            _ <- store(stream, storePassword)(keyStore)
          } yield keyStore
      )
      .attemptT

  def keyPairFromStorePath[F[_]: Sync](
    path: String,
    alias: String
  ): EitherT[F, Throwable, KeyPair] =
    for {
      env <- loadEnvPasswords
      keyPair <- reader(path)
        .evalMap(unlockKeyStore[F](env.storepass))
        .evalMap(unlockKeyPair[F](alias, env.keypass))
        .use(_.pure[F])
        .attemptT
    } yield keyPair
}
