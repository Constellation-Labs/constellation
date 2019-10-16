package org.constellation.crypto

import java.io.FileInputStream

import cats.effect._
import java.security.{Key, KeyPair, KeyStore, PrivateKey}

object KeyStoreUtils {
  // TODO: kpudlik: Consider using platform-agnostic PKCS12 because JKS works only with Java.
  val storeType = "JKS"

  private def reader[F[_]: Sync](keyStorePath: String): Resource[F, FileInputStream] =
    Resource.fromAutoCloseable(Sync[F].delay {
      new FileInputStream(keyStorePath)
    })

  private def unlockKeyStore[F[_]: Sync](
    password: Array[Char]
  )(stream: FileInputStream): F[KeyStore] = Sync[F].delay {
    val keyStore = KeyStore.getInstance(storeType)
    keyStore.load(stream, password)
    keyStore
  }

  private def unlockKeyPair[F[_]: Sync](alias: String, keyPassword: Array[Char])(keyStore: KeyStore): F[KeyPair] =
    Sync[F].delay {
      val privateKey = keyStore.getKey(alias, keyPassword).asInstanceOf[PrivateKey]
      val publicKey = keyStore.getCertificate(alias).getPublicKey
      new KeyPair(publicKey, privateKey)
    }

  def fromPath[F[_]: Sync](
    path: String,
    alias: String,
    storePassword: Array[Char],
    keyPassword: Array[Char]
  ): Resource[F, KeyPair] =
    reader(path)
      .evalMap(unlockKeyStore[F](storePassword))
      .evalMap(unlockKeyPair[F](alias, keyPassword))
}
