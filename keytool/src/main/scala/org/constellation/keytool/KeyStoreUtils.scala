package org.constellation.keytool

import java.io._
import java.nio.charset.Charset
import java.nio.file.FileAlreadyExistsException
import java.security.cert.Certificate
import java.security.{Key, KeyPair, KeyStore, PrivateKey}

import cats.data.EitherT
import cats.effect._
import cats.syntax.all._
import org.bouncycastle.util.io.pem.{PemObject, PemWriter}
import org.constellation.keytool.cert.{DistinguishedName, SelfSignedCertificate}

object KeyStoreUtils {

  private val storeType: String = "PKCS12"
  private val storeExtension: String = "p12"
  private val singlePasswordKeyStoreSuffix: String = "_v2"
  private val privateKeyHexName: String = "id_ecdsa.hex"

  def readFromFileStream[F[_]: Sync, T](dataPath: String, streamParser: FileInputStream => F[T]) =
    reader(dataPath)
      .use(streamParser)
      .attemptT

  def storeWithFileStream[F[_]: Sync](path: String, bufferedWriter: FileOutputStream => F[Unit]) =
    writer(path)
      .use(bufferedWriter)
      .attemptT

  def parseFileOfTypeOp[F[_]: Sync, T](parser: String => Option[T])(stream: FileInputStream) =
    Resource
      .fromAutoCloseable(Sync[F].delay {
        new BufferedReader(new InputStreamReader(stream))
      })
      .use(inStream => Option(inStream.readLine()).flatMap(parser).pure[F])

  def writeTypeToFileStream[F[_]: Sync, T](serializer: T => String)(obj: T)(stream: FileOutputStream) =
    Resource
      .fromAutoCloseable(Sync[F].delay {
        new BufferedWriter(new OutputStreamWriter(stream))
      })
      .use(
        outStream =>
          Sync[F].delay {
            outStream.write(serializer(obj))
          }
      )

  def storeKeyPemDecrypted[F[_]: Sync](key: Key)(stream: FileOutputStream): F[Unit] =
    Resource
      .fromAutoCloseable(Sync[F].delay {
        new PemWriter(new OutputStreamWriter(stream))
      })
      .use { pemWriter =>
        val pemObj = new PemObject("EC KEY", key.getEncoded)
        Sync[F].delay {
          pemWriter.writeObject(pemObj)
        }
      }

  def exportPrivateKeyAsHex[F[_]: Sync](
    path: String,
    alias: String,
    storePassword: Array[Char],
    keyPassword: Array[Char]
  ): EitherT[F, Throwable, String] =
    for {
      keyPair <- keyPairFromStorePath(path, alias, storePassword, keyPassword)
      hex = KeyUtils.privateKeyToHex(keyPair.getPrivate)
      _ <- writer(pathDir(path) + privateKeyHexName)
        .use(
          os =>
            Sync[F].delay {
              os.write(hex.getBytes(Charset.forName("UTF-8")))
            }
        )
        .attemptT
    } yield hex

  def migrateKeyStoreToSinglePassword[F[_]: Sync](
    path: String,
    alias: String,
    storePassword: Array[Char],
    keyPassword: Array[Char]
  ): EitherT[F, Throwable, KeyStore] =
    for {
      keyPair <- keyPairFromStorePath(path, alias, storePassword, keyPassword)
      newKeyStore <- putKeyPairToStorePath(keyPair, withSinglePasswordSuffix(path), alias, storePassword, storePassword)
    } yield newKeyStore

  /**
    * Generates new keypair and puts it to new keyStore at path
    */
  def generateKeyPairToStorePath[F[_]: Sync](
    path: String,
    alias: String,
    storePassword: Array[Char],
    keyPassword: Array[Char]
  ): EitherT[F, Throwable, KeyStore] =
    writer(withExtension(path))
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

  /**
    * Puts existing to new keyStore at path
    */
  def putKeyPairToStorePath[F[_]: Sync](
    keyPair: KeyPair,
    path: String,
    alias: String,
    storePassword: Array[Char],
    keyPassword: Array[Char]
  ): EitherT[F, Throwable, KeyStore] =
    writer(withExtension(path))
      .use(
        stream =>
          for {
            keyStore <- createEmptyKeyStore(storePassword)
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

  def keyPairFromStorePath[F[_]: Sync](
    path: String,
    alias: String,
    storepass: Array[Char],
    keypass: Array[Char]
  ): EitherT[F, Throwable, KeyPair] =
    reader(path)
      .evalMap(unlockKeyStore[F](storepass))
      .evalMap(unlockKeyPair[F](alias, keypass))
      .use(_.pure[F])
      .attemptT

  private def reader[F[_]: Sync](path: String): Resource[F, FileInputStream] =
    Resource.fromAutoCloseable(Sync[F].delay {
      new FileInputStream(path)
    })

  private def writer[F[_]: Sync](path: String): Resource[F, FileOutputStream] =
    Resource.fromAutoCloseable(Sync[F].delay {
      val file = new File(path)
      if (file.exists()) {
        throw new FileAlreadyExistsException(file.getPath)
      }
      new FileOutputStream(file, false)
    })

  private def generateCertificateChain[F[_]: Sync](keyPair: KeyPair): F[Array[Certificate]] =
    Sync[F].delay {
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

  def loadEnvPasswords[F[_]: Sync]: EitherT[F, Throwable, Passwords] =
    EitherT.fromEither[F] {
      for {
        storepass <- sys.env.get("CL_STOREPASS").toRight(new RuntimeException("CL_STOREPASS is missing in environment"))
        keypass <- sys.env.get("CL_KEYPASS").toRight(new RuntimeException("CL_KEYPASS is missing in environment"))
      } yield Passwords(storepass = storepass.toCharArray, keypass = keypass.toCharArray)
    }

  private def pathDir(path: String): String = {
    val slashIndex = path.lastIndexOf("/")
    if (slashIndex > 0) {
      path.substring(0, slashIndex + 1)
    } else {
      ""
    }
  }

  private def withSinglePasswordSuffix(path: String): String =
    withExtension(withoutExtension(path) + singlePasswordKeyStoreSuffix)

  private def withExtension(path: String): String =
    if (path.endsWith(storeExtension)) path else s"$path.$storeExtension"

  private def withoutExtension(path: String): String = {
    val dotIndex = path.lastIndexOf(".")
    if (dotIndex > 0) {
      path.substring(0, dotIndex)
    } else {
      path
    }
  }
}
