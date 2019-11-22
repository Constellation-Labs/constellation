package org.constellation.util

import java.io._
import java.security.spec.PKCS8EncodedKeySpec
import java.security._

import org.bouncycastle.jce.ECNamedCurveTable
import org.bouncycastle.jce.interfaces.ECPrivateKey
import org.bouncycastle.jce.spec.ECPublicKeySpec
import org.constellation.util.GenerateAddressString.addressWriter
import org.constellation.util.WalletClient.loadCliParams
import org.spongycastle.openssl.jcajce.JcaPEMKeyConverter
import org.spongycastle.openssl.{PEMKeyPair, PEMParser}

import scala.util.Try
//import java.security.interfaces.ECPrivateKey
import java.util.Base64

import cats.data.EitherT
import cats.effect.{ExitCode, IO, IOApp, Sync}
import constellation._
import org.bouncycastle.asn1.ASN1Sequence
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers
import org.bouncycastle.util.io.pem.{PemObject, PemWriter}
import org.bouncycastle.asn1.ASN1Object
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo
import org.constellation.domain.transaction.{LastTransactionRef, TransactionService}
import org.constellation.keytool.{KeyStoreUtils, KeyUtils}
import org.constellation.primitives.Transaction
import scopt.OParser

/*
 todo: move to schema project
 */
object WalletClient extends IOApp {
  /*
  Note: these vals need type annotation to compile
   */
  val transactionParser: FileInputStream => IO[Option[Transaction]] =
    KeyStoreUtils.parseFileOfTypeOp[IO, Transaction](ParseExt(_).x[Transaction])
  val transactionWriter: Transaction => FileOutputStream => IO[Unit] =
    KeyStoreUtils.storeTypeToFileStream[IO, Transaction](SerExt(_).json)

  def run(args: List[String]): IO[ExitCode] = {
    for {
      cliParams <- loadCliParams[IO](args)
      kp <- loadKeyPairFrom[IO](cliParams)
      prevTransactionOp <- KeyStoreUtils.readFromFileStream[IO, Option[Transaction]](cliParams.addressPath,
                                                                                     transactionParser)
      transactionEdge = TransactionService.createTransactionEdge( //todo, we need to sign on Ordinal + lastTxRef
        KeyUtils.publicKeyToAddressString(kp.getPublic),
        cliParams.destination,
        cliParams.amount.toDouble.toLong,
        kp
      )
      transaction = Transaction(transactionEdge, prevTransactionOp.map(_.lastTxRef).getOrElse(LastTransactionRef.empty))
      transactionWriteBuffer = transactionWriter(transaction)
      _ <- KeyStoreUtils.storeWithFileStream[IO](cliParams.storePath, transactionWriteBuffer)
    } yield transaction
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  //todo add case for storepass keypass as env variables
  def loadKeyPairFrom[F[_]: Sync](cliParams: WalletCliConfig): EitherT[F, Throwable, KeyPair] = {
    if (cliParams.privateKeyStr == null)
      KeyStoreUtils
        .keyPairFromStorePath[F](cliParams.keystore, cliParams.alias, cliParams.storepass, cliParams.keypass)
    else {
      val kp = KeyUtils.keyPairFromPemStr(cliParams.privateKeyStr, cliParams.pubKeyStr)
      val eitherLoadOrThrow =
        Try(Right(kp)).getOrElse(Left(new Throwable("Couldn't load KeyPair with PrivateKey provided")))

      EitherT(Sync[F].delay { eitherLoadOrThrow })
    }
  }

  def toASN1Obj(keyPair: KeyPair): ASN1ObjectInstance = {
    val privateKeyInfo: PrivateKeyInfo = PrivateKeyInfo.getInstance(ASN1Sequence.getInstance(keyPair.getPrivate.getEncoded))
    val isEC = privateKeyInfo.getPrivateKeyAlgorithm().getAlgorithm().equals(X9ObjectIdentifiers.id_ecPublicKey)
    if (! isEC) {
      throw new Exception ("not EC key")
    } else {
      val getASN1: ASN1ObjectInstance = new ASN1ObjectInstance(privateKeyInfo)
      getASN1
    }
  }

  def dumpPrivateKeyPem(keyPair: KeyPair, outputDir: String) = {
    val aSN1Obj: ASN1ObjectInstance = toASN1Obj(keyPair)
    val decryptedKeyOutput = new FileOutputStream(outputDir)
    val pemWriter = new PemWriter(new OutputStreamWriter(decryptedKeyOutput))
    val privPemObj = new PemObject("EC PRIVATE KEY", aSN1Obj.getEncoded("DER"))
    pemWriter.writeObject(privPemObj)
    pemWriter.close()
  }

  def parsePrivPem(path: String): PrivateKey = {
    val reader = new FileReader(path)
    val pemParser = new PEMParser(reader)
    val pemKeyPair = pemParser.readObject().asInstanceOf[PEMKeyPair]
    new JcaPEMKeyConverter().getKeyPair(pemKeyPair).getPrivate
  }

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, WalletCliConfig] = {
    val builder = OParser.builder[WalletCliConfig]
    val cliParser = {
      import builder._
      OParser.sequence(
        programName("wallet-client"),
        opt[String]("keystore").required
          .action((x, c) => c.copy(keystore = x)),
        opt[String]("alias").required
          .action((x, c) => c.copy(alias = x)),
        opt[String]("storepass").required
          .action((x, c) => c.copy(storepass = x.toCharArray)),
        opt[String]("keypass").required
          .action((x, c) => c.copy(keypass = x.toCharArray)) required,
        opt[String]("address_path").optional()
          .action((x, c) => c.copy(addressPath = x)),
        opt[String]("amount").required
          .action((x, c) => c.copy(amount = x)),
        opt[String]("fee").required
          .action((x, c) => c.copy(fee = x)),
        opt[String]("destination").required
          .action((x, c) => c.copy(destination = x)),
        opt[String]("store_path").required
          .action((x, c) => c.copy(storePath = x)),
        opt[String]("priv_key_str").optional
          .action((x, c) => c.copy(privateKeyStr = x)),
        opt[String]("pub_key_str").optional
          .action((x, c) => c.copy(pubKeyStr = x))
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, WalletCliConfig()).toRight(new RuntimeException("wallet CLI params are missing"))
    }
  }
}

class ASN1ObjectInstance(pKI: PrivateKeyInfo) extends ASN1Object {
  def toASN1Primitive = pKI.parsePrivateKey().toASN1Primitive
  override def getEncoded(str: String): Array[Byte] = super.getEncoded(str)
}

object GenerateAddressString extends IOApp {
  val addressWriter: String => FileOutputStream => IO[Unit] =
    KeyStoreUtils.storeTypeToFileStream[IO, String](SerExt(_).json)

  def run(args: List[String]): IO[ExitCode] = {
    for {
    cliParams <- loadCliParams[IO](args)
    address = KeyUtils.publicKeyToAddressString(KeyUtils.pemToPublicKey(cliParams.pubKeyStr))
    _ <- KeyStoreUtils.storeWithFileStream[IO](cliParams.storePath, addressWriter(address))
    } yield address
  }.fold[ExitCode](throw _, _ => ExitCode.Success)

  def loadCliParams[F[_]: Sync](args: Seq[String]): EitherT[F, Throwable, PublicKeyToAddressStringConfig] = {
    val builder = OParser.builder[PublicKeyToAddressStringConfig]
    val cliParser = {
      import builder._
      OParser.sequence(
        programName("address-generator"),
        opt[String]("pub_key_str").optional
          .action((x, c) => c.copy(pubKeyStr = x)),
        opt[String]("store_path").required
          .action((x, c) => c.copy(storePath = x)),
      )
    }
    EitherT.fromEither[F] {
      OParser.parse(cliParser, args, PublicKeyToAddressStringConfig()).toRight(new RuntimeException("Address CLI params are missing"))
    }
  }
}

case class PublicKeyToAddressStringConfig(
    pubKeyStr: String = null,
    storePath: String = null
  )


case class WalletCliConfig(
  keystore: String = null,
  alias: String = null,
  storepass: Array[Char] = null,
  keypass: Array[Char] = null,
  addressPath: String = null,
  amount: String = null,
  fee: String = null,
  destination: String = null,
  storePath: String = null,
  privateKeyStr: String = null,
  pubKeyStr: String = null
)
