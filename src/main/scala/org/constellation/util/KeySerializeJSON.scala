package org.constellation.util

import java.security.{KeyPair, PrivateKey, PublicKey}

import org.constellation.crypto.KeyUtils.{hexToPrivateKey, hexToPublicKey, privateKeyToHex, publicKeyToHex}
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, Formats, JObject}

trait KeySerializeJSON {

  implicit val constellationFormats: Formats

  class PrivateKeySerializer
      extends CustomSerializer[PrivateKey](
        format =>
          ({
            case jObj: JObject =>
              // implicit val f: Formats = format
              hexToPrivateKey((jObj \ "key").extract[String])
          }, {
            case key: PrivateKey =>
              JObject("key" -> JString(privateKeyToHex(key)))
          })
      )

  class PublicKeySerializer
      extends CustomSerializer[PublicKey](
        format =>
          ({
            case jstr: JObject =>
              // implicit val f: Formats = format
              hexToPublicKey((jstr \ "key").extract[String])
          }, {
            case key: PublicKey =>
              JObject("key" -> JString(publicKeyToHex(key)))
          })
      )

  class KeyPairSerializer
      extends CustomSerializer[KeyPair](
        format =>
          ({
            case jObj: JObject =>
              //  implicit val f: Formats = format
              val pubKey = (jObj \ "publicKey").extract[PublicKey]
              val privKey = (jObj \ "privateKey").extract[PrivateKey]
              val kp = new KeyPair(pubKey, privKey)
              kp
          }, {
            case key: KeyPair =>
              //  implicit val f: Formats = format
              JObject(
                "publicKey" -> JObject("key" -> JString(publicKeyToHex(key.getPublic))),
                "privateKey" -> JObject("key" -> JString(privateKeyToHex(key.getPrivate)))
              )
          })
      )

}
