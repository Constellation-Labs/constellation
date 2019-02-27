package org.constellation.ui


import org.scalajs.dom._
import org.scalajs.dom.raw.XMLHttpRequest
import upickle.default._


object XHR {

  def post[W: Writer, R: Reader]
  (
    payload: W,
    callback: R => Unit,
    path: String,
    printSer: Boolean = false
  ): Unit = {
    val xhr = new XMLHttpRequest()
    xhr.onreadystatechange = (e: Event) => {
      if (xhr.readyState == 4 && xhr.status == 200) {
        val text = {
       //   import upickle.legacy._
          //println("Response text", xhr.responseText)
          read[R](xhr.responseText)
        }
        callback(text)
      }
    }
    xhr.open("POST", path)
    xhr.setRequestHeader("Content-Type", "application/json")
    val out = {
    //  import upickle.legacy._
      write[W](payload)
    }
    if (printSer) println("POST", out)
    xhr.send(out)
  }

  def get[R: Reader]
  (
    callback: R => Unit,
    path: String
  ): Unit = {
    val xhr = new XMLHttpRequest()
    xhr.onreadystatechange = (e: Event) => {
      if (xhr.readyState == 4 && xhr.status == 200) {
        val text = {
        //  import upickle.legacy._
          //println("Response text", xhr.responseText)
          read[R](xhr.responseText)
        }
        callback(text)
      }
    }
    xhr.open("GET", path)
    xhr.send()
  }
}
