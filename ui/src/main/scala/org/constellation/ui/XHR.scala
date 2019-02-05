package org.constellation.ui

import org.scalajs.dom._
import org.scalajs.dom.raw.XMLHttpRequest

object XHR {

  def post[W: upickle.Writer, R: upickle.Reader]
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
          import upickle._
          //println("Response text", xhr.responseText)
          read[R](xhr.responseText)
        }
        callback(text)
      }
    }
    xhr.open("POST", path)
    xhr.setRequestHeader("Content-Type", "application/json")
    val out = {
      import upickle._
      write(payload)
    }
    if (printSer) println("POST", out)
    xhr.send(out)
  }

  def get[R: upickle.Reader]
  (
    callback: R => Unit,
    path: String
  ): Unit = {
    val xhr = new XMLHttpRequest()
    xhr.onreadystatechange = (e: Event) => {
      if (xhr.readyState == 4 && xhr.status == 200) {
        val text = {
          import upickle._
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

