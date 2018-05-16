package org.constellation.ui


import org.scalajs.dom
import org.scalajs.dom.raw.{HTMLDivElement, HTMLElement, HTMLInputElement, MouseEvent}

import scala.scalajs.js
import scala.scalajs.js.{Date, JSApp}
import scala.scalajs.js.annotation.JSExport
import scala.util.Try

object App extends JSApp {

  @JSExport
  def main(): Unit = {

    import scalatags.JsDom.all._
    println("hello world")
  }

}