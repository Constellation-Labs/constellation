package org.constellation

import com.typesafe.scalalogging.Logger
import org.scalatest.FlatSpec
import slick.jdbc.H2Profile.api._

import scala.concurrent.ExecutionContext.Implicits.global
// Use H2Profile to connect to an H2 database

import constellation._

class H2SlickTest extends FlatSpec {

  val logger = Logger("H2SlickTest")

  // Definition of the SUPPLIERS table

  class Suppliers(tag: Tag)
      extends Table[(Int, String, String, String, String, String)](tag, "SUPPLIERS") {

    def id = column[Int]("SUP_ID", O.PrimaryKey) // This is the primary key column

    def name = column[String]("SUP_NAME")

    def street = column[String]("STREET")

    def city = column[String]("CITY")

    def state = column[String]("STATE")

    def zip = column[String]("ZIP")
    // Every table needs a * projection with the same type as the table's type parameter

    def * = (id, name, street, city, state, zip)
  }
  val suppliers = TableQuery[Suppliers]

  // Definition of the COFFEES table

  class Coffees(tag: Tag) extends Table[(String, Int, Double, Int, Int)](tag, "COFFEES") {

    def name = column[String]("COF_NAME", O.PrimaryKey)

    def supID = column[Int]("SUP_ID")

    def price = column[Double]("PRICE")

    def sales = column[Int]("SALES")

    def total = column[Int]("TOTAL")

    def * = (name, supID, price, sales, total)
    // A reified foreign key relation that can be navigated to create a join

    def supplier = foreignKey("SUP_FK", supID, suppliers)(_.id)
  }
  val coffees = TableQuery[Coffees]

  "H2" should "initialize" in {

    val db = Database.forConfig("h2mem1")
    try {
      val setup = DBIO.seq(
        // Create the tables, including primary and foreign keys
        (suppliers.schema ++ coffees.schema).create,
        // Insert some suppliers
        suppliers += (101, "Acme, Inc.", "99 Market Street", "Groundsville", "CA", "95199"),
        suppliers += (49, "Superior Coffee", "1 Party Place", "Mendocino", "CA", "95460"),
        suppliers += (150, "The High Ground", "100 Coffee Lane", "Meadows", "CA", "93966"),
        // Equivalent SQL code:
        // insert into SUPPLIERS(SUP_ID, SUP_NAME, STREET, CITY, STATE, ZIP) values (?,?,?,?,?,?)

        // Insert some coffees (using JDBC's batch insert feature, if supported by the DB)
        coffees ++= Seq(
          ("Colombian", 101, 7.99, 0, 0),
          ("French_Roast", 49, 8.99, 0, 0),
          ("Espresso", 150, 9.99, 0, 0),
          ("Colombian_Decaf", 101, 8.99, 0, 0),
          ("French_Roast_Decaf", 49, 9.99, 0, 0)
        )
        // Equivalent SQL code:
        // insert into COFFEES(COF_NAME, SUP_ID, PRICE, SALES, TOTAL) values (?,?,?,?,?)
      )

      val setupFuture = db.run(setup)

      setupFuture.get()

      // Read all coffees and print them to the console
      logger.debug("Coffees:")
      db.run(coffees.result)
        .map(_.foreach {
          case (name, supID, price, sales, total) =>
            logger.debug("  " + name + "\t" + supID + "\t" + price + "\t" + sales + "\t" + total)
        })
      // Equivalent SQL code:
      // select COF_NAME, SUP_ID, PRICE, SALES, TOTAL from COFFEES

    } finally db.close

    assert(true)

  }

}
