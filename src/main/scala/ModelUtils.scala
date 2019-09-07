import breeze.linalg.DenseVector

object ModelUtils extends App {

  def fitDistribution(numNodes: Int) = {

    val x = DenseVector.zeros[Double](numNodes)
    val test = x(3 to 4) := .5
    test
  }

  val a = DenseVector(1d, 1d, 1d)//DenseMatrix.zeros[Double](10,1)
  val b: DenseVector[Double] = DenseVector(2.0d, 2.0d, 2.0d)
  val c = a * b
//  val d: DenseVector[Double] = b.:^=(2.0d)

//  val v = DenseVector(1.0, 2.0, 3.0)
//  val y =  v :* 2.0

//  println(d)

}
