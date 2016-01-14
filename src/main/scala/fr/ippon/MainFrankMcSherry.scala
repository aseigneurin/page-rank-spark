package fr.ippon

import fr.ippon.MainFrankMcSherry.Edge
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

// http://www.frankmcsherry.org/pagerank/distributed/performance/2015/07/08/pagerank.html
object MainFrankMcSherry {

  case class Edge(src: Int, dest: Int)

  def main(args: Array[String]) {

    val edges = Seq(
      Edge(0, 1),
      Edge(0, 2),
      Edge(1, 2),
      Edge(2, 0),
      Edge(3, 2)
    )

    val vertices = 4

    val alpha = 0.85

    // mutable per-vertex state
    val src = new Array[Double](vertices)
    val dst = new Array[Double](vertices)
    val deg = new Array[Double](vertices)

    // determine vertex degrees
    edges.map(edge => deg(edge.dest) += 1)
    println("deg=" + deg.mkString(", "))

    // perform 20 iterations
    for (i <- Range(1, 2)) {
      // prepare src ranks
      for (vertex <- Range(0, vertices)) {
        src(vertex) = alpha * dst(vertex) / deg(vertex)
        dst(vertex) = 1.0 - alpha
      }

      println("iteration " + i)
      println("src=" + src.mkString(", "))
      println("dst=" + dst.mkString(", "))

      // do the expensive part
      edges.map(edge => dst(edge.dest) += src(edge.src))
    }

    println("src=" + src.mkString(", "))
    println("dst=" + dst.mkString(", "))
  }

}