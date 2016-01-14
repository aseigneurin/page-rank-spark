package fr.ippon

import com.google.common.base.Stopwatch

// http://www.cs.princeton.edu/~chazelle/courses/BIB/pagerank.htm
object MainIanRogers {

  case class Edge(src: Int, dest: Int)

  val vertices = 4
  val alpha = 0.85

  def main(args: Array[String]) {
    val stopwatch = new Stopwatch().start()

    val edges = Seq(
      Edge(0, 1),
      Edge(0, 2),
      Edge(1, 2),
      Edge(2, 0),
      Edge(3, 2)
    )

    val pr = computePageRankV1(edges)

    println("pr=" + pr.mkString(", "))
    println("Elapsed: " + stopwatch)
  }

  def computePageRankV1(edges: Seq[Edge]): Seq[Double] = {
    val pr = new Array[Double](vertices)
    val delta = new Array[Double](vertices)
    val deg = new Array[Double](vertices)

    // determine vertex degrees
    edges.map(edge => deg(edge.src) += 1)
    println("deg=" + deg.mkString(", "))

    for (i <- Range(0, 20)) {
      println("iteration " + (i + 1))

      // compute the delta
      for (vertex <- Range(0, vertices))
        delta(vertex) = 0
      edges.map(edge => delta(edge.dest) += alpha * pr(edge.src) / deg(edge.src))

      println("delta=" + delta.mkString(", "))

      // compute the new PR
      for (vertex <- Range(0, vertices)) {
        pr(vertex) = (1 - alpha) + delta(vertex)
      }

      println("pr=" + pr.mkString(", "))
      println()
    }

    pr
  }
//
//  //  class EdgeAndDegree(src: Int, dest: Int, var degree: Int)
//
//  class EdgeDegreePageRank(src: Int, dest: Int, var degree: Int, var pageRank: Double)
//
//  def computePageRankV2(edges: Seq[Edge]): Seq[Double] = {
//    //    val pr = new Array[Double](vertices)
//    //    val delta = new Array[Double](vertices)
//    //    val deg = new Array[Double](vertices)
//
//    val degrees: Map[Int, Int] = edges.groupBy(edge => edge.src)
//      .mapValues(edges => edges.length)
//
//    val edgeDegreePageRanks: Array[EdgeDegreePageRank] = edges
//      .map(edge => new EdgeDegreePageRank(edge.src, edge.dest, degrees(edge.src), 1 - alpha))
//      .toArray
//
//    for (i <- Range(0, 20)) {
//      println("iteration " + (i + 1))
//
//      // compute the delta
//      for (vertex <- Range(0, vertices))
//        delta(vertex) = 0
//      edges.map(edge => delta(edge.dest) += alpha * pr(edge.src) / deg(edge.src))
//
//      println("delta=" + delta.mkString(", "))
//
//      // compute the new PR
//      for (vertex <- Range(0, vertices)) {
//        pr(vertex) = (1 - alpha) + delta(vertex)
//      }
//
//      println("pr=" + pr.mkString(", "))
//      println()
//    }
//
//    pr
//  }

}