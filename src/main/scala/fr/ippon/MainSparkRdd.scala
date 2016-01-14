package fr.ippon

import com.google.common.base.Stopwatch
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object MainSparkRdd {

  case class Edge(src: String, dest: String)

  case class EdgeAndDegree(src: String, dest: String, degree: Int)

  case class EdgeDegreePageRank(src: String, dest: String, degree: Int, pageRank: Double)

  case class Delta(dest: String, delta: Double)

  case class VertexAndPageRank(vertex: String, pageRank: Double)

  val alpha = 0.85
  val iterations = 40

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("page-rank")
    .set("spark.ui.enabled", "false")
    .set("spark.ui.showConsoleProgress", "false")
  //    .set("spark.sql.shuffle.partitions", "1")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]) {
    val stopwatch = new Stopwatch().start()

    val edgesRDD = loadData()
    //      .cache()

    println("Records: " + edgesRDD.count())
    println("Elapsed: " + stopwatch)

    val res = computePageRank(edgesRDD)

    val resDF = sqlContext.createDataFrame(res)
      .sort(col("pageRank").desc)
    resDF.show()

    println("Elapsed: " + stopwatch.stop())
  }

  def loadData(): RDD[Edge] = {
    //    val edges = Seq(
    //      Edge("A", "B"),
    //      Edge("A", "C"),
    //      Edge("B", "C"),
    //      Edge("C", "A"),
    //      Edge("D", "C")
    //    )
    //    sc.parallelize(edges)

    sc.textFile("data/twitter_rv_sample_1M.net", minPartitions = 8)
      .map(_.split("\t"))
      .map { case Array(src, dest) => Edge(src, dest) }
  }

  def computePageRank(edgesRDD: RDD[Edge]): RDD[VertexAndPageRank] = {
    val edgeAndDegreesRDD = computeDegree(edgesRDD)

    var edgeDegreePageRankRDD = edgeAndDegreesRDD.map(e => EdgeDegreePageRank(e.src, e.dest, e.degree, 1 - alpha))

    // cache
    edgeDegreePageRankRDD.persist()
    edgeDegreePageRankRDD.count() // force computing

    for (i <- Range(0, iterations)) {
      println("Iteration " + i)
      val stopwatch = new Stopwatch().start()

      // compute the delta
      val reducedDeltasRDD = computeDeltas(edgeDegreePageRankRDD)

      println("reducedDeltasRDD.partitions: " + reducedDeltasRDD.partitions.length)

      //      println()
      //      println("-- reducedDeltasRDD")
      //      reducedDeltasRDD.foreach(println)

      // keep the previous RDD to uncache it later
      val prevEdgeDegreePageRankRDD = edgeDegreePageRankRDD

      // compute the new PR
      edgeDegreePageRankRDD = updatePageRanks(edgeDegreePageRankRDD, reducedDeltasRDD)

      println("edgeDegreePageRankRDD.partitions: " + edgeDegreePageRankRDD.partitions.length)

      //      println()
      //      println("-- edgeDegreePageRankRDD")
      //      edgeDegreePageRankRDD.foreach(println)

      // cache
      edgeDegreePageRankRDD.persist()
      edgeDegreePageRankRDD.count() // force computing
      prevEdgeDegreePageRankRDD.unpersist()

      println("Elapsed in iteration " + i + ": " + stopwatch)
      println()
      stopwatch.reset()
    }

    edgeDegreePageRankRDD
      .map(e => VertexAndPageRank(e.src, e.pageRank))
      .distinct()
  }

  def computeDegree(edgesRDD: RDD[Edge]): RDD[EdgeAndDegree] = {
    val edgesDegreeRDD = edgesRDD.map(edge => (edge.src, 1))
      .reduceByKey(_ + _)

    val edgeAndDegreesRDD = edgesRDD.map(edge => (edge.src, edge))
      .join(edgesDegreeRDD)
      .map {
        case (_, (edge: Edge, degree: Int)) => EdgeAndDegree(edge.src, edge.dest, degree)
      }

    edgeAndDegreesRDD
  }

  def computeDeltas(edgeDegreePageRankRDD: RDD[EdgeDegreePageRank]): RDD[(String, Double)] = {
    val deltasRDD = edgeDegreePageRankRDD.map(e => (e.dest, alpha * e.pageRank / e.degree))

    deltasRDD.reduceByKey(_ + _)
  }

  def updatePageRanks(edgeDegreePageRankRDD: RDD[EdgeDegreePageRank], reducedDeltasRDD: RDD[(String, Double)]): RDD[EdgeDegreePageRank] = {
    edgeDegreePageRankRDD.map(e => (e.src, e))
      .leftOuterJoin(reducedDeltasRDD)
      .map {
        case (src, (e, Some(delta))) => EdgeDegreePageRank(e.src, e.dest, e.degree, (1 - alpha) + delta)
        case (src, (e, None)) => EdgeDegreePageRank(e.src, e.dest, e.degree, (1 - alpha))
      }
  }

}