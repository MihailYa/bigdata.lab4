package com.sparkTutorial.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.orc.util.BloomFilterIO.Encoding
import org.apache.spark._
import org.apache.spark.graphx.{VertexId, _}
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.TraversableOnce
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


object Lab4 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("s").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val sqlContext = SparkSession.builder().config(conf).getOrCreate().sqlContext
    import sqlContext.implicits._

    val vertsAirports: RDD[(VertexId, (Float, Float, String))] = sqlContext.read.format("csv").load("rsc/airports-extended.dat").map(fields => {
      (toLongOrZero(fields.getString(0)), (toFloatOrZero(fields.getString(6)), toFloatOrZero(fields.getString(7)), toStringOrEmpty(fields.getString(3))))
    }).rdd

    //    def vertsAirports: RDD[(VertexId, (Float, Float))] = sc.textFile("rsc/airports-extended.dat")
    //      .map(f => {
    //      val fields = f.split(",")
    //      (toLongOrZero(fields(0)), (toFloatOrZero(fields(6)), toFloatOrZero(fields(7))))
    //    })

    //    def vertsAirlines: RDD[(VertexId, (Float, Float))] = sc.textFile("rsc/airports-extended.dat").map(f => {
    //      val fields = f.split(",")
    //      (fields(0).toLong, (fields(6).toFloat, fields(7).toFloat))
    //    })

    val routesEdges: RDD[Edge[Float]] = sqlContext.read.format("csv").load("rsc/routes.dat").map(fields => {
      val srcAirportId = toLongOrZero(fields.getString(3))
      val dstAirportId = toLongOrZero(fields.getString(5))
      //      val srcAirport = vertsAirports.lookup(srcAirportId).head
      //      val dstAirport = vertsAirports.lookup(dstAirportId).head

      val dist = 1f //Math.sqrt((srcAirport._1 - dstAirport._1)* (srcAirport._1 - dstAirport._1) + (srcAirport._2 - dstAirport._2)* (srcAirport._2 - dstAirport._2)).toFloat
      Edge(srcAirportId, dstAirportId, dist)
    }).rdd


    //    def edges: RDD[Edge[Int]] = sc.textFile("rsc/routes.dat").map(f => {
    //      val fields = f.split(",")
    //      Edge(toLongOrZero(fields(1)), toLongOrZero(fields(3)), 1)
    //    })

    val graph = Graph(vertsAirports, routesEdges)

    // Task 1
    //Знайти авіакомпанію з найбільшою сумою відстаней всіх рейсів. Теж саме з
    //найменшою сумою


    // Task 2
    //Знайти всі можливі рейси між Україною та Італією (при не більше ніж 2-х
    //стиковках).
    val ukraine = "Ukraine"
    val italy = "Italy"
    val res2 = lib.ShortestPaths.run(graph,
      graph.vertices.filter(
        param => {
          if (param._2 == null) {
            println(param)
            false
          } else {
            param._2._3.equals(italy)
          }
        }
      ).map(param => param._1).collect()
    ).vertices
      .filter(param => param != null)
      .filter(param => param._2 != null)
      .filter(param => {
        if (param == null || param._2 == null) {
          false
        } else {
          val lookup = vertsAirports.lookup(param._1)
          lookup.nonEmpty && param._2.size <= 2 && lookup.head._3.equals(ukraine)
        }
      }).collect()
    println("#2: " + res2.length)


    // Task 3
    //println("3 max: " + graph.inDegrees.reduce((a, b) => if(a._2 > b._2) a else b))
    //println("3 min: " + graph.inDegrees.reduce((a, b) => if(a._2 < b._2) a else b))


    // Task 4
    // Знайти найкоротший та найдовший маршрут між 2 заданими аеропортами. За
    //довжину рейсу беремо відстань між аеропортами. Необхідно врахувати маршрути з
    //пересадками
    //    import org.apache.spark.graphx._
    //    def dijkstra[VD](g: Graph[VD, Float], origin: VertexId) = {
    //      var g2 = g.mapVertices(
    //        (vid, vd) => (false, if (vid == origin) 0 else Float.MaxValue))
    //
    //      for (i <- 1L to g.vertices.count - 1) {
    //        val currentVertexId =
    //          g2.vertices.filter(!_._2._1)
    //            .fold((0L, (false, Float.MaxValue)))((a, b) =>
    //              if (a._2._2 < b._2._2) a else b)
    //            ._1
    //
    //        val newDistances = g2.aggregateMessages[Float](
    //          ctx => if (ctx.srcId == currentVertexId)
    //            ctx.sendToDst(ctx.srcAttr._2 + ctx.attr),
    //          (a, b) => math.min(a, b))
    //
    //        g2 = g2.outerJoinVertices(newDistances)((vid, vd, newSum) =>
    //          (vd._1 || vid == currentVertexId,
    //            math.min(vd._2, newSum.getOrElse(Float.MaxValue))))
    //      }
    //
    //      g.outerJoinVertices(g2.vertices)((vid, vd, dist) =>
    //        (vd, dist.getOrElse((false, Float.MaxValue))._2))
    //    }
    //
    //    println("#4: " + dijkstra(graph, 1L).vertices.map(_._2).collect)


    //lib.TriangleCount.run(graph)

    // Task 6
    // Знайти аеропорти, що не мають сполучення між собою (якщо такі є)
    val res = lib.ShortestPaths.run(graph, Array(1L)).vertices.filter(param => param._2.isEmpty).collect()
    println("#6 Has no connections count: " + res.length)
    println("#6 [1] " + res(0)._1)
    println("#6 [2] " + res(1)._1)
    println("#6 [3] " + res(2)._1)
    //println("#6 " + res(1)._1 + " " + res(1)._2)
    //println("#6 " + res(2)._1 + " " + res(2)._2)

    graph.connectedComponents()

    println(graph.vertices.count())
  }

  def toLongOrZero(value: String): Long = {
    if (value == "\\N")
      0
    else
      value.toLong
  }

  def toFloatOrZero(value: String): Float = {
    if (value == "\\N")
      0
    else
      value.toFloat
  }

  def toStringOrEmpty(value: String): String = {
    if (value == "\\N")
      ""
    else
      value
  }
}
