package com.simon

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import java.util

/**
  * Created by simon on 2017/8/24.
  */
object attrUniLocal {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("test")
    val sc = new SparkContext(conf)
    val user_network_info = sc.textFile("E:\\test\\uni\\uni").map(_.split("\\|")).map(x => (x(0), x(12) + "|" + x(13) + "|" + x(14))).reduceByKey(_ + "," + _)
    val uniRDD = user_network_info.map(
      x => {
        val user_id = x._1
        var smscounterList = new util.ArrayList[Int]
        var incounterList = new util.ArrayList[Int]
        var outcounterList = new util.ArrayList[Int]

        var smscounter = 0
        var incounter = 0
        var outcounter = 0

        var strArr: Array[String] = x._2.split(",")

        for (i <- 0 to (strArr.length - 1)) {
          val split: Array[String] = strArr(i).split("\\|")
          println(split(0)+"|"+split(1)+"|"+split(2))
          smscounterList.add(split(0).toInt)
          incounterList.add(split(1).toInt)
          outcounterList.add(split(2).toInt)
        }

        for (i <- 0 to smscounterList.size() - 1) {
          smscounter += smscounterList.get(i)
        }

        for (i <- 0 to incounterList.size() - 1) {
          incounter += incounterList.get(i)
        }

        for (i <- 0 to outcounterList.size() - 1) {
          outcounter += outcounterList.get(i)
        }

        val smscounterAvg = (smscounter.toDouble / smscounterList.size()).formatted("%.2f")
        val incounterAvg = (incounter.toDouble / incounterList.size()).formatted("%.2f")
        val outcounterAvg = (outcounter.toDouble / outcounterList.size()).formatted("%.2f")
        (user_id, smscounterAvg, incounterAvg, outcounterAvg)
      }).map(x => x._1 + "|" + x._2 + "|" + x._3 + "|" + x._4)
    uniRDD.collect().foreach(println)
  }
}
