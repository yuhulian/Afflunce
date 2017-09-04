package com.simon

import java.util

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by simon on 2017/8/24.
  */
object attrUni {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Wrong number of arguments!")
      println("Usage: <$PROVINCE> <$MONTH> <$RESULT_DIR>")
      System.exit(1)
    }
    val province = args(0)
    val monthid = args(1)
    val year = args(1).toString.substring(0, 4)
    val month = args(1).toString.substring(4, 6)
    val resultDir = args(2)+"/"+monthid+"/"+province

    val conf = new SparkConf().setAppName("attrUni")
    val sc = new SparkContext(conf)

    val user_network_info = sc.textFile("/user/ss_deploy/"+ province + "/cip/common/user_network_info/0/"+ year + "/" + month + "/[0-3][0-9]/csv_2_3/p*").map(_.split("\\|")).map(x => (x(0), x(12) + "|" + x(13) + "|" + x(14))).reduceByKey(_ + "," + _)
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
    uniRDD.coalesce(64).saveAsTextFile(resultDir)
  }
}
