package com.simon

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 1.这部分数据来自于CRM
  * 2.包括了ageband，datacomsuption，arpu以及deviceprice
  * 3.datacomsuption，arpu数据使用20161201-20170501周期
  * 4.deviceprice数据使用20160701-20170701周期
  * Created by simon on 2017/8/21.
  */
object attrCRMLocal {
  def main(args: Array[String]): Unit = {
    //    if (args.length != 4){
    //      println("wrong number of arguments!")
    //      println("usage:<$province><$start_dt><$end_dt><$result_dir>")
    //    }

    //parse the arguments
    //    val province = args(0)
    val start_dt = 20151201
    val end_dt = 20160201
    val start_year = start_dt.toString.substring(0, 4)
    val end_year = end_dt.toString.substring(0, 4)
    val start_month = start_dt.toString.substring(0, 6)
    val end_month = end_dt.toString.substring(0, 6)
    //    val result_dir = args(3)

    //initialize environment
    val conf = new SparkConf().setMaster("local[1]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    println(start_year)
    println(start_month)
    println(end_year)
    println(end_month)

    //month list generating
    def genMonthList(): List[String] = {
      val s_year = start_year.toInt
      val e_year = end_year.toInt
      var str = ""
      var monthIndex: List[String] = List()
      for (y <- s_year to e_year)
        for (m <- 1 to 12) {
          if (m < 10) {
            str = y.toString + "0" + m.toString
            monthIndex = str :: monthIndex
          }
          else
            monthIndex = (y.toString + m.toString) :: monthIndex
        }
      monthIndex.reverse
    }

    for (str <- genMonthList) {
      println(str)
    }

    println("-----------------------------------")

    def getMonths: List[String] = {
      val primeList = genMonthList()
      var afterDropList: List[String] = primeList
      for (s <- primeList) {
        if (s.toInt > end_month.toInt) {
          afterDropList = afterDropList.dropRight(1)
        }
        if (s.toInt < start_month.toInt) {
          afterDropList = afterDropList.drop(1)
        }
      }
      afterDropList
    }

//    println("getMonths:" + getMonths.toString())
//
//    for (mm <- getMonths) {
//      val curr_year = mm.toString.substring(0, 4)
//      val curr_month = mm.toString.substring(4, 6)
//      println("E:\\test\\crm\\" + curr_year + "\\" + curr_month + "\\p*")
//    }

    var listRDD: List[RDD[(String, String, String, String)]] = List()
    val aggRDD = {
      for (mm <- getMonths) {
        val curr_year = mm.toString.substring(0, 4)
        val curr_month = mm.toString.substring(4, 6)
        val crmRDD = sc.textFile("E:\\test\\crm\\" + curr_year + "\\" + curr_month + "\\p*").map(_.split("\\,")).map(x => {
          val user_id = x(0)
          val ageband = x(1)
          val arpu = x(2)
          val datacomsuption = x(3)
          (user_id, ageband, arpu, datacomsuption)
        })
        listRDD = listRDD :+ crmRDD
      }
      listRDD.reduce(_ union _)
    }.map(x=>(x._1,x._2+"|"+x._3+"|"+x._4)).reduceByKey(_+","+_)
    aggRDD.cache()

  }
}
