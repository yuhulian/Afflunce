package com.simon

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.util.control.Breaks.{break, breakable}

/**
  * 1.这部分数据来自于CRM
  * 2.包括了ageband，datacomsuption，arpu以及deviceprice
  * 3.datacomsuption，arpu数据使用20161201-20170501周期
  * 4.deviceprice数据使用20160701-20170701周期
  * Created by simon on 2017/8/21.
  */
object attrCRM {
  def main(args: Array[String]): Unit = {
    //    if (args.length != 4){
    //      println("wrong number of arguments!")
    //      println("usage:<$province><$start_dt><$end_dt><$result_dir>")
    //    }

    //parse the arguments
    //    val province = args(0)
    val start_dt = 201512
    val end_dt = 201702
    val start_year = start_dt.toString.substring(0, 4)
    val end_year = end_dt.toString.substring(0, 4)
    //    val result_dir = args(3)

    //initialize environment
    val conf = new SparkConf().setMaster("local[1]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    println(start_year)
    println(end_year)

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
      monthIndex
    }

    def getMonths:List[String] = {
      val primeList = genMonthList()
      var afterDropList: List[String] = primeList
      for (s <- primeList) {
        if (s.toInt > end_dt.toInt) {
          afterDropList = afterDropList.drop(1)
        }
        if (s.toInt < start_dt.toInt) {
          afterDropList = afterDropList.dropRight(1)
        }
        //afterDropList = afterDropList.reverse
        //      println(afterDropList)
      }

      afterDropList.reverse
    }


      val aggRDD = for (mm <- getMonths;i <- 0 to getMonths.size) {
        val curr_year = mm.toString.substring(0, 4)
        val curr_month = mm.toString.substring(4, 6)
        val crmRDD = sc.textFile("/user/ss_deploy/cip/common/ucrm/0/"+curr_year+"/"+curr_month+"/01/csv_3_1/p*").map(_.split("\\|")).map(x => {
          val user_id = x(0)
          val ageband = x(6)
          val arpu = x(10)
          val datacomsuption = x(19)
          (user_id,ageband,arpu,datacomsuption)
      })
        sc.union(crmRDD)
    }




  }
}
