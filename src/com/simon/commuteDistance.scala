package com.simon

import java.text.SimpleDateFormat

import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by simon on 2017/8/24.
  */
object commuteDistance {
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

    val conf = new SparkConf().setAppName("commuteDistance")
    val sc = new SparkContext(conf)
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def getHourFigure(timeStr: String): Int = {
      val trimed = timeStr.substring(timeStr.indexOf(" "), timeStr.indexOf(":"))
      val testtime = if (trimed.startsWith("0"))
        trimed.toInt
      else
        trimed.drop(1).toInt
      testtime
    }

    //需要提取user_id（第2个），timeStart（第3个），timeStop（第4个），prevLocationId（第9个），nextLocationId（第12个），distance（第18个）
    //然后统计符合时间段范围的journey按照user_id+prevLocationId+nextLocationId为key进行count，选择最大那个的距离
    val jpRDD = sc.textFile("/user/ss_deploy/"+province+"/cip/ulm/journey_plus_final/0/"+year+"/"+month+"/[0-3][0-9]/csv_1_4/p*").map(_.split("\\|")).map(x => {
      val user_id = x(1)
      val timeStart = getHourFigure(x(2))
      //2017-03-01 05:46:00
      val timeStop = getHourFigure(x(3))
      val prevLocationId = x(8)
      val nextLocationId = x(11)
      val distance = x(17)
      (user_id, timeStart, timeStop, prevLocationId, nextLocationId, distance)
    }).filter(x => ((x._2 >= 5 && x._3 <= 10) || (x._2 >= 17 && x._3 <= 22))).map(x => (x._1, x._6)).reduceByKey(_ + "," + _)

    jpRDD.cache()

    val commuteRDD = jpRDD.map(x => {
      val user_id = x._1
      val distanceList = x._2
      val split = distanceList.split(",")
      var commuteList: List[String] = List()
      for (i <- 0 until split.length) {
        val distance = split(i)
        commuteList = commuteList :+ distance
      }
      val distance = commuteList.groupBy(identity).maxBy(_._2.size)._1
      user_id+"|"+distance
    })

    commuteRDD.coalesce(64).saveAsTextFile(resultDir, classOf[SnappyCodec])
  }
}
