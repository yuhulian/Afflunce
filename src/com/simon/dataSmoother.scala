package com.simon

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by simon on 2017/8/21.
  */
object dataSmoother {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Wrong number of arguments!")
      println("Usage: <$PROVINCE> <$INPUT_ROOT_DIR> <$RESULT_DIR>")
      System.exit(1)
    }
    val province = args(0)
    val input_dir = args(1)+"/"+province
    val resultDir = args(2)+"/"+province

    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rdd = sc.textFile(input_dir).map(_.split("\\|")).map(x=>{
      val user_id = x(0)
      val ageband = x(1).toDouble
      val datacomsuption = x(2).toDouble
      val arpu = x(3).toDouble
      val sms_counter = x(4).toDouble
      val incoming_call_counter = x(5).toDouble
      val outgoint_call_counter = x(6).toDouble
      val localnumpoi = x(7).toDouble
      val localnumdwells = x(8).toDouble
      val roamnumpois = x(9).toDouble
      val roamnumdwells = x(10).toDouble
      val numroamcity = x(11).toDouble
      val commutedistance = x(12).toDouble
      val numuserlabels = x(13).toDouble
      val deviceprice = x(14).toDouble
      val homerealestateprice = x(15).toDouble

      (user_id,ageband,datacomsuption,arpu,deviceprice,sms_counter,incoming_call_counter,outgoint_call_counter,localnumpoi,localnumdwells,roamnumpois,roamnumdwells,numroamcity,commutedistance,numuserlabels,homerealestateprice)
    })
    
    val df = rdd.map(x=>(x._3,x._4,x._5,x._6,x._7,x._8,x._9,x._10,x._11,x._12,x._13,x._14,x._15,x._16)).toDF("datacomsuption","arpu","deviceprice","sms_counter","incoming_call_counter","outgoint_call_counter","localnumpoi","localnumdwells","roamnumpois","roamnumdwells","numroamcity","commutedistance","numuserlabels","homerealestateprice")

    val count = df.count()

    //preocess datacomsuption
    val vec1 = df.select("datacomsuption").rdd.map(row => row.getAs[Double]("datacomsuption"))
    val qp1 = new QuarterPercentile(vec1,count)
    //replace missing
    val fillMissingRDD1 = new QuarterPercentile(qp1.replaceMissingRecord,count)
    //replace outlier
    val clearVec1 = fillMissingRDD1.replaceOutlier

    //process arpu
    val vec2 = df.select("arpu").rdd.map(row => row.getAs[Double]("arpu"))
    val qp2 = new QuarterPercentile(vec2,count)
    //replace missing
    val fillMissingRDD2 = new QuarterPercentile(qp2.replaceMissingRecord,count)
    //replace outlier
    val clearVec2 = fillMissingRDD2.replaceOutlier

    //deviceprice
    val vec3 = df.select("deviceprice").rdd.map(row => row.getAs[Double]("deviceprice"))
    val qp3 = new QuarterPercentile(vec3,count)
    //replace missing
    val fillMissingRDD3 = new QuarterPercentile(qp3.replaceMissingRecord,count)
    //replace outlier
    val clearVec3 = fillMissingRDD3.replaceOutlier

    //sms_counter
    val vec4 = df.select("sms_counter").rdd.map(row => row.getAs[Double]("sms_counter"))
    val qp4 = new QuarterPercentile(vec4,count)
    //replace missing
    val fillMissingRDD4 = new QuarterPercentile(qp4.replaceMissingRecord,count)
    //replace outlier
    val clearVec4 = fillMissingRDD4.replaceOutlier


    //incoming_call_counter
    val vec5 = df.select("incoming_call_counter").rdd.map(row => row.getAs[Double]("incoming_call_counter"))
    val qp5 = new QuarterPercentile(vec5,count)
    //replace missing
    val fillMissingRDD5 = new QuarterPercentile(qp5.replaceMissingRecord,count)
    //replace outlier
    val clearVec5 = fillMissingRDD5.replaceOutlier


    //outgoint_call_counter
    val vec6 = df.select("outgoint_call_counter").rdd.map(row => row.getAs[Double]("outgoint_call_counter"))
    val qp6 = new QuarterPercentile(vec6,count)
    //replace missing
    val fillMissingRDD6 = new QuarterPercentile(qp6.replaceMissingRecord,count)
    //replace outlier
    val clearVec6 = fillMissingRDD6.replaceOutlier


    //localnumpoi
    val vec7 = df.select("localnumpoi").rdd.map(row => row.getAs[Double]("localnumpoi"))
    val qp7 = new QuarterPercentile(vec7,count)
    //replace missing
    val fillMissingRDD7 = new QuarterPercentile(qp7.replaceMissingRecord,count)
    //replace outlier
    val clearVec7 = fillMissingRDD7.replaceOutlier

    //localnumdwells
    val vec8 = df.select("localnumdwells").rdd.map(row => row.getAs[Double]("localnumdwells"))
    val qp8 = new QuarterPercentile(vec8,count)
    //replace missing
    val fillMissingRDD8 = new QuarterPercentile(qp8.replaceMissingRecord,count)
    //replace outlier
    val clearVec8 = fillMissingRDD8.replaceOutlier

    //roamnumpois
    val vec9 = df.select("roamnumpois").rdd.map(row => row.getAs[Double]("roamnumpois"))
    val qp9 = new QuarterPercentile(vec9,count)
    //replace missing
    val fillMissingRDD9 = new QuarterPercentile(qp9.replaceMissingRecord,count)
    //replace outlier
    val clearVec9 = fillMissingRDD9.replaceOutlier

    //roamnumdwells
    val vec10 = df.select("roamnumdwells").rdd.map(row => row.getAs[Double]("roamnumdwells"))
    val qp10 = new QuarterPercentile(vec10,count)
    //replace missing
    val fillMissingRDD10 = new QuarterPercentile(qp10.replaceMissingRecord,count)
    //replace outlier
    val clearVec10 = fillMissingRDD10.replaceOutlier

    //numroamcity
    val vec11 = df.select("numroamcity").rdd.map(row => row.getAs[Double]("numroamcity"))
    val qp11 = new QuarterPercentile(vec11,count)
    //replace missing
    val fillMissingRDD11 = new QuarterPercentile(qp11.replaceMissingRecord,count)
    //replace outlier
    val clearVec11 = fillMissingRDD11.replaceOutlier

    //commutedistance
    val vec12 = df.select("commutedistance").rdd.map(row => row.getAs[Double]("commutedistance"))
    val qp12 = new QuarterPercentile(vec12,count)
    //replace missing
    val fillMissingRDD12 = new QuarterPercentile(qp12.replaceMissingRecord,count)
    //replace outlier
    val clearVec12 = fillMissingRDD12.replaceOutlier

    //numuserlabels
    val vec13 = df.select("numuserlabels").rdd.map(row => row.getAs[Double]("numuserlabels"))
    val qp13 = new QuarterPercentile(vec13,count)
    //replace missing
    val fillMissingRDD13 = new QuarterPercentile(qp13.replaceMissingRecord,count)
    //replace outlier
    val clearVec13 = fillMissingRDD13.replaceOutlier

    //homerealestateprice
    val vec14 = df.select("homerealestateprice").rdd.map(row => row.getAs[Double]("homerealestateprice"))
    val qp14 = new QuarterPercentile(vec14,count)
    //replace missing
    val fillMissingRDD14 = new QuarterPercentile(qp14.replaceMissingRecord,count)
    //replace outlier
    val clearVec14 = fillMissingRDD14.replaceOutlier

    //merge datacomsuption
    val step1 = rdd.zip(clearVec1).map(x=>x._1._1+"|"+x._1._2.formatted("%.1f")+"|"+x._2.formatted("%.1f"))

    //merge arpu
    val step2 = step1.zip(clearVec2).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge deviceprice
    val step3 = step2.zip(clearVec3).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge sms_counter
    val step4 = step3.zip(clearVec4).map(x=>x._1+"|"+x._2.formatted("%.1f"))
    //merge incoming_call_counter
    val step5 = step4.zip(clearVec5).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge outgoint_call_counter
    val step6 = step5.zip(clearVec6).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge localnumpoi
    val step7 = step6.zip(clearVec7).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge roamnumdwells
    val step8 = step7.zip(clearVec8).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge roamnumpois
    val step9 = step8.zip(clearVec9).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge roamnumdwells
    val step10 = step9.zip(clearVec10).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge numroamcity
    val step11 = step10.zip(clearVec11).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge commutedistance
    val step12 = step11.zip(clearVec12).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge numuserlabels
    val step13 = step12.zip(clearVec13).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    //merge homerealestateprice
    val step14 = step13.zip(clearVec14).map(x=>x._1+"|"+x._2.formatted("%.1f"))

    step14.coalesce(16).saveAsTextFile(resultDir)
  }
}
