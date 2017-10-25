package com.simon

import java.util

import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 说明：该程序目前是写死的目录，后续需要调整
  * Created by simon on 2017/9/1.
  */
object getAverageTmpV1 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Wrong number of arguments!")
      println("Usage: <$PROVINCE> <$RESULT_DIR>")
      System.exit(1)
    }
    val province = args(0)
    val resultDir = args(1) + "/" + province


    val conf: SparkConf = new SparkConf().setAppName("getAverage")
    val sc: SparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //get ageband, datacomsuption, arpu, deviceprice
    val crmRDD = sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/basicinfo/crm/20[1-2][0-9][0-1][0-9]01/" + province + "/p*").map(_.split("\\|")).map(x => (x(0), x(1) + "|" + x(2) + "|" + x(3) + "|" + x(4))).reduceByKey(_ + "," + _)

    val crmFinalRDD = crmRDD.map(x => {
      val user_id = x._1
      val crminfo = x._2
      var agebandList = new util.ArrayList[String]
      var datacomsuptionList = new util.ArrayList[String]
      var arpuList = new util.ArrayList[String]
      var devicepriceList = new util.ArrayList[String]

      val split = crminfo.split(",")

      var strArr: Array[String] = x._2.split(",")
      for (i <- 0 to (strArr.length - 1)) {
        val split: Array[String] = strArr(i).split("\\|")
        val ageband = split(0)
        val datacomsuption = split(1)
        val arpu = split(2)
        val deviceprice = split(3)

        agebandList.add(ageband)
        datacomsuptionList.add(datacomsuption)
        arpuList.add(arpu)
        devicepriceList.add(deviceprice)
      }

      var ageband = 0.0
      var datacomsuption = 0.0
      var arpu = 0.0
      var deviceprice = 0.0

      for (i <- 0 until agebandList.size(); if !agebandList.get(i).equals("-999")) {
        ageband += agebandList.get(i).toDouble
      }
      ageband = ageband / agebandList.size()

      for (i <- 0 until datacomsuptionList.size(); if !datacomsuptionList.get(i).equals("-999")) {
        datacomsuption += datacomsuptionList.get(i).toDouble
      }
      datacomsuption = datacomsuption / datacomsuptionList.size()

      for (i <- 0 until arpuList.size(); if !arpuList.get(i).equals("-999")) {
        arpu += arpuList.get(i).toDouble
      }
      arpu = arpu / arpuList.size()

      for (i <- 0 until devicepriceList.size(); if !devicepriceList.get(i).equals("-999")) {
        deviceprice += devicepriceList.get(i).toDouble
      }
      deviceprice = deviceprice / devicepriceList.size()

      (user_id, ageband + "|" + datacomsuption + "|" + arpu + "|" + deviceprice)
    })

    crmFinalRDD.cache()

    //get sms_counter, incoming_calls, outgoing_calls
    val uniRDD = sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/basicinfo/calls/20[1-2][0-9][0-1][0-9]01/" + province + "/p*").map(_.split("\\|")).map(x => (x(0), x(1) + "|" + x(2))).reduceByKey(_ + "," + _)
    val uniFinalRDD = uniRDD.map(x => {
      val user_id = x._1
      val uniinfo = x._2
      //var smsList = new util.ArrayList[String]
      var incallList = new util.ArrayList[String]
      var outcallList = new util.ArrayList[String]

      val split = uniinfo.split(",")

      var strArr: Array[String] = x._2.split(",")
      for (i <- 0 to (strArr.length - 1)) {
        val split: Array[String] = strArr(i).split("\\|")
        //val sms = split(0)
        val incall = split(0)
        val outcall = split(1)

        //smsList.add(sms)
        incallList.add(incall)
        outcallList.add(outcall)
      }

      var sms = 0.0
      var incall = 0.0
      var outcall = 0.0

      //      for(i<- 0 until smsList.size(); if ! smsList.get(i).equals("-999")){
      //        sms += smsList.get(i).toDouble
      //      }
      //      sms = sms/smsList.size()

      for (i <- 0 until incallList.size(); if !incallList.get(i).equals("-999")) {
        incall += incallList.get(i).toDouble
      }
      incall = incall / incallList.size()

      for (i <- 0 until outcallList.size(); if !outcallList.get(i).equals("-999")) {
        outcall += outcallList.get(i).toDouble
      }
      outcall = outcall / outcallList.size()


      //(user_id,sms+"|"+incall+"|"+outcall)
      (user_id, incall + "|" + outcall)
    })

    uniFinalRDD.cache()

    //get localnumpois, localnumdwells, roamnumpois, roamnumdwells, numroamcities
    val poiRDD = sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/basicinfo/poi/20[1-2][0-9][0-1][0-9]01/" + province + "/p*").map(_.split("\\|")).map(x => (x(0), x(1) + "|" + x(2) + "|" + x(3) + "|" + x(4) + "|" + x(5))).reduceByKey(_ + "," + _)
    val poiFinalRDD = poiRDD.map(x => {
      val user_id = x._1
      val poiinfo = x._2

      var localnumpoisList = new util.ArrayList[String]
      var localnumdwellsList = new util.ArrayList[String]
      var roamnumpoisList = new util.ArrayList[String]
      var roamnumdwellsList = new util.ArrayList[String]
      var numroamcitiesList = new util.ArrayList[String]

      val split = poiinfo.split(",")

      var strArr: Array[String] = x._2.split(",")
      for (i <- 0 to (strArr.length - 1)) {
        val split: Array[String] = strArr(i).split("\\|")
        val localnumpois = split(0)
        val localnumdwells = split(1)
        val roamnumpois = split(2)
        val roamnumdwells = split(3)
        val numroamcities = split(4)

        localnumpoisList.add(localnumpois)
        localnumdwellsList.add(localnumdwells)
        roamnumpoisList.add(roamnumpois)
        roamnumdwellsList.add(roamnumdwells)
        numroamcitiesList.add(numroamcities)
      }

      var localnumpois = 0.0
      var localnumdwells = 0.0
      var roamnumpois = 0.0
      var roamnumdwells = 0.0
      var numroamcities = 0.0

      for (i <- 0 until localnumpoisList.size(); if !localnumpoisList.get(i).equals("-999")) {
        localnumpois += localnumpoisList.get(i).toDouble
      }
      localnumpois = localnumpois / localnumpoisList.size()

      for (i <- 0 until localnumdwellsList.size(); if !localnumdwellsList.get(i).equals("-999")) {
        localnumdwells += localnumdwellsList.get(i).toDouble
      }
      localnumdwells = localnumdwells / localnumdwellsList.size()

      for (i <- 0 until roamnumpoisList.size(); if !roamnumpoisList.get(i).equals("-999")) {
        roamnumpois += roamnumpoisList.get(i).toDouble
      }
      roamnumpois = roamnumpois / roamnumpoisList.size()

      for (i <- 0 until roamnumdwellsList.size(); if !roamnumdwellsList.get(i).equals("-999")) {
        roamnumdwells += roamnumdwellsList.get(i).toDouble
      }
      roamnumdwells = roamnumdwells / roamnumdwellsList.size()

      for (i <- 0 until numroamcitiesList.size(); if !numroamcitiesList.get(i).equals("-999")) {
        numroamcities += numroamcitiesList.get(i).toDouble
      }
      numroamcities = numroamcities / numroamcitiesList.size()

      (user_id, localnumpois + "|" + localnumdwells + "|" + roamnumpois + "|" + roamnumdwells + "|" + numroamcities)
    })

    poiFinalRDD.cache()

    //get commutedistance
    val commuteRDD = sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/basicinfo/commute/20[1-2][0-9][0-1][0-9]01/" + province + "/p*").map(_.split("\\|")).map(x => (x(0), x(1))).reduceByKey(_ + "," + _)
    val commuteFinalRDD = commuteRDD.map(x => {
      val user_id = x._1
      val commuteinfo = x._2
      var commuteList = new util.ArrayList[String]

      val split = commuteinfo.split(",")

      var strArr: Array[String] = x._2.split(",")
      for (i <- 0 to (strArr.length - 1)) {
        val split: Array[String] = strArr(i).split("\\|")
        val commutedistance = split(0)
        commuteList.add(commutedistance)
      }

      var commutedistance = 0.0
      for (i <- 0 until commuteList.size(); if !commuteList.get(i).equals("-999")) {
        commutedistance += commuteList.get(i).toDouble
      }
      commutedistance = commutedistance / commuteList.size()
      (user_id, commutedistance)
    })
    commuteFinalRDD.cache()

    //get numoftags
    val tagRDD = sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/basicinfo/tags/20[1-2][0-9][0-1][0-9]01/" + province + "/p*").map(_.split("\\|")).map(x => (x(0), x(1))).reduceByKey(_ + "," + _)
    val tagFinalRDD = tagRDD.map(x => {
      val user_id = x._1
      val taginfo = x._2
      var tagList = new util.ArrayList[String]

      val split = taginfo.split(",")

      var strArr: Array[String] = x._2.split(",")
      for (i <- 0 to (strArr.length - 1)) {
        val split: Array[String] = strArr(i).split("\\|")
        val tags = split(0)
        tagList.add(tags)
      }

      var tags = 0.0
      for (i <- 0 until tagList.size(); if !tagList.get(i).equals("-999")) {
        tags += tagList.get(i).toDouble
      }
      tags = tags / tagList.size()
      (user_id, tags)
    })
    tagFinalRDD.cache()

    //get real estate price of home area
    val rsRDD = sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/basicinfo/rsprice/csv_1_0/" + province + "/*/p*").map(_.split("\\|")).map(x => (x(0), x(1)))
    rsRDD.cache()

    val step1RDD = crmFinalRDD.leftOuterJoin(uniFinalRDD).map(x => (x._1, x._2._1 + "|" + x._2._2.getOrElse("-999|-999")))
    val step2RDD = step1RDD.leftOuterJoin(poiFinalRDD).map(x => (x._1, x._2._1 + "|" + x._2._2.getOrElse("-999|-999|-999|-999|-999")))
    val step3RDD = step2RDD.leftOuterJoin(commuteFinalRDD).map(x => (x._1, x._2._1 + "|" + x._2._2.getOrElse("-999")))
    val step4RDD = step3RDD.leftOuterJoin(tagFinalRDD).map(x => (x._1, x._2._1 + "|" + x._2._2.getOrElse("-999")))
    val finalRDD = step4RDD.leftOuterJoin(rsRDD).map(x => x._1 + "|" + x._2._1 + "|" + x._2._2.getOrElse("-999")).coalesce(16).saveAsTextFile(resultDir, classOf[SnappyCodec])

    //    val rdd = finalRDD.map(_.split("\\|")).map(x=>{
    //      val user_id = x(0)
    //      val ageband = x(1).toDouble
    //      val datacomsuption = x(2).toDouble
    //      val arpu = x(3).toDouble
    //      val deviceprice = x(4).toDouble
    //      val sms_counter = x(5).toDouble
    //      val incoming_call_counter = x(6).toDouble
    //      val outgoint_call_counter = x(7).toDouble
    //      val localnumpoi = x(8).toDouble
    //      val localnumdwells = x(9).toDouble
    //      val roamnumpois = x(10).toDouble
    //      val roamnumdwells = x(11).toDouble
    //      val numroamcity = x(12).toDouble
    //      val commutedistance = x(13).toDouble
    //      val numuserlabels = x(14).toDouble
    //      val rsprice = x(15).toDouble
    //      //      val homerealestateprice = x(20).toDouble
    //
    //      (ageband,datacomsuption,arpu,deviceprice,sms_counter,incoming_call_counter,outgoint_call_counter,localnumpoi,localnumdwells,roamnumpois,roamnumdwells,numroamcity,commutedistance,numuserlabels,rsprice)
    //    }).toDF("ageband","datacomsuption","arpu","deviceprice","sms_counter","incoming_call_counter","outgoint_call_counter","localnumpoi","localnumdwells","roamnumpois","roamnumdwells","numroamcity","commutedistance","numuserlabels","rsprice")
    //
    //    val count = rdd.count()
    //
    //    val vec = rdd.select("datacomsuption").rdd.map(row => row.getAs[Double]("datacomsuption"))
    //    //    val count = vec.count()
    //    val qp = new QuarterPercentile(vec,count)
    //    val fillMissingRDD = new QuarterPercentile(qp.replaceMissingRecord,count)
  }
}
