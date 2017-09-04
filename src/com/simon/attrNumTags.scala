package com.simon

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by simon on 2017/8/29.
  */
object attrNumTags {
  def main(args: Array[String]) {
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

    val conf: SparkConf = new SparkConf().setAppName("com.Countlabels")
    val sc: SparkContext = new SparkContext(conf)
    val InputPath = args(0)
    val outputPath = args(1)
    //3551387653202018725|H007005|7|66887|074|201701
    val data=sc.textFile("/user/ss_deploy/workspace/user_labels/"+year+"/"+month+"/"+province+"/p*").map(x=>{
      var o = x.split("\\|")
      o(0)+"|"+o(1)
    }).distinct().map(x=>{
      var o = x.split("\\|")
      (o(0),1)
    }).reduceByKey(_+_).map(x=>{x._1+"|"+x._2})
    data.coalesce(64).saveAsTextFile(resultDir)
  }
}
