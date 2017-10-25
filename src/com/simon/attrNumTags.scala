package com.simon

import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 说明：这是最新版本
  * 计算每个用户每个月的标签数量
  * 数据结构：user_id|label_codes|duration|flux|province|month
  * 数据样例：3551387653202018725|H007005|7|66887|074|201701
  * Created by simon on 2017/8/29.
  */
object attrNumTags {
  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Wrong number of arguments!")
      println("Usage: <$ROOT_DIR> <$MONTH> <$RESULT_DIR>")
      System.exit(1)
    }
    val ROOT_DIR = args(0)

    val monthid = args(1)
    val year = args(1).toString.substring(0, 4)
    val month = args(1).toString.substring(4, 6)
    val resultDir = args(2) + "/" + monthid + "/"

    val conf: SparkConf = new SparkConf().setAppName("NumTags")
    val sc: SparkContext = new SparkContext(conf)

//    val ROOT_DIR = "/user/ss_deploy/workspace/user_labels"
    //

    val provinceList = List("010","011","013","017","018","019","030","031","034","036","038","050","051","059","070","071","074","075","076","079","081","083","084","085","086","087","088","089","090","091","097")
    for(province <- provinceList){
      val data=sc.textFile(ROOT_DIR + "/" + year + "/" + month + "/" + province + "/p*").map(x=>{
        var o = x.split("\\|")
        o(0)+"|"+o(1)
      }).distinct().map(x=>{
        var o = x.split("\\|")
        (o(0),1)
      }).reduceByKey(_+_).map(x=>{x._1+"|"+x._2})
      data.coalesce(26).saveAsTextFile(resultDir + province, classOf[SnappyCodec])
    }
  }
}
