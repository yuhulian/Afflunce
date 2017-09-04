package test
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext

import com.simon.QuarterPercentile

/**
  * Created by simon on 2017/8/17.
  */
object simonTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("test")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val rdd = sc.textFile("E:\\features").map(_.split("\\|")).map(x=>{
      val ageband = x(0).toDouble
      val datacomsuption = x(1).toDouble
      val arpu = x(2).toDouble
      val events_2g = x(3).toDouble
      val events_3g = x(4).toDouble
      val events_4g = x(5).toDouble
      val sms_counter = x(6).toDouble
      val incoming_call_counter = x(7).toDouble
      val outgoint_call_counter = x(8).toDouble
      val num_data_connections_2g = x(9).toDouble
      val num_data_connections_3g = x(10).toDouble
      val num_data_connections_4g = x(11).toDouble
      val localnumpoi = x(12).toDouble
      val localnumdwells = x(13).toDouble
      val roamnumpois = x(14).toDouble
      val roamnumdwells = x(15).toDouble
      val numroamcity = x(16).toDouble
      val commutedistance = x(17).toDouble
      val numuserlabels = x(18).toDouble
      val deviceprice = x(19).toDouble
//      val homerealestateprice = x(20).toDouble

      (ageband,datacomsuption,arpu,events_2g,events_3g,events_4g,sms_counter,incoming_call_counter,outgoint_call_counter,num_data_connections_2g,num_data_connections_3g,num_data_connections_4g,localnumpoi,localnumdwells,roamnumpois,roamnumdwells,numroamcity,commutedistance,numuserlabels,deviceprice)
    }).toDF("ageband","datacomsuption","arpu","events_2g","events_3g","events_4g","sms_counter","incoming_call_counter","outgoint_call_counter","num_data_connections_2g","num_data_connections_3g","num_data_connections_4g","localnumpoi","localnumdwells","roamnumpois","roamnumdwells","numroamcity","commutedistance","numuserlabels","deviceprice")

    val count = rdd.count()

    val vec = rdd.select("deviceprice").rdd.map(row => row.getAs[Double]("deviceprice"))
//    val count = vec.count()
    val qp = new QuarterPercentile(vec,count)

    println("-----deviceprice------")
    println("qp median:"+qp.median)
    println("qp uav:"+qp.uav)
    println("qp lif:"+qp.lif)
    println("qp q1:"+qp.q1)
    println("qp q3:"+qp.q3)
    println("qp iqr:"+qp.iqr)

    println("--------------------------")

    val fillMissingRDD = new QuarterPercentile(qp.replaceMissingRecord,count)
    println("median:"+fillMissingRDD.median)
    println("uav:"+fillMissingRDD.uav)
    println("lif:"+fillMissingRDD.lif)
    println("uof:"+fillMissingRDD.uof)
    println("lof:"+fillMissingRDD.lof)
    println("q1:"+fillMissingRDD.q1)
    println("q3:"+fillMissingRDD.q3)
    println("iqr:"+fillMissingRDD.iqr)
    //val clearVec = fillMissingRDD.replaceOutlier
    //clearVec.zip(vec).map(x=>x._1+"|"+x._2).saveAsTextFile("E:\\deviceprice")
  }
}
