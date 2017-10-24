package com.simon

import org.apache.spark.{SparkConf, SparkContext}
import java.util

/**
  * * 程序功能：根据传入的省份、月度参数筛选符合条件的活跃用户，活跃用户包含了几个方面
  *  1. 符合通话设备类型，包括了:1：功能机 2：智能手机 4：儿童电话手表 5：国外手机 8:平板
  *  2：数据位置：/user/ss_deploy/workspace/crm/2017/07/011/result/
  *  3：数据结构（有问题）：MONTH_ID|USER_ID|IMEI_TAC|AGEBAND|GENDER|ISCONTRACT|ISPREPAY|CLIENTTYPE|TELEMATIC|DATAONLY|DISTRIBUTION_CHANNEL_ID|TENURE|CURRENTCONTRACTS|PREMIUMCONTRACTS|SERVICE_TYPE|USERSTATUS|DATACONSUMPTION|MONTHLYSPEND|MVNO|PROVINCE_ID|ADMIN_ID|HOME_ZONE_ID
  *  4: 样例数据：|4912212863554400522|FDEC18C914E9C90805744992CCC5BF6E|9DAD37D4D6C8FFFDE8D7887708B80435|05|01|0|1|0|0|0|1010a0398|3|04|66000616|30AAAAAA|01|85.8837890625|16.21|0|011|201705|V0110000|110114
  *  5: 取user_id(1)，imei_tac(2)，ageband(3)，telematic(8),data_consumption(16),arpu(17)
  * Created by simon on 2017/9/1.
  */
object attrVCRM {
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
    val ROOT_DIR = "/user/ss_deploy/workspace/crm"
    val inpath = ROOT_DIR  + "/" + year + "/" + month + "/"

    val conf = new SparkConf().setAppName("attrVCRM")
    val sc = new SparkContext(conf)

    val humanList = sc.textFile("/user/ss_deploy/workspace/crm/2017/06/011/result/p*").map(_.split("\\|")).map(x=>(x(1),x(9))).filter(x=>(x._2.equals("1")||x._2.equals("2")||x._2.equals("4")||x._2.equals("5")||x._2.equals("8")))

    //x(1)-->user_id,x(3)-->TAC,x(4)-->ageband, x(17)-->datacomsuption, x(18)-->arpu(monthly spend)
    //key-->user_id, value-->(user_id,TAC,ageband,datacomsuption,arpu)
    val crmRDD = sc.textFile("/user/ss_deploy/workspace/crm/" + year + "/" + month + "/" + province +"/result/p*").map(_.split("\\|")).map(x=>(x(1),(x(1),x(3),x(4),x(17),x(18))))

    //key-->TAC, value-->(user_id,ageband,datacomsuption,arpu)
    val humanFinalRDD = humanList.join(crmRDD).map(x=>(x._2._2._2,(x._2._2._1,x._2._2._3,x._2._2._4,x._2._2._5)))

    //key-->tac, value-->device_model
    val tacModelRDD = sc.textFile("/serv/smartsteps/raw/reference/device_type/"+ year + "/" + month + "/000/CM*").map(_.split("\\t")).map(x=>(x(0),x(2)))

    //key-->device_model,value-->(user_id,ageband,datacomsuption,arpu)
    val deviceModelRDD = humanFinalRDD.leftOuterJoin(tacModelRDD).map(x=>(x._2._2.getOrElse("-999"),(x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4)))

    //key-->device_model,price
    val modelPriceRDD= sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/input/deviceTypeInfo.txt").map(_.split("\\t")).map(x=>(x(0),x(1)))

    //key-->user_id,value-->ageband|datacomsuption|arpu|device_price
    val deviceTmpRDD = deviceModelRDD.leftOuterJoin(modelPriceRDD).map(x=>(x._2._1._1,x._2._1._2+"|"+x._2._1._3+"|"+x._2._1._4+"|"+x._2._2.getOrElse(-999)))


    val deviceRDD = deviceTmpRDD.map{x =>
      val user_id = x._1
      var priceList = new util.ArrayList[String]
      var crmList: List[String] = List()
      var strArr: Array[String] = x._2.split(",")
      for (i <- 0 to (strArr.length - 1)) {
        val split: Array[String] = strArr(i).split("\\|") //ageband|datacomsuption|arpu|device_price
        priceList.add(split(3))
        val crminfo = split(0)+"|"+split(1)+"|"+split(2)
        crmList = crmList :+ crminfo
      }

      var maxPrice = 0.0
      for (i <- 0 to priceList.size() - 1) {
        val varPrice = priceList.get(i).toDouble
        if(varPrice > maxPrice){
          maxPrice = varPrice
        }
      }
      val crminfo = crmList.groupBy(identity).maxBy(_._2.size)._1

      (user_id,crminfo+"|"+maxPrice)
    }


    //deviceprice, key-->TAC, value-->price
    //val deviceprice = sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/deviceTypeInfo.txt").map(_.split("\\t")).map(x=>(x(0),x(1)))

    //key-->user_id, value-->ageband|datacomsumption|arpu|deviceprice
    //val crmFinal = crmRDD.leftOuterJoin(deviceprice).map(x=>(x._2._1._1,x._2._1._2+"|"+x._2._1._3+"|"+x._2._1._4+"|"+"|"+x._2._2.getOrElse("-999")))

    deviceRDD.map(x=>x._1+"|"+x._2).coalesce(16).saveAsTextFile(resultDir)
  }
}
