package com.simon

/**
  * Created by simon on 2017/8/31.
  */
class attrDevicePrice {
//  def main(args: Array[String]): Unit = {
//    val humanList = sc.textFile("/user/ss_deploy/workspace/crm/2017/06/011/result/p*").map(_.split("\\|")).map(x=>)
//    val userDeviceRDD = sc.textFile("/user/ss_deploy/device_info/"+ year+month+ "/"+ province +"/part*").map(_.split("\\|")).map(x=>(x(3),x(0)))
//
//    val deviceRDD= sc.textFile("/user/ss_deploy/workspace/simon/spendingpower/input/deviceTypeInfo.txt").map(_.split("\\t")).map(x=>(x(0),x(1)))
//    val deviceTmpRDD = userDeviceRDD.leftOuterJoin(deviceRDD).map(x=>x._2._1+"|"+x._2._2.getOrElse(-1))
//
//    val deviceRDD = deviceTmpRDD.map{x =>
//      val user_id = x._1
//      var priceList = new util.ArrayList[String]
//      var strArr: Array[String] = x._2.split(",")
//      for (i <- 0 to (strArr.length - 1)) {
//        priceList.add(strArr(i))
//      }
//      var maxPrice = 0.0
//      for (i <- 0 to priceList.size() - 1) {
//        val varPrice = priceList.get(i).toDouble
//        if(varPrice > maxPrice){
//          maxPrice = varPrice
//        }
//      }
//      (user_id,maxPrice)
//    }
//  }
}
