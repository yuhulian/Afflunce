package com.simon

import org.apache.spark.rdd._

import scala.math._

/**
  * Created by simon on 2017/8/15.
  *  Median
  *  IQR  = Q3 - Q1 (third quartile - fisrt quartile)
  *  outlier >= Q3 + 3*IQR (UOF) || outlier <= Q1 - 3*IQR (LOF)
  *  suspected outlier >= Q3 + 1.5*IQR (UAV) || suspected outlier <= Q1 - 1.5*IQR (LIF)
  * Q1 = val(n+1)/4
  * Q2 = if odd: (val(n)+1)/2 even:[val(n/2) + val(n/2+1)]/2
  */

class QuarterPercentile(val vec:RDD[Double],val count:Long) extends java.io.Serializable{
   private val sorted = vec.sortBy(identity).zipWithIndex().map {  //By using identity, which is a "nice" way to say x => x,zipWithIndex give each element an index to make a pair of (value,index)
    case (value, index) => (index, value)
  }
  val median: Double = this.getMedian
  val q1: Double = this.getFirstQuarterPercentile
  val q3: Double = this.getThirdQuarterPercentile
  val uav: Double = this.getUAV
  val lif: Double = this.getLIF
  val uof: Double = this.getUOF
  val lof: Double = this.getLOF
  val iqr: Double = this.getIQR

  def getMedian:Double = {
    val median:Double = if (count % 2 == 0){
      val low = count / 2 -1  //start index from zero, so we need to minus 1 for the index
      val high = low + 1
      (sorted.lookup(low).head + sorted.lookup(high).head).toDouble / 2
    }else
      sorted.lookup(count / 2).head.toDouble
    median
  }

  def getFirstQuarterPercentile:Double = {
    val q1:Double = if (count % 4 == 0){
      val firtQuarterIndex = count / 4 - 1
      sorted.lookup(firtQuarterIndex).head.toDouble
    }else{
      val fisrtQuarterIndex: Double = count / 4.0
      val f = floor _
      val c = ceil _
      val floorIndex: Double = f(fisrtQuarterIndex) - 1
      val ceilIndex: Double = c(fisrtQuarterIndex) - 1
      val deci: Double = fisrtQuarterIndex - floorIndex //小数部分
      sorted.lookup(floorIndex.toLong).head.toDouble * (1 - deci) + sorted.lookup(ceilIndex.toLong).head.toDouble * deci
    }
    q1
  }
  @transient
  def getThirdQuarterPercentile = {
    val q3:Double = if (count % 4 == 0){
      val thirdQuarterIndex = 3 * count / 4 - 1
      sorted.lookup(thirdQuarterIndex).head.toDouble
    }else{
      val thirdQuarterIndex: Double = 3 * count / 4.0
      val f = floor _
      val c = ceil _
      val floorIndex: Double = f(thirdQuarterIndex) - 1
      val ceilIndex: Double = c(thirdQuarterIndex) - 1
      val deci: Double = thirdQuarterIndex - floorIndex //小数部分
      sorted.lookup(floorIndex.toLong).head.toDouble * (1 - deci) + sorted.lookup(ceilIndex.toLong).head.toDouble * deci
    }
    q3
  }

  def getIQR: Double = q3 - q1

  def getUAV: Double = q3 + (q3 - q1) * 1.5

  def getLIF: Double = q1 - (q3 - q1) * 1.5

  def getUOF: Double = q3 + (q3 - q1) * 3.0

  def getLOF: Double = q1 - (q3 - q1) * 3.0

  def replaceUpperOutlier:RDD[Double] = {
    val newvec:RDD[Double] = vec.map(x=>{
      var feature = x
      if(feature > uof)
        feature = uof
      feature
    })
    newvec
  }

  def replaceLowerOutlier:RDD[Double] = {
    val newvec:RDD[Double] = vec.map(x=>{
      var feature = x
      if(feature < lof)
        feature = lof
      feature
    })
    newvec
  }

  def replaceOutlier:RDD[Double] = {
    val newvec:RDD[Double] = vec.map(x=>{
      var feature = x
      if(feature < lof)
        feature = lof
      else if(feature > uof)
        feature = uof
      feature
    })
    newvec
  }

  def replaceMissingRecord:RDD[Double] = {
    val newvec:RDD[Double] = vec.map(x=>{
      var feature = x
      if(Math.abs(x + 999) < 0.00001)
        feature = median
      feature
    })
    newvec
  }
}
