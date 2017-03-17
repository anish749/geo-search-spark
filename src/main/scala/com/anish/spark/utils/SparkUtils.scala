package com.anish.spark.utils

import com.anish.spark.geosearch.Search._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by anish on 12/03/17.
  */
object SparkUtils {

  /**
    * Returns the SparkSession object. Manages the configs for the spark session
    *
    * @return
    */
  def getSparkSession: SparkSession = {
    val sparkConf = new SparkConf
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[3]")
    }
    if (!sparkConf.contains("spark.app.name")) {
      sparkConf.setAppName("GeoSearchSpark-" + getClass.getName)
    }
    SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()
  }
}
