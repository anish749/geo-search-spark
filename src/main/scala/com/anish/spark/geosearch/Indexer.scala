package com.anish.spark.geosearch

import com.anish.spark.utils.SparkUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * This class is used to create / update the index (the available latitude and longitudes to map)
  * Before the first run the index must be created and then loaded everytime the job runs.
  *
  * Created by anish on 12/03/17.
  */
object Indexer {
  private val indexFilePath = "index/"
  private val inputLatLonPath = "input/availableLatLon"
  private val delimiterInAvailableLatLon = ";"

  /**
    * Main function. Creates the index and stores it in given location
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.getSparkSession
    println("Reading index from " + inputLatLonPath + " and storing to " + indexFilePath)
    createIndex(sparkSession, inputLatLonPath, indexFilePath, delimiterInAvailableLatLon)

    println("Index successfully created and stored in : " + indexFilePath)
  }

  /**
    * Create the index from available latitude longitude values. This index is stored in a HDFS path specified as
    * indexFilePath. This operation is done only once or as an when new latitude longitudes are added / removed or
    * updated. This is not executed as part of every Spark Job.
    *
    * @param sparkSession
    * @param inputLatLonPath
    * @param indexFilePath
    */
  def createIndex(sparkSession: SparkSession, inputLatLonPath: String, indexFilePath: String, delimiterInAvailableLatLon: String): Unit = {
    import sparkSession.implicits._
    sparkSession
      .read
      .option("delimiter", delimiterInAvailableLatLon)
      .csv(inputLatLonPath)
      .toDF("lat", "lon")
      .select($"lat".cast(DoubleType), $"lon".cast(DoubleType))
      .filter($"lat" >= -90.0 && $"lat" <= 90.0)
      .filter($"lon" >= -180.0 && $"lon" <= 180.0) // Filter invalid lat and lons
      .coalesce(1) // distributed sorting is not used since the index would be fairly small in size
      .sort(asc("lat"), asc("lon"))
      .write
      .mode(SaveMode.Overwrite)
      .csv(indexFilePath)
  }
}
