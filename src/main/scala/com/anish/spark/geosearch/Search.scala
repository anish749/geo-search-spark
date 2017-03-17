package com.anish.spark.geosearch

import com.anish.spark.geosearch.models.Coordinate
import com.anish.spark.utils.SparkUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * Created by anish on 14/03/17.
  */
object Search {

  private val indexFilePath = "index/"

  private val unknownLatLonPath = "input/unknownLatLon"
  private val outputPath = "output/"

  def main(args: Array[String]): Unit = {
    val latLimit = 0.5
    val lonLimit = 4.0
    val maxDistMiles = 30.0
    runSparkBatchSearch(indexFilePath, unknownLatLonPath, outputPath, latLimit, lonLimit, maxDistMiles)
  }

  /**
    * Runs a batch job using Spark to search for the lat longs. All inputs are HDFS / S3 or local paths read by Spark.
    *
    * @param indexFilePath     Path where sorted index is stored. This is the output of the Indexer program.
    * @param unknownLatLonPath Path to unknown Geo Coordinates which are to be searched.
    * @param outputPath        Path where the mapped output is stored.
    */
  def runSparkBatchSearch(indexFilePath: String, unknownLatLonPath: String, outputPath: String, latLimit: Double, lonLimit: Double, maxDistMiles: Double): Unit = {
    val sparkSession = SparkUtils.getSparkSession
    import sparkSession.implicits._

    val indexBc = loadIndex(sparkSession, indexFilePath)
    val unknownLatLon: Dataset[Coordinate] = sparkSession.read.textFile(unknownLatLonPath).as[String].map(toCoordinate)

    // search to find the mapped Dataset
    val mappedLatLon = search(sparkSession, indexBc, unknownLatLon, latLimit, lonLimit, maxDistMiles)

    // Save data to given output path
    mappedLatLon.write.mode(SaveMode.Overwrite).csv(outputPath)
  }

  /**
    * The core search logic implementation. This uses a broad casted sorted List of Coordinate objects as an index.
    * The logic implemented in this method first goes through the latitude and finds the nearest matching latitude
    * within specified limits using Binary Search. Then it filters for the longitudes within the longitude limits.
    * Now it applies linear search to find the available coordinate with the smallest distance which also should be
    * less than the max search radius specified.
    *
    * @param sparkSession  The spark session. This is used for importing spark implicits
    * @param indexBc       Available latitude longitude Spark Broadcast variable
    * @param unknownLatLon A spark Dataset of the Coordinates that are to be mapped.
    * @return A DataSet of Row, which has all given Coordinates mapped to available Coordinates. (null for those which
    *         were not mapped)
    */
  def search(sparkSession: SparkSession, indexBc: Broadcast[List[Coordinate]], unknownLatLon: Dataset[Coordinate], latLimit: Double, lonLimit: Double, maxDistMiles: Double): Dataset[Row] = {
    import sparkSession.implicits._
    val idx = indexBc.value

    // For each Coordinate in Dataset[Coordinate] the lookup should find one coordinate from the index of available coordinates
    val mapper = (searchLat: Double, searchLon: Double) => {
      // Mapper function which maps one search Coordinate to one of the available or returns a null

      // implementation of binary search
      // Step 1 - Get the limits in which the lat can fall
      def bsLat(low: Int, hi: Int): (Int, Int) = {
        if (low >= hi) return (low, hi)
        val mid = (hi + low) / 2
        idx match {
          case a: List[Coordinate] if searchLat - latLimit > a(mid).lat => bsLat(mid + 1, hi)
          case a: List[Coordinate] if searchLat + latLimit < a(mid).lat => bsLat(low, mid - 1)
          // mid value is almost correct. Now refine
          case a: List[Coordinate] if math.abs(a(mid).lat - searchLat) < latLimit => {
            a match {
              case b if searchLat - latLimit >= b((low + mid) / 2).lat => bsLat((low + mid) / 2, hi)
              case _ => None
            }
            a match {
              case b if searchLat + latLimit <= b((mid + hi) / 2).lat => bsLat(low, (mid + hi) / 2)
              case _ => (low, hi)
            }
          }
          case _ => (low, hi)
        }
      }
      val (low, hi) = bsLat(0, idx.size - 1)
      // currently hi = the max index after which the lat will exceed the
      // lat limit and low = the min index beyond which the lad will
      // exceed the lat limit.

      // Step 2 - Filter long values and find min distance to map searched
      // location
      // Binary Search would not work on this, because the lon values are sorted only for exactly equal lats
      // Take slice of the index based on lats where the nearest Coordinate will lie
      val slicedIdx = idx.slice(low, hi + 1)

      // 2 cases - if search long is near international date line,
      // take abs value. Then filter and narrow down search based on
      // longitude. else ignore
      val lonFilteredIdx = slicedIdx.filter(i => ((searchLon >= 180 - lonLimit || searchLon <= -180 + lonLimit)
        || (i.lon <= searchLon + lonLimit
        && i.lon >= searchLon - lonLimit)))

      // Calculate distance for all indices left and take the min one
      lonFilteredIdx match {
        case a if lonFilteredIdx.nonEmpty => // Some values exists for which the distance has to be calculated
          val search = Coordinate(searchLat, searchLon)
          val nearestCoordinateDistTuple = lonFilteredIdx
            .map(c => (c, geoDist(c, search))) // Calculate distance for all points
            .reduce((a, b) => if (a._2 < b._2) a else b) // Find min dist
          if (nearestCoordinateDistTuple._2 < maxDistMiles) // Check if it stills falls within the max dist limit
            Some(nearestCoordinateDistTuple._1)
          else
            None
        case _ => // No match found
          None
      }

    }

    val mapper_udf = udf(mapper)
    val mappedLatLon: Dataset[Row] = unknownLatLon.withColumn("_mappedCoordinate", mapper_udf($"lat", $"lon"))
      // Break the Struct returned by the udf into normal columns
      .select($"lat", $"lon", $"_mappedCoordinate.lat".as("mappedLat"), $"_mappedCoordinate.lon".as("mappedLon"))

    mappedLatLon
  }

  /**
    * Read the index of available latitudes and longitudes and broadcast them.
    *
    * Since Spark reader is used for reading, the index File can be present in HDFS / S3 or any distributed file system
    * supported by Spark.
    *
    * @param indexFilePath
    */
  def loadIndex(sparkSession: SparkSession, indexFilePath: String) = {
    val index = sparkSession.sparkContext
      .wholeTextFiles(indexFilePath, minPartitions = 1)
      .coalesce(1) // Force into 1 partition to maintain sorted order
      .flatMap(_._2.split("\n"))
      .map(toCoordinate)
      .collect
      .toList

    sparkSession.sparkContext.broadcast(index)
  }

  /**
    * Utility method to convert a pair of Latitude and Longitude to a Coordinate Object
    *
    * @param pair
    * @return Coordinate object
    */
  def toCoordinate(pair: String): Coordinate = {
    val spl = pair.split(",")
    Coordinate(spl(0).toDouble, spl(1).toDouble)
  }

  /**
    * Haversine Geo Distance formula to calculate geo-distance between two
    * points.
    *
    * @param ilat1
    * @param ilon1
    * @param ilat2
    * @param ilon2
    * @return Distance between point (ilat1,ilon1) and (ilat2,ilon2)
    */
  private def geoDist(ilat1: Double, ilon1: Double, ilat2: Double, ilon2: Double): Double = {
    val long2 = ilon2 * math.Pi / 180
    val lat2 = ilat2 * math.Pi / 180
    val long1 = ilon1 * math.Pi / 180
    val lat1 = ilat1 * math.Pi / 180

    val dlon = long2 - long1
    val dlat = lat2 - lat1
    val a = math.pow(math.sin(dlat / 2), 2) + math.cos(lat1) * math.cos(lat2) * math.pow(math.sin(dlon / 2), 2)
    val c = 2 * math.atan2(Math.sqrt(a), math.sqrt(1 - a))
    val result = 3961 * c
    // for kilometers, use 6373 instead
    result
  }

  /**
    * Distance between two Coordinate Objects
    *
    * @param c1
    * @param c2
    * @return
    */
  private def geoDist(c1: Coordinate, c2: Coordinate): Double = {
    geoDist(c1.lat, c1.lon, c2.lat, c2.lon)
  }

}
