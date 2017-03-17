package com.anish.spark.geosearch

import java.io.File

import com.anish.spark.SparkTestUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

//import org.junit.runner.RunWith
//import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Unit test class for the Search Class. This creates a SparkContext while evaluating the test cases
  *
  * Created by anish on 17/03/17.
  */
//@RunWith(classOf[JUnitRunner]) // Runs this test with surefire
class SearchTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sparkSession: SparkSession = _

  before {
    sparkSession = SparkTestUtils.getSparkSession
  }

  behavior of "Search Application"
  it should "Map given Geo Coordinates to nearest available coordinates" in {
    val unknownLatLonPath = SparkTestUtils.getResourcePath("/unknownLatLon/unknownLatLon.txt")
    val tmpOutputPath = "tmp_unitTestTemp_" + System.currentTimeMillis()
    val latLimit = 0.5
    val lonLimit = 4.0
    val maxDistMiles = 30.0
    Search.runSparkBatchSearch(indexFilePath = "index/", unknownLatLonPath = unknownLatLonPath,
      outputPath = tmpOutputPath, latLimit = latLimit, lonLimit = lonLimit, maxDistMiles = maxDistMiles)

    val expectedMappedDF = sparkSession.read.csv(SparkTestUtils.getResourcePath("/expectedMappedDataFrame"))
    val actualMappedDF = sparkSession.read.csv(tmpOutputPath)

    SparkTestUtils.dfEquals(actualMappedDF, expectedMappedDF)

    // Delete temp output that was created
    val tmpOutput = new File(tmpOutputPath)
    tmpOutput.exists() shouldBe true
    if (tmpOutput.isDirectory) {
      FileUtils.deleteDirectory(tmpOutput)
      tmpOutput.exists() shouldBe false
    }
  }
}
