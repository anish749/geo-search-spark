package com.anish.spark.geosearch

import java.io.File

import com.anish.spark.SparkTestUtils
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

/**
  * Created by anish on 17/03/17.
  */
class IndexerTest extends FlatSpec with Matchers with BeforeAndAfter {

  var sparkSession: SparkSession = _

  before {
    sparkSession = SparkTestUtils.getSparkSession
  }

  behavior of "Indexer Application"
  it should "Create a sorted index out of the given geo coordinates" in {

    val inputLatLonPath = SparkTestUtils.getResourcePath("/availableLatLon/")
    val tmpIndexPath = "tmp_unitTestTemp_" + System.currentTimeMillis()

    Indexer.createIndex(sparkSession = sparkSession, inputLatLonPath = inputLatLonPath, indexFilePath = tmpIndexPath, delimiterInAvailableLatLon = "|")

    val expectedIndexPath = SparkTestUtils.getResourcePath("/expectedSortedIndex/")

    val expectedIndexDf = sparkSession.read.csv(expectedIndexPath)
    val actualIndexDf = sparkSession.read.csv(tmpIndexPath)

    SparkTestUtils.dfEquals(actualIndexDf, expectedIndexDf)


    // Delete temp output that was created
    val tmpOutput = new File(tmpIndexPath)
    tmpOutput.exists() shouldBe true
    if (tmpOutput.isDirectory) {
      FileUtils.deleteDirectory(tmpOutput)
      tmpOutput.exists() shouldBe false
    }

  }
}
