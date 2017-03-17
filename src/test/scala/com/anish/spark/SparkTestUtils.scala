package com.anish.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.Matchers

/**
  * Created by anish on 17/03/17.
  */
object SparkTestUtils extends Matchers {

  /**
    * Spark Session object. This class acts as a Singleton for the sparkSession being used by multiple test cases.
    */
  private var sparkSession: SparkSession = _

  /**
    * Returns the SparkSession object. Used to maintain one Spark Session across all test sessions.
    *
    * @return
    */
  def getSparkSession = {

    if (sparkSession == null || sparkSession.sparkContext.isStopped) {
      sparkSession = SparkSession
        .builder
        .master("local[3]")
        .appName("SparkUnitTest_" + getClass.getName)
        .getOrCreate()
    }
    sparkSession
  }

  /**
    * Gets absolute file path of a resource.
    *
    * @param pathInResource
    * @return actual path of file
    */
  def getResourcePath(pathInResource: String): String = {
    getClass.getResource(pathInResource).getPath
  }

  /**
    * Compares two dataframes and ensures that they have the same schema (ignore nullable) and the same values
    * This collects both data frames in the driver, thus not suitable for very large test data.
    *
    * @param actualDF   The DF we want to check for correctness
    * @param correctDF  The correct DF we use for comparison
    * @param onlySchema only compare the schemas of the dataframes
    */
  def dfEquals(actualDF: DataFrame, correctDF: DataFrame, onlySchema: Boolean = false): Unit = {
    actualDF.schema.map(f => (f.name, f.dataType)).toSet shouldBe correctDF.schema.map(f => (f.name, f.dataType)).toSet
    if (!onlySchema) {
      actualDF.collect.map(_.toSeq.toSet).toSet shouldBe correctDF.collect.map(_.toSeq.toSet).toSet
    }
  }
}
