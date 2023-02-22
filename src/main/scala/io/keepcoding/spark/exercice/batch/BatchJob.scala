package io.keepcoding.spark.exercise.batch

import io.keepcoding.spark.exercice.batch.BytesBatch.readFromStorage

import java.sql.Timestamp
import java.time.OffsetDateTime
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(year: Int, month: Int, day: Int, hour: Int, timestamp: Timestamp, id: String, metric: String, value: Long)

trait BatchJob {

  val spark: SparkSession

  def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame

  def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame

  def enrichUserWithMetadata(antennaDF: DataFrame, metadataDF: DataFrame): DataFrame

  def devicesCountBy(dataFrame: DataFrame, types: String):DataFrame

  def quotaUser(dataFrame: DataFrame):DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit


  /*def run(args: Array[String]): Unit = {
    val Array(filterDate, storagePath, jdbcURI,  jdbcMetadataTable, jdbcHourlyGroupTable, jdbcQuoteTable, user, password) = args
    println(s"Running with: ${args.toSeq}")

    val userDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readUserMetadata(jdbcURI, jdbcMetadataTable, user, password)
    val countUserDF = devicesCountBy(userDF, "app_total_bytes")
    writeToJdbc(countUserDF, jdbcURI, jdbcHourlyGroupTable, user, password)
    val enrichDF = enrichUserWithMetadata(countUserDF, metadataDF)
    val quotaDF = quotaUser(enrichDF)
    writeToJdbc(quotaDF, jdbcURI, jdbcQuoteTable, user, password)


    spark.close()
  }*/

}
