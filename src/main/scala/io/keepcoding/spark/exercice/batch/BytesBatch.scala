package io.keepcoding.spark.exercice.batch
import io.keepcoding.spark.exercise.batch.BatchJob
import org.apache.spark.sql.functions.{col, lit, sum, window}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.time.OffsetDateTime

object BytesBatch extends BatchJob{
  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Práctica final Batch")
    .getOrCreate()

  import spark.implicits._

  override def readFromStorage(storagePath: String, filterDate: OffsetDateTime): DataFrame = {
    spark
      .read
      .format("parquet")
      .load(s"${storagePath}")
      .filter(
        $"year" === filterDate.getYear &&
          $"month" === filterDate.getMonthValue &&
          $"day" === filterDate.getDayOfMonth &&
          $"hour" === filterDate.getHour
      )
  }

   def readUserMetadata(jdbcURI: String, jdbcTable: String, user: String, password: String): DataFrame = {
    spark
      .read
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .load()
  }

  override def devicesCountBy(dataFrame: DataFrame, column: String): DataFrame = {
    val aggColumn:String = if (column == "id") "user_total_bytes" else if (column == "antenna_id") "antenna_total_bytes" else if (column == "app")  "app_total_bytes" else "error"

    dataFrame
      .select($"timestamp",col(column),$"bytes")
      .groupBy(col(column), window($"timestamp", "1 hour"))
      .agg(sum($"bytes").alias("value"))
      .select(($"window.start").as("timestamp"), col(column).as("id"), $"value", lit(aggColumn).as("type"))
  }

  override def enrichUserWithMetadata(userDF: DataFrame, metadataDF: DataFrame): DataFrame = {
    import spark.implicits._
    userDF.as("user")
      .join(
        metadataDF.as("metadata"),
        $"metadata.id" === $"user.id"
      )
      .drop($"metadata.id")
  }

  override def quotaUser(dataFrame: DataFrame):DataFrame = {
    dataFrame
      .filter($"value">$"quota")
      .select($"email", $"value".as("usage"),$"quota",$"timestamp")
  }

  override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Unit = {
    dataFrame
      .write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcURI)
      .option("dbtable", jdbcTable)
      .option("user", user)
      .option("password", password)
      .save()


  }

  def main(args: Array[String]): Unit = {
    val storagePath = """C:\Users\prilo\OneDrive\Desktop\KeepCoding\BootCamp\practicas\BigData-Processing\practica\src\main\resources\storage\data"""
    val sqlServer = "35.192.37.9"
    val user = "postgres"
    val password = "keepcoding"
    val jdbcURI = s"jdbc:postgresql://$sqlServer/postgres"
    val jdbcMetadataTable = "user_metadata"
    val jdbcHourlyGroupTable = "bytes_hourly"
    val jdbcQuoteTable = "user_quota_limit"
    val filterDate = "2022-10-29T17:00:00Z"
    val UserColumn = "id"
    val appColumn = "app"
    val antennaColumn = "antenna_id"

    val deviceDF = readFromStorage(storagePath, OffsetDateTime.parse(filterDate))
    val metadataDF = readUserMetadata(jdbcURI, jdbcMetadataTable, user, password)
    val countUserDF = devicesCountBy(deviceDF, UserColumn)
    val countAppDF = devicesCountBy(deviceDF, appColumn)
    val countAntennaDF = devicesCountBy(deviceDF, antennaColumn)

    val enrichDF = enrichUserWithMetadata(countUserDF, metadataDF)
    val quotaDF = quotaUser(enrichDF)

    writeToJdbc(countUserDF, jdbcURI, jdbcHourlyGroupTable, user, password)
    writeToJdbc(countAppDF, jdbcURI, jdbcHourlyGroupTable, user, password)
    writeToJdbc(countAntennaDF, jdbcURI, jdbcHourlyGroupTable, user, password)
    writeToJdbc(quotaDF, jdbcURI, jdbcQuoteTable, user, password)
  }

}

