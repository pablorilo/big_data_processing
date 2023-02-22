package io.keepcoding.spark.exercice.streaming
import io.keepcoding.spark.exercise.streaming.{AntennaMessage, StreamingJob}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, dayofmonth, from_json, hour, lit, month, sum,  window, year}
import org.apache.spark.sql.types.{StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object bytesStreaming extends StreamingJob{

  override val spark: SparkSession = SparkSession
    .builder()
    .master("local[20]")
    .appName("Práctica Final")
    .getOrCreate()

  override def readFromKafka(kafkaServer: String, topic: String): DataFrame= {
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("subscribe", topic)
      .load()
  }
  override def parserJsonData(dataFrame: DataFrame): DataFrame = {

    val antennaMessageSchema: StructType = ScalaReflection.schemaFor[AntennaMessage].dataType.asInstanceOf[StructType]
      import spark.implicits._

    dataFrame
      .select(from_json(col("value").cast(StringType), antennaMessageSchema).as("json"))
      .select("json.*")
      .withColumn("timestamp",$"timestamp".cast(TimestampType))// probablemente no haga falta al pillar el squema de antennaMessageSchema

  }

 override def computeDevicesCountBy(dataFrame: DataFrame, device: String) : DataFrame = {
    import spark.implicits._
    val aggColumn = if (device == "id") "user" else if (device == "antenna_id") "antenna" else if(device == "app") "app"

    dataFrame
      .select($"timestamp",col(device),$"bytes")
      .withWatermark("timestamp", "1 minutes")
      .groupBy(col(device), window($"timestamp", "5 minutes"))
      .agg(
        sum($"bytes").as("value")
      )
      .select($"window.start".as("timestamp"),col(device).as("id"), $"value", lit(s"${aggColumn}_total_bytes").as("type"))
  }

 override def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit] = Future {
   dataFrame
     .writeStream
     .foreachBatch { (data: DataFrame,BatchId:Long) =>
       data
         .write
         .mode(SaveMode.Append)
         .format("jdbc")
         .option("driver", "org.postgresql.Driver")
         .option("url", jdbcURI)
         .option("dbtable", jdbcTable)
         .option("user", user)
         .option("password", password)
         .save()
     }
     .start()
     .awaitTermination()
 }

  override def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit] = Future {
    import spark.implicits._
    dataFrame
      .select(
        $"timestamp",$"id",$"antenna_id",$"bytes",$"app",
        year($"timestamp").as("year"),
        month($"timestamp").as("month"),
        dayofmonth($"timestamp").as("day"),
        hour($"timestamp").as("hour")
      )
      .writeStream
      .format("parquet")
      .option("path", s"$storageRootPath/data")
      .option("checkpointLocation", s"$storageRootPath/checkpoint")
      .partitionBy("year", "month", "day", "hour")
      .start
      .awaitTermination()
    }


 def main(args: Array[String]): Unit = {
   //Datos servidores
   val kafkaServer = "34.76.100.193:9092"
   val topic = "devices"
   val jdbcURI = s"jdbc:postgresql://35.192.37.9:5432/postgres"
   val user = "postgres"
   val password = "keepcoding"
   //Rutas almacenamiento
   val storageRootPath = """C:\\Users\\prilo\\OneDrive\\Desktop\\KeepCoding\\BootCamp\\practicas\\BigData-Processing\\practica\\src\\main\\resources\\storage"""
   //tables
   val jdbcTableBytes = "bytes"
   //Groupby
   val groupByUser = "id"
   val groupByApp = "app"
   val groupByAntenna = "antenna_id"


   val kafkaDF = readFromKafka(kafkaServer, topic)
   val parseDF = parserJsonData(kafkaDF)
   val storageFurure = writeToStorage(parseDF, storageRootPath)
   val countUserDF = computeDevicesCountBy(parseDF, groupByUser)
   val countAppDF = computeDevicesCountBy(parseDF, groupByApp)
   val countAntennaDF = computeDevicesCountBy(parseDF, groupByAntenna)

   //Realizamos la carga de los DF en postgres
   val jdbcUserFuture = writeToJdbc(countUserDF, jdbcURI, jdbcTableBytes, user, password)
   val jdbcAppFuture = writeToJdbc(countAppDF, jdbcURI, jdbcTableBytes, user, password)
   val jdbcAntennaFuture = writeToJdbc(countAntennaDF, jdbcURI, jdbcTableBytes, user, password)


   Await.result(
     Future.sequence(Seq(storageFurure, jdbcUserFuture, jdbcAppFuture, jdbcAntennaFuture)), Duration.Inf) //

   spark.close()


 }
}
