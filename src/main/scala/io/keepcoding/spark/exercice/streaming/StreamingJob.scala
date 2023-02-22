package io.keepcoding.spark.exercise.streaming

import io.keepcoding.spark.exercice.streaming.bytesStreaming.readFromKafka

import java.sql.Timestamp
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class AntennaMessage(timestamp: Timestamp, id: String, antenna_id: String, bytes: Long, app :String)

trait StreamingJob {

  val spark: SparkSession

  def readFromKafka(kafkaServer: String, topic: String): DataFrame

  def parserJsonData(dataFrame: DataFrame): DataFrame

  def computeDevicesCountBy(dataFrame: DataFrame, groupby : String): DataFrame

  def writeToJdbc(dataFrame: DataFrame, jdbcURI: String, jdbcTable: String, user: String, password: String): Future[Unit]

  def writeToStorage(dataFrame: DataFrame, storageRootPath: String): Future[Unit]

  def run(args: Array[String]): Unit = {
    val Array(kafkaServer, topic,  jdbcURI,user,password, storageRootPath,jdbcTableBytes,groupByUser,groupByApp,groupByAntenna) = args
        println(s"Running with: ${args.toSeq}")


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


