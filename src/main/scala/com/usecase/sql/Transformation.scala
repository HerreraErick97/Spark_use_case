package com.usecase.sql

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Transformation {

  def addLiteralColumn(data: DataFrame): DataFrame = {
    data.select(col("*"), lit(1).alias("literal"))
  }

  def createTop(data: DataFrame): DataFrame = {
    val ventana = Window.orderBy(col("records").desc)
    val total = data.count()
    val dfTop = data
      .select("noc").groupBy("noc")
      .agg(count("noc").as("records"))
      .withColumn("total_records", lit(total))
    dfTop.withColumn("rank", row_number().over(ventana))

  }

  def getTableFiltered(data: DataFrame): DataFrame = {
    data.filter(col("noc") === "Japan")
  }

  def createPercent(data: DataFrame): DataFrame = {
    data.withColumn("percent", round(col("records").divide(col("total_records")).multiply(100), 10  ))
  }

  def createMessage(data: DataFrame): DataFrame = {
    val rank_medals = data.first().getAs[Int]("rank")
    val number_medals = data.first().getAs[Int]("total")
    data.withColumn("message", lit(s"Japan in place $rank_medals with $number_medals won medals!"))
  }

  def joinObjects(data1: DataFrame, data2: DataFrame, data3: DataFrame): DataFrame ={
     val dfAthletes = data1.select("records", "percent", "noc", "rank")
       .withColumnRenamed("records", "athletes_number")
       .withColumnRenamed("percent", "athletes_percent_number")
       .withColumnRenamed("rank", "rank_athletes_number")

    val dfCoaches = data2.select("records", "percent", "noc", "rank")
      .withColumnRenamed("records", "coaches_number")
      .withColumnRenamed("percent", "coaches_percent_number")
      .withColumnRenamed("rank", "rank_coaches_number")

    val dfMedals = data3.select("total", "message", "noc")
      .withColumnRenamed("total", "medals_number")

    val join = dfAthletes.join(dfCoaches,"noc").join(dfMedals, "noc")

    join.select("athletes_number","athletes_percent_number",
      "coaches_number","coaches_percent_number",
      "medals_number", "message", "noc",
      "rank_athletes_number", "rank_coaches_number")


  }

  
}
