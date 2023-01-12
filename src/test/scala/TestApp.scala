import com.typesafe.config.Config
import com.usecase.sql.Transformation
import com.usecase.utils.LoadConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

import java.util

class TestApp extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("example")
    .getOrCreate()

  val conf:Config = LoadConf.getConfig
  def readParquet(input: String): DataFrame = {
    println(">>>>>" + conf)
    println(">>>>>" + conf.getString(input))
    spark.read.parquet(conf.getString(input))
  }

  test("Primer valor de la columna records sea 615") {
    val dfData = readParquet("input.pathAthletes")
    val dfTop = Transformation.createTop(dfData)
    val record = dfTop.first().getAs[Long]("records");
    //println(">>>>>" + record)
    assert(record == 615)
  }

  test("Que el noc sea igual a japan") {
    val dfData = readParquet("input.pathAthletes")
    val dfFilter = Transformation.getTableFiltered(dfData)
    val noc = dfFilter.first().getAs[String]("noc");
    //println(">>>>> " + noc)
    assert(noc === "Japan")
  }

  test("Que el tipo de dato de la columna percent sea Double") {
    val dfData = readParquet("input.pathAthletes")
    val dfTop = Transformation.createTop(dfData)
    val dfFilter = Transformation.getTableFiltered(dfTop)
    val dfPercent = Transformation.createPercent(dfFilter)
    val tipo = dfPercent.dtypes.find(_._1 == "percent").get._2
    //println(">>>>> " + tipo)
    assert(tipo === "DoubleType")
  }

  test("Que el valor de rank sea 3") {
    val dfData = readParquet("input.pathMedals")
    val dfFilter = Transformation.getTableFiltered(dfData)
    val dfMessage = Transformation.createMessage(dfFilter)
    val rank = dfMessage.first().getAs[Int]("rank")
    //println(">>>>> " + rank)
    assert(rank == 3)
  }

  test("Que el valor de medals number sea 58") {
    //input
    val dfAthletes = readParquet("input.pathAthletes")
    val dfCoaches = readParquet("input.pathCoaches")
    val dfMedals = readParquet("input.pathMedals")
    //createTop
    val topAthletes = Transformation.createTop(dfAthletes)
    val topCoaches = Transformation.createTop(dfCoaches)
    //Filter
    val athletesFiltered = Transformation.getTableFiltered(topAthletes)
    val coachesFiltered = Transformation.getTableFiltered(topCoaches)
    val medalsFiltered = Transformation.getTableFiltered(dfMedals)
    //Resume
    val athletesResume = Transformation.createPercent(athletesFiltered)
    val coachesResume = Transformation.createPercent(coachesFiltered)
    //message
    val dfMessage = Transformation.createMessage(medalsFiltered)
    //join
    val dfJoin = Transformation.joinObjects(athletesResume, coachesResume, dfMessage)

    val rank = dfJoin.first().getAs[Int]("medals_number")
    //println(">>>>> " + rank)
    assert(rank == 58)
  }



}
