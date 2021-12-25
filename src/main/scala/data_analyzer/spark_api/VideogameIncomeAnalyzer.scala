package data_analyzer.spark_api

import data_analyzer.provider._
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object VideogameIncomeAnalyzer extends App
  with SparkSessionProviderComponent
  with VideogameIncomeAnalyzerDataset {

  override def sparkSessionProvider = new DefaultSparkSessionProvider("IncomeAnalyzer")

  genreIncome("vgsales.csv")
  platformIncome("vgsales.csv")

}

trait VideogameIncomeAnalyzerDataset {
  this: SparkSessionProviderComponent =>

  private val sparkSession = sparkSessionProvider.sparkSession


  def genreIncome(path: String): Unit = {

    val dfGenreIncome: DataFrame = sparkSession.read
      .option("header", "true")
      .csv(path)

    val columnToSum = List(dfGenreIncome("NA_Sales"), dfGenreIncome("EU_Sales"), dfGenreIncome("JP_Sales"), dfGenreIncome("Other_Sales"))

    val genreIncome = dfGenreIncome
      .withColumn("allRegionSales($M)", round(columnToSum.reduce(_ + _)).cast(IntegerType))
      .groupBy("Genre")
      .agg(sum("allRegionSales($M)").as("allRegionSales($M)"))
      .select("Genre", "allRegionSales($M)")
      .sort(desc("allRegionSales($M)"))

    genreIncome.show(genreIncome.count.toInt)
  }
  def platformIncome(path: String): Unit = {

    val dfGenreIncome: DataFrame = sparkSession.read
      .option("header", "true")
      .csv(path)

    val columnToSum = List(dfGenreIncome("NA_Sales"), dfGenreIncome("EU_Sales"), dfGenreIncome("JP_Sales"), dfGenreIncome("Other_Sales"))

    val platformIncome = dfGenreIncome
      .withColumn("allRegionSales($M)", round(columnToSum.reduce(_ + _)).cast(IntegerType))
      .groupBy("Platform")
      .agg(sum("allRegionSales($M)").as("allRegionSales($M)"))
      .select("Platform", "allRegionSales($M)")
      .sort(desc("allRegionSales($M)"))

    platformIncome.show(platformIncome.count.toInt)
  }
}