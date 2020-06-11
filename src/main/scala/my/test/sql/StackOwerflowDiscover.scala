package my.test.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StackOwerflowDiscover {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("StackOwerflowDiscover Application")
      .master("local[*]")
      .getOrCreate()

    val dataFrame: DataFrame = spark.read
      .option("header", true)
      .csv("src/resources/survey_results_public.csv")

    val time = System.currentTimeMillis()

    dataFrame.show(false)

    languageDesireNextYear(spark, dataFrame)
    //percentDevelopersByCountry(spark, dataFrame)
    //percentDevelopersByType(spark, dataFrame)

    println(s"Time Calculating: ${System.currentTimeMillis() - time}")

    // added just for looking at Spark UI 10 min
    Thread.sleep(600000)

    spark.close()

  }

  def languageDesireNextYear(spark: SparkSession, dataFrame: DataFrame): Unit = {

    import spark.implicits._

    val allLanguagesDF = dataFrame
      .flatMap(row => {
        row.getAs[String]("LanguageDesireNextYear").split(";").map((_, 1))
      })
    val count = allLanguagesDF.count()

    val res =
      allLanguagesDF
        .groupBy(col("_1").alias("Language"))
        .agg(sum(col("_2")).alias("Count"))
        .withColumn("Percent", (col("Count") / count * 100).cast("decimal(10,2)"))
        .sort(col("Count").desc)

    res.show(100, false)
  }

  def percentDevelopersByCountry(spark: SparkSession, dataFrame: DataFrame): Unit = {

    import spark.implicits._

    val count = dataFrame.count()

    val res =
      dataFrame
        .map(row => (row.getAs[String]("Country"), 1))
        .groupBy(col("_1").alias("Country"))
        .agg(sum(col("_2")).alias("Count"))
        .withColumn("Percent", (col("Count") / count * 100).cast("decimal(10,2)"))
        .sort(col("Count").desc)

    res.show(100, false)
  }

  def percentDevelopersByType(spark: SparkSession, dataFrame: DataFrame): Unit = {

    import spark.implicits._

    val allDevs = dataFrame
      .flatMap(row => {
        row.getAs[String]("DevType").split(";").map((_, 1))
      })
    val count = allDevs.count()

    val res =
      allDevs
        .groupBy(col("_1").alias("DevType"))
        .agg(sum(col("_2")).alias("Count"))
        .withColumn("Percent", (col("Count") / count * 100).cast("decimal(10,2)"))
        .sort(col("Count").desc)

    res.show(100, false)
  }

}
