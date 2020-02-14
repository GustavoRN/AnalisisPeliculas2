package PeliculasAnalisis

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._

object Main_Example {

  def main(args: Array[String])
  {
    val spark = SparkSession.builder().appName("Corazon").master("local").getOrCreate()
    val ds:Dataset[String] = spark.read.textFile("C:\\Users\\Gustavo Rangel\\IdeaProjects\\PeliculasAnalisis\\Analisis\\src\\main\\scala\\resources\\u.data")
    val df = ds.withColumn("value", split(col("value"), "\t")).select((0 until 4).map(i => col("value").getItem(i).as(s"col$i")): _*).toDF("movie_id","user_id","movie_rating","release_date")
    val skipable_first_row = df.first()
    val df1 = df.filter(row => row != skipable_first_row)
    //df1.show()
    val fil = df1.groupBy("movie_id").count()
    val res = fil.orderBy(desc("count")).toDF("movie_id", "# Vistas")
    res.show(10)

  }
}
