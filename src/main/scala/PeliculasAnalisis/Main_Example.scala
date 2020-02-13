package PeliculasAnalisis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main_Example {

  def main(args: Array[String])
  {
    val conf = new SparkConf().setAppName("Peliculas").setMaster("local").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    /*val rdd = sc.textFile("C:\\Users\\Gustavo Rangel\\Desktop\\Spark con Scala\\u.data")
    val coun = rdd.count()
    rdd.collect().foreach(println)
    println(s"$coun")*/

    val spark = SparkSession.builder().appName("Peliculas").master("local").getOrCreate()
    val df = spark.read.csv("C:\\Users\\Gustavo Rangel\\Desktop\\Spark con Scala\\heart.csv").toDF("age","sex","cp","trestbps","chol","fbs","restecg","thalach","exang","oldpeak","slope","ca","thal","target")
    val skipable_first_row = df.first()
    println(s"$skipable_first_row")
    val df1 = df.filter(row => row != skipable_first_row)
    df1.show()
    val coun = df1.count()
    println(s"$coun")
    filter(df)
  }

  def filter (dataFrame: DataFrame): Unit ={
    dataFrame.show
  }
}
