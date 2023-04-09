package bookExercises

import org.apache.spark.sql.SparkSession


object Schemas extends App{
  val spark = SparkSession
    .builder()
    .appName("Schemas and DataFrames")
    .config("spark.master", "local")
    .getOrCreate()
}
