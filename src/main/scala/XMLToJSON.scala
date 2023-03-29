import com.databricks.spark.xml._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s.jackson.JsonMethods.mapper

import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps

object XMLToJSON {
  def main(main: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("XMLToJSON")
      .config("spark.master", "local")
      .getOrCreate()

    val xmlPath = "src/data/nestedxml.xml"
    val jsonsPath = "src/data/jsons/"

    val df = spark.read
      .option("rowTag", "items")
      .xml(xmlPath)

    df.show()
    df.printSchema()

    // val booksJSON = booksDF.toJSON
    /*df.write
      .format("json")
      .option("multiLine", "true")
      .option("overwrite", "true")
      .save(jsonsPath)
     */

    // Validando que la estructura del JSON sea correcta

    //val jsonString = Source.fromFile("/home/jorge/IdeaProjects/sparkGettingStarted/src/data/jsons/part-00000-541365c4-2b7c-49e5-abfc-c379f0913fdd-c000.json").mkString
    //val json = parse(jsonString)

    //json match {
      //case JObject(fields) => println("JSON valido: " + pretty(json))
      //case _ => println("JSON invalido")
    //}

    //println(jsonString)

    spark.close()
  }
}
