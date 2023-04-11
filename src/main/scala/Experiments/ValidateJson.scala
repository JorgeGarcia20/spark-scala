package Experiments

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.databricks.spark.xml._
import org.json4s
import org.json4s.jackson.JsonMethods._

object ValidateJson extends App{

  val spark = SparkSession
    .builder()
    .appName("Validating JSONs")
    .master("local[*]")
    .getOrCreate()

  val df = spark.read.option("rootTag", "data").option("rowTag", "person").xml("src/data/person.xml")
  //df.show(false)
  //df.printSchema()
  //println(df.schema)

  val jsonDF = df.toJSON.first()
  //println(jsonDF)

  case class Person(id: String, name: String, age: Int, genre: String, country: String)

  val json = parse(jsonDF)
  val prettyJson = pretty(json).toString


  println(prettyJson.getClass.getName)


  jsonDF match {
    case Some(map: Map[String, Any]) =>
      val id = map.getOrElse("_id", "").asInstanceOf[String]
      val name = map.getOrElse("name", "").asInstanceOf[String]
      val age = map.getOrElse("age", "0").asInstanceOf[Int]
      val genre = map.getOrElse("genre", "").asInstanceOf[String]
      val country = map.getOrElse("country", "").asInstanceOf[String]

      val person = Person(id, name, age, genre, country)

      person match {
        case Person(id, name, age, genre, country) if id.length == 18 =>
          println("Valid")
        case _ => println("Invalid")
      }
  }

  /*
  json match {
    case JObject(fields) => println("JSON valido: " + pretty(json))
    case _ => println("JSON invalido")
  }
   */


  spark.close()
}
