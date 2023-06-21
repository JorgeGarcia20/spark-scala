package XMLToJson
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import java.io.PrintWriter
import java.io.File

object Main extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  val xmlPath = "src/data/nestedxml.xml"
  val rootTag = "items"
  val rowTag = "item"

  val df = spark.read.option("rootTag", rootTag).option("rowTag", rowTag).xml(xmlPath)

  df.printSchema()
  val jsonPath = "src/data/jsons/items"
  df.coalesce(1).write.json(jsonPath)

  //df.show()
  /*
  val dfJsonString  = df.toJSON.first().replace("_VALUE", "value").replace("_", "")

  val jsonPath = "src/data/jsons/jsonTest.json"
  val file = new File(jsonPath)
  val writer = new PrintWriter(file)
  writer.println(dfJsonString)
  writer.close()
  println(s"Archivo guardado satisfactoriamente en: $jsonPath")
   */
}
