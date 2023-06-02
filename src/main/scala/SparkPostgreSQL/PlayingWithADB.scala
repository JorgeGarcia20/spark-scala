package SparkPostgreSQL
import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.types._
import java.util.Properties

object PlayingWithADB extends App {
  val spark = SparkSession.builder().master("local").getOrCreate()

  val connectionProperties = new Properties()
  connectionProperties.put("user", "postgres")
  connectionProperties.put("password", "my_password")
  connectionProperties.put("driver", "org.postgresql.Driver")
  val jdbcURL = "jdbc:postgresql://localhost:5432/school_db"
  val tableName = "students"

  val getStudentsDataQuery = s"SELECT * FROM $tableName"

  def getDataFromPostgres(query: String): DataFrame = {
    try {
      val df = spark.read.format("jdbc")
        .option("url", jdbcURL)
        .option("query", query)
        .option("user", connectionProperties.getProperty("user"))
        .option("password", connectionProperties.getProperty("password"))
        .option("driver", connectionProperties.getProperty("driver"))
        .load()
      df
    } catch {
      case e: Exception => println(s"Algo salio mal: ${e.getMessage}")
      spark.emptyDataFrame
    }
  }

  val schemaStudents = StructType(Seq(
    StructField("student_id", IntegerType, nullable = true),
    StructField("name", StringType, nullable = true),
    StructField("last_name", StringType, nullable = true),
    StructField("date_birth", DateType, nullable = true),
    StructField("dni", StringType, nullable = true),
    StructField("semester", StringType, nullable = true),
    StructField("group_id", StringType, nullable = true)
  ))
/*
  val newAlumn = Seq(
    (2000, "Jorge", "Garcia", "1999-20-10", "124-223-1244", 2, 5),
    (2001, "Alberto", "Estrada", "1999-20-10", "924-341-1394", 6, 9)
  )

  val newAlumnDf = spark.createDataFrame(newAlumn)
    .toDF("student_id", "name", "last_name", "date_birth", "dni", "semester", "group_id")
*/

  val newAlumnsDF = spark.read.option("header", "true")
    .schema(schemaStudents)
    .csv("/home/jorge/IdeaProjects/sparkGettingStarted/src/data/students.csv")
  try{
    newAlumnsDF.write.mode(SaveMode.Append).jdbc(jdbcURL, tableName, connectionProperties)
  }catch {
    case e: Exception => println("Algo salio mal :(")
  }

  val query2 = s"SELECT * FROM $tableName WHERE student_id IN (2000, 2001)"

  try {
    val df = spark.read.format("jdbc")
      .option("url", jdbcURL)
      .option("query", query2)
      .option("user", connectionProperties.getProperty("user"))
      .option("password", connectionProperties.getProperty("password"))
      .option("driver", connectionProperties.getProperty("driver"))
      .load()
    df.show()
  } catch {
    case e: Exception => println(s"Algo salio mal: ${e.getMessage}")
  }

  spark.close()
}
