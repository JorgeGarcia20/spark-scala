package DataFrames
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

object DataFrameAPI extends App{
  // Creando una SparkSession
  val spark = SparkSession.builder().master("local").getOrCreate()
  val usersPath = "src/data/users_data.json"

  // Schemas y DataFrames
  val schemaUsers = StructType(Array(
      StructField("Name", StringType, nullable = false),
      StructField("Tall", DoubleType, nullable = false),
      StructField("Age", IntegerType, nullable = false),
      StructField("Gender", StringType, nullable = false),
  ))

  // Otra forma de crear schemas
  val schemaUsers2 = "Name STRING, Tall DOUBLE, Age INT, Gender STRING"

  val usersDF = spark.read.schema(schemaUsers2).json(usersPath)
  usersDF.show(20, truncate = false)
  usersDF.printSchema()

  // Columnas y expresiones
  // Se puede seleccionar una columna y realizar operaciones con ella
  // para seleccionar una columnas se puede usando col("col_name") o expr("col_name" ...expression) o usando $

  println("Calculando el promedio de edad con respecto al genero")
  val usersAverageAgeDF = usersDF.groupBy("Gender").agg(avg("Age"))
  usersAverageAgeDF.show()

  // Convertiendo centimetros a metros
  usersDF.withColumn("Tall", col("Tall") / 100).show(10)

  // Ordenando por edades de manera ascendente
  usersDF.sort(col("Age")).show(10)

  // Ordenando por altura de manera descendente
  usersDF.sort(col("Tall").desc).show(10)

  // Creando Filas (ROWS)

  val userRow = Row("Jorge", 169, 23, "M")
  val userRows = Seq(("Jorge", 169, 23, "M"), ("Alberto", 176, 21, "M"), ("Maria", 159, 22, "F"))
  // val newUserDF = userRows.toDF("Name", "Tall", "Age", "Gender")

  // Guardando los datos de los usuarios como parquet
  // val parquetPath = "src/data/parquests/users_data/"
  // usersDF.write.format("parquet").save(parquetPath)

  // proyecciones y filtros (Projections and filters)
  usersDF.select("Name", "Age").where(col("Age") > 20).show()
  usersDF.select("Name", "Tall").where(col("Tall") < 160).show()

  spark.close()
}
