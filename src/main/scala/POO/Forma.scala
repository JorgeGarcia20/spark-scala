package POO
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

class Forma(ruta: String, spark: SparkSession){
  private def leerXML: DataFrame =  spark.read.option("rootTag", "items").option("rowTag", "item").xml(ruta)

  private def expandNestedColumn(df_temp: DataFrame): DataFrame = {
    var df: DataFrame = df_temp
    var selectClauseList = List.empty[String]

    for (columnName <- df.schema.names) {
      df.schema(columnName).dataType match {
        case _: ArrayType =>
          df = df.withColumn(columnName, explode(df(columnName)).alias(columnName))
          selectClauseList :+= columnName
        case structType: StructType =>
          for (field <- structType.fields) {
            selectClauseList :+= columnName + "." + field.name
          }
        case _ =>
          selectClauseList :+= columnName
      }
    }
    val columnNames = selectClauseList.map(name => col(name).alias(name.replace('.', '_')))
    df.select(columnNames: _*)
  }

  private def aplanarDataFrame(df: DataFrame): DataFrame = {
    var dfTemp = df
    var nestedColumnCount = 1
    while (nestedColumnCount != 0) {
      var nestedColumnCountTemp = 0

      for (columnName <- dfTemp.schema.names) {
        if (dfTemp.schema(columnName).dataType.isInstanceOf[ArrayType]
          ||
          dfTemp.schema(columnName).dataType.isInstanceOf[StructType]) {
          nestedColumnCountTemp += 1
        }
      }

      if (nestedColumnCountTemp != 0) {
        dfTemp = expandNestedColumn(dfTemp)
      }
      nestedColumnCount = nestedColumnCountTemp
    }
    dfTemp
  }

  def orquestador: DataFrame = {
    try{
      val df = leerXML
      aplanarDataFrame(df)
    } catch {
      case e: Exception => println(s"Algo salio mal: ${e.getMessage}")
      spark.emptyDataFrame
    }
  }
}

object main extends App{
  val spark = SparkSession.builder().master("local").getOrCreate()
  val ruta = "src/data/nestedxml.xml"
  val newDataFrame = new Forma(ruta, spark)
  val dfAplanado = newDataFrame.orquestador
  dfAplanado.show()
}
