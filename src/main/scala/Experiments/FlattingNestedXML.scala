package Experiments

import com.databricks.spark.xml._
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object FlattingNestedXML extends App {
  val spark = SparkSession
    .builder()
    .appName("FlattingADataFrame")
    .config("spark.master", "local")
    .getOrCreate()

  def expandNestedColumn(df_temp: DataFrame): DataFrame ={
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
    df.select(columnNames:_*)
  }

  def main(df: DataFrame): DataFrame = {
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

  val xmlPath = "src/data/nestedxml.xml"
  val df = spark.read.option("rowTag", "items").xml(xmlPath)
  val flattedDF = main(df)
  flattedDF.show()

  spark.close()

  /*
  for (columnName <- df.schema.names) {
    if (df.schema(columnName).dataType.isInstanceOf[ArrayType]){
      df = df.withColumn(columnName, explode(df(columnName)).alias(columnName))
      selectClauseList :+= columnName
    }else if (df.schema(columnName).dataType.isInstanceOf[StructType]){
      for (field <- df.schema(columnName).dataType.asInstanceOf[StructType].fields){
        selectClauseList :+= columnName + "." + field.name
      }
    }else {
      selectClauseList :+= columnName
    }
  }
  * */
}
