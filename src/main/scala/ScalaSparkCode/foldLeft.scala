package ScalaSparkCode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrameNaFunctions, SparkSession}
import org.apache.spark.sql.functions.{col, lit, split, to_timestamp,collect_list,udf}
import com.typesafe.config.ConfigFactory
//import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}
//import org.apache.spark.sql.Dataset
//import org.apache.spark.sql.{Encoder, Encoders}

object foldLeft {

  def main(args: Array[String]) = {


    Logger.getLogger("org").setLevel(Level.ERROR)

    val props = ConfigFactory.load()
    val spark = SparkSession.builder()
      .appName("foldLeft")
      .master("local")
      .getOrCreate()

/*
    val sch =  StructType(
      List( StructField("id", IntegerType ,true),
        StructField("name", StringType ,true),
        StructField("state", StringType ,true),
        StructField("phno", LongType ,true),
        StructField("age", IntegerType ,true),
        StructField("dob", StringType ,true)) )

   val ownschema = new StructType()
     .add("id", IntegerType ,true )
     .add("name", StringType ,true)
      .add("state", StringType ,true)
     .add("phno", LongType ,true)
     .add("age", IntegerType ,true)
     .add("dob", StringType ,true)
*/



    val schemaValue = props.getString("landingFileSchema")
    val schema = functions_Required.schemaCreate(schemaValue)


    val dataset = spark.read.schema(schema)
      .csv("/Users/himanshubhardwaj/dataNull.csv")
      .withColumn("dob" , to_timestamp(col("dob"), "dd/MM/yyyy"))
    dataset.printSchema()

      //dataset.show(10)
      // dataset.printSchema()

 //finding cid with one or more null values in its columns so we will iterate through columns reason given down

    var emptyDF = spark.emptyDataFrame.withColumn("cid" , lit(0))
     .withColumn("col_name", lit(""))

    dataset.columns.foldLeft(dataset){(dx,c) =>
    emptyDF = emptyDF.union(dx.select("id").where(col(s"$c").isNull)
      .withColumn("col_name",lit(s"$c")))
    dataset
    }

   val finalDF= emptyDF.groupBy("cid").agg(collect_list(col("col_name")))
      .distinct()
    

    finalDF.show(10)


 //another use case where we compare 2 dfs and add missing columns which is present in 1 and not another

    val customer = spark.read
      .option("header", "true")
      .option("sep","|")
      .option("inferSchema", "true")
      .csv("/Users/himanshubhardwaj/customer.csv")
      .withColumn("asset", (split(col("asset"), ",")))


    customer.show(10)
    customer.printSchema()

   val x = dataset.columns.toSet.diff(customer.columns.toSet) //columns present in 'dataset' & not present in 'customer'
    val y = dataset.schema.fields.toSet.diff(customer.schema.fields.toSet)

    println(s"\n x: $x \n y: $y \n\n")

   val newData=  x.foldLeft(customer){(cust,c)=>
      cust.withColumn(s"$c", lit(""))
    }
    newData.show(10)


    spark.stop()
  }
}



/* we iterate using columns, alternatively what we can do is that we can use withColumn and use when otherwise condition
and specify the columns which have null values and select its cid and put it as value for the new column. and finally select
new column to get custId of rows which has 1 or more than 1 null values in its fields but problem comes when there are 500
rows we cant specify manually that many rows to check null values so we iterate using columns

we have to use union in order to add new values to empty dataframe created. but one may think that we could also just
create a new df and do withCoulmn instead of doing union where we add new column using withCoulmn and specify the value.
but we cannot do that withColumn is used to add or modify values of a existing or new column
*but union adds the value to all columns in a dataset from below.
such that it will unions rows to a dataset
withColumn operates on a single complete column so it will change the values of each and every element of column or add a column
but in order to add a row to a dataset we use union operation.. so uinion is a horizontal operation and withColumn is
vertical operation

line 77,78 so we are extracting the columns from dataframe so we can extract columns in any of the 2 ways the first way will
give us simply name of column like (dob,id,name) and 2nd way will give us in form of struct field as displayed
*/
