package ScalaSparkCode

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType, TimestampType}


/*1 null value replacing with a default value,
* 2. use file system object to check paths both i/p and o/p
* 3. convert final dF into RDD and use aggregateByKey in it
* 4. replace a given string with given string
* 5. config factory to load schema of dataset loaded
* 6. handle null values and adding missing columns using foldLeft */
object functions_Required  {

  def schemaCreate(str :String)={

    val strSplit = str.split(",")

    var schema = new StructType()
     val dictMap = Map("StringType()" -> StringType , "IntegerType()" -> IntegerType, "LongType()" -> LongType)

    for (i <- strSplit){
        val arr = i.split(" ")
      val types =dictMap.getOrElse(arr(1),IntegerType)
     schema=schema.add(arr(0), types,true)

    }
schema
  }





}
