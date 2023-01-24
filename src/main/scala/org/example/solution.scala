package org.example

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, lower, row_number, sum, trim}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

import scala.Console.println
import scala.math.Fractional.Implicits.infixFractionalOps
//import org.apache.spark.sql.impl

//import org.apache.spark.implicits._
object solution {

  def main(args:Array[String]): Unit ={

    // Initialize spark session
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();
    // Define logging level
    spark.sparkContext.setLogLevel("ERROR")
    //Create RDD from external CSV source; doing this
    import spark.implicits
    val rdd_main = spark.sparkContext.textFile("/home/ayushchatur/sales_data_sample.csv")
    val header = rdd_main.first()
    // skip the first row
    val main_rdd = rdd_main.filter( row=> row!= header)

    // filter only those columns which are needed and convert to type
    val colm_rdd = main_rdd.map(
      line => {
        val fields = line.substring(1, line.length() - 1).split(",")
        //      println(fields  + "class" + fields.getClass())
        (fields(0).toInt, fields(9).toInt, fields(10),fields(2).toDouble, fields(6))

      })


    // filter based on criteria
    val filtered = colm_rdd.filter( _._5.toLowerCase() == "shipped")

//    val k = main_rdd.filter( f => (f.))
    // create a combined key based on year and product line
    val new_key = filtered.map(
      k => (k._2.toString()+k._3, k )
    )

    // combiner that runs per record ( return a tuple with price and "1") -> 1 for maintaing frequency
    def createCombiner = (price:Double) =>
      (price, 1)
  // combiner that runs within partition
    val mergeval = (acc: (Double, Int), element: (Double)) =>
      // add prices and increment frequency by one for each record with same key
      (acc._1 + element, acc._2 + 1)
// combiner that runs across partition
    val mergeCombiner = (accumulator1: (Double, Int), accumulator2: (Double, Int)) =>
      (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2)
    println("**********************")
    new_key.foreach(f => {
      println(f)
    })

    // get rid of all othe columns, all information is in key and values
    val another_new_key = new_key.map { x => (x._1, x._2._4) }

    // run the aggregate operation
    val rdd2 = another_new_key.combineByKey(
      createCombiner,
      mergeval,
      mergeCombiner
    )


    // rounding to 2 decimal places  , amp values can also be used for efficiency
    val final_rdd = rdd2.map(x => (x._1, ((x._2._1 / x._2._2 * 100).round / 100.toDouble)))

  // post process: split the key
    val split_rdd = final_rdd.map( x => ( ((x._1.substring(0,4),x._1.substring(4,x._1.length )),  x._2))
    )
    // order by year and product line
    val sorted = split_rdd.sortByKey()
    val rowRDD:RDD[Row] = sorted.map(p => Row(p._1._1, p._1._2, p._2))
  // write to file
    val schema = new StructType()
      .add(StructField("YEAR_ID", StringType, false))
      .add(StructField("PRODUCTLINE", StringType, false))
      .add(StructField("AVERAGE_SALES_AMT", DoubleType, false))
    val dd = spark.createDataFrame(rowRDD, schema)
    dd.coalesce(rowRDD.getNumPartitions).write.option("header", true).option("overwrite", true).csv("result_cs1")
  }
}