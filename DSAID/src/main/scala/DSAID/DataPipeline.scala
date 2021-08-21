package DSAID

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._



object DataPipeline {
  
   
   	def main(args:Array[String]):Unit={ 

	        val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
          val spark=SparkSession.builder().getOrCreate()					
					import spark.implicits._
					
					val df1 = spark.read.format("csv").option("header","true").load(".//data/dataset1.csv")
					
					//Section 1
					//Split the name field into first_name, and last_name & Delete any rows which do not have a name
					//Remove any zeros prepended to the price field & Create a new field named above_100, which is true if the price is strictly greater than 100
					
          val df2 = df1.select(split(col("name")," ").getItem(0).as("First_Name"),
              split(col("name")," ").getItem(1).as("Last_Name"),col("price"))
              .drop("name")
              .na.drop()
              .withColumn("price", regexp_replace(df1("price"), "^0*", ""))
              .withColumn("above_100",col("price") > 100)
              .show()
					

					
					
					
   	}
}