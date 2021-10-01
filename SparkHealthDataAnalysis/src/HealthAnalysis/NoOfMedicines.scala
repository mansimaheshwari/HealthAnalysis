package HealthAnalysis

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object NoOfMedicines {
  
  def main(args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","Word Count")
    
    
//    ----------------   remove the headers from the respective files ---------------
    
    
    var r3=sc.textFile("D:/ClouderaShared/medications.csv")
              .map(x=>(x.split(",")(0),x.split(",")(1).toInt))
              .reduceByKey(_+_)
              .sortBy(x=>x._2, false)
              .take(10)
              .foreach(println)
  }
}