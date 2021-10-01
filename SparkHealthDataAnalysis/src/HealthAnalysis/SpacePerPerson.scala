package HealthAnalysis

import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object SpacePerPerson {
  
  def main(args : Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc=new SparkContext("local[*]","Word Count")
    
//    ----------------   remove the headers from the respective files ---------------
    
    
    var r1=sc.textFile("D:/ClouderaShared/demographic.csv")
              .map(x=>(x.split(",")(0),x.split(",")(1).toFloat))        // (id,family members)
              
    var r2=sc.textFile("D:/ClouderaShared/questionnaire.csv")
              .map(x=>(x.split(",")(0),x.split(",")(1).toInt))        // (id,rooms)
              
    val r3=r2.join(r1)
              .mapValues(x=>x._1/x._2)
              .sortBy(_._2,false)
              .take(20)
              .foreach(println)
              
  }
}