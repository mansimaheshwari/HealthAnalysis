package HealthAnalysis

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField


object DrugSideEffects {
  
   def sparkConf : SparkConf={
        val sConf=new SparkConf
        sConf.set("spark.app.name", "DrugSideEffects")
        sConf.set("spark.master","local[*]")
        sConf // == return statement
      }
      
  def main(args : Array[String]){
    
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger(getClass.getName).error("DrugSideEffects")
      
        val spark=SparkSession.builder()                      
                            .config(sparkConf)
                            .config("spark.sql.shuffle.partitions","10")    // to make the no of partitions to 3 instead of 200 (spark default)
                            .getOrCreate()
    
        

//       val bcast=spark.sparkContext.broadcast(args)
       import spark.implicits._
       val sc=spark.sparkContext
       
       
       sc.parallelize(args)
       val schema = StructType(List(
              StructField("drugs", StringType)
              ))
    
    
       val df1=  sc.parallelize(args).toDF()
                                     .map(x=>x.getString(0).toLowerCase())
                                     .toDF("drugs")
                   
//            .select("*")  
//             .show(5)
                     
                                     
                                     
  
       val df2=spark.read
                  .format("csv")
                  .option("path","D:/HealthDataSpark/input/drugSideEffect.csv")
                  .option("header",true)
                  .schema("drugName String, sideEffect String")  //  .option("inferSchema",true)
                  .load
                  .map(x=>(x.getString(0).toLowerCase(),x.getString(1)))
                  .toDF("drugName","sideEffect")
//            .select("*")  
//             .show(5)
                        
       import org.apache.spark.sql.functions._  // import to use agg() function

       val joinCond=df1("drugs")===df2("drugName")
       val joinType="inner"        
                  df2.join(df1,joinCond,joinType)
                  .drop(df1("drugs"))
                  .drop(df2("drugName"))
                  .flatMap(x=>x.getString(0).split(","))
                  .toDF("Side_Effect")
                  .dropDuplicates("Side_Effect")
//                  .select("*")  
                   .show()
                  
                  
//      out.write
//          .format("csv")
//          .mode(SaveMode.Overwrite)
//          .option("header", true)
//          .option("path","D:/HealthDataSpark/output/JunkFoodRelatingDiseaseCount")
//          .save
                                  
      scala.io.StdIn.readLine()
      spark.close()
  }
  
}