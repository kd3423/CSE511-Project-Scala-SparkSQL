package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions

object HotzoneAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {

    var pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pointDf.createOrReplaceTempView("point")

    // Parse point data formats
    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pointDf = spark.sql("select trim(_c5) as _c5 from point")
    pointDf.createOrReplaceTempView("point")

    // Load rectangle data
    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rectangleDf.createOrReplaceTempView("rectangle")

    // Join two datasets
    //Register the user defined fuction ST_Contians.
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    // Run the user defined function to get all the rectangles with a point that lies within it.
    val joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where ST_Contains(rectangle._c0,point._c5)")
    joinDf.createOrReplaceTempView("joinResult")

    // YOU NEED TO CHANGE THIS PART


    // We get output something like [rectangleString, pointString] this means that a pointString falls within the rectangleString.
    // Here rectangleString is something like "-73.795658,40.743334,-73.753772,40.779114"
    // pointString is like "-73.766,40.7534"
    // |rectangleString S1, pointString P1|
    // |rectangleString S1, pointString P2|
    // |rectangleString S1, pointString P3|
    // ...
    // Count all the points that lie in a given rectangle. We need to do group by rectangle and count all the points that lie in that rectangle.
    val temp1 = joinDf.groupBy("rectangle").agg(count("*").alias("count"))
    return temp1.select("rectangle", "count").orderBy(asc("rectangle")).repartition(1)

    // return joinDf // YOU NEED TO CHANGE THIS PART
  }

}
