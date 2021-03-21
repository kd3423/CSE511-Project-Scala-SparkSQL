package cse512

import org.apache.spark.sql.SparkSession
import scala.math
object SpatialQuery extends App{

  def ST_Contains(rectangleQuery: String, point: String): Boolean = {
    if (!rectangleQuery.isEmpty && !point.isEmpty){
      val recPts = rectangleQuery.split(",").map(_.toDouble)
      val pt = point.split(",").map(_.toDouble)

      if(pt(0)<=recPts(2) && pt(0)>=recPts(0) && pt(1)>=recPts(1) && pt(1)<=recPts(3)) return true
      else if(pt(0)>=recPts(2) && pt(0)<=recPts(0) && pt(1)<=recPts(1) && pt(1)>=recPts(3)) return true
      else return false
    }

    return false
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean =
	{
		if (pointString1 == null || pointString2 == null || pointString1.isEmpty() || pointString2.isEmpty())
			return false
        val al = pointString1.split(",")
		val bl = pointString2.split(",")
        val a = al(0).toDouble
        val b = al(1).toDouble
        val c = bl(0).toDouble
        val d = bl(1).toDouble

		val indist = math.pow(a - b, 2) + math.pow(c - d, 2)
        val dist = math.sqrt(indist)
        if (dist <= distance)
            return true
        else
            return false
    }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{ST_Contains(queryRectangle,pointString)})

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>{ST_Contains(queryRectangle,pointString)})

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> {ST_Within(pointString1, pointString2, distance)})

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=> {ST_Within(pointString1, pointString2, distance)})
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
