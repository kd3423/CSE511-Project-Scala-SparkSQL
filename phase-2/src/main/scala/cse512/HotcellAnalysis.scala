package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
 {
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  //Create a temporary view pickupInfo
  pickupInfo.createOrReplaceTempView("pickupInfoTempView")

  // Function to check whether the point is inside the cube boundary
  spark.udf.register("isCellWithinTheCube", (x: Double, y:Double, z:Double) => ( (x >= minX) && (x<=maxX) && (y>minY) && (y<=maxY) && (z >=minZ) && (z<=maxZ)))

  //Get all the points which are in a given boundary
  var insideBoundaryPoints = spark.sql("select x,y,z from pickupInfoTempView where isCellWithinTheCube(x,y,z) order by z,y,x").persist()
  insideBoundaryPoints.createOrReplaceTempView("insideBoundaryPointsTempView")

  //Count all the pickups from same cell
  val countOfPointsInACell = spark.sql("select x,y,z,count(*) as numPoints from insideBoundaryPointsTempView group by z,y,x order by z,y,x").persist()
  countOfPointsInACell.createOrReplaceTempView("countOfPointsInACellTempView")

  //Define
  spark.udf.register("squareInput", (x: Int) => (x*x).toDouble)
  val sumOfPoints = spark.sql("select count(*) as numberOfCellsWithAtleastOnePoint, sum(numPoints) as totalPointsInAGivenRegion, sum(squared(numPoints)) as squaredSumOfAllPointsInGivenRegion from countOfPointsInACell")
  sumOfPoints.createOrReplaceTempView("sumOfPoints")

   //val numberOfCellsWithAtleastOnePoint = sumOfPoints.first().getLong(0)
   val totalPoints = sumOfPoints.first().getLong(1)
   val squaredSumCells = sumOfPoints.first().getDouble(2)

   val XMean = totalPoints/ numCells
   val StdDev = math.sqrt((squaredSumCells / numCells) - (XMean*XMean))
   return pickupInfo // YOU NEED TO CHANGE THIS PART
 }
}
