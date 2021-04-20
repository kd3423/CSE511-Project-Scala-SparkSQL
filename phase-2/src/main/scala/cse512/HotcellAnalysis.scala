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

    pickupInfo.createOrReplaceTempView("pickupInfoView")

    spark.udf.register("inBound", (x: Double, y:Double, z:Int) =>  ( (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) ))

    val filterDf = spark.sql("select x,y,z from pickupInfoView where inBound(x, y, z) order by z,y,x").persist()
    filterDf.createOrReplaceTempView("filterDfView")

    val filterpDf = spark.sql("select x,y,z,count(*) as numPoints from filterDfView group by z,y,x order by z,y,x").persist()
    filterpDf.createOrReplaceTempView("filterpDfView")

    spark.udf.register("square", (x: Int) => (x*x).toDouble)
    val sPoints = spark.sql("select count(*) as countOnePoinCells, sum(numPoints) as totalPoints, sum(square(numPoints)) as squaredCells from filterpDfView")
    sPoints.createOrReplaceTempView("sPoints")

    val countOnePoinCells = sPoints.first().getLong(0)
    val totalPoints = sPoints.first().getLong(1) 
    val squaredCells = sPoints.first().getDouble(2) 


    val X = totalPoints / numCells
    val SD = math.sqrt((squaredCells / numCells) - (X * X) )

    spark.udf.register("CountNeighbor", (lx:Int, ly:Int, lz:Int, rx:Int, ry:Int, rz:Int, px:Int, py:Int, pz:Int)
    => ((HotcellUtils.CountNeighbor(lx, ly, lz, rx, ry, rz, px, py, pz))))
    val Ns = spark.sql("select " + "fp1.x as x, " + "fp1.y as y, " + "fp1.z as z, " + "CountNeighbor("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "fp1.x,fp1.y,fp1.z) as NTotal, " + 
        "count(*) as NValidPts, " + "sum(fp2.numPoints) as NSumPts " + "from filterpDfView as fp1, filterpDfView as fp2 " + 
        "where (fp2.x = fp1.x+1 or fp2.x = fp1.x or fp2.x = fp1.x-1) and (fp2.y = fp1.y+1 or fp2.y = fp1.y or fp2.y = fp1.y-1) and (fp2.z = fp1.z+1 or fp2.z = fp1.z or fp2.z = fp1.z-1) " + 
        "group by fp1.z, fp1.y, fp1.x order by fp1.z, fp1.y, fp1.x").persist()

    Ns.createOrReplaceTempView("NView")

    spark.udf.register("ZScore", (x: Int, y: Int, z: Int, n: Int, avg:Double, sd: Double, total: Int, sum: Int) => ((
    HotcellUtils.ZScore(x, y, z, n, avg, sd, total, sum))))
    val NsDesc = spark.sql("select x, y, z, " + "ZScore(x, y, z," +numCells+ ", " + X + ", " + SD + ", NTotal, NSumPts) as gordStat " + "from NView " + "order by gordStat desc")
    NsDesc.createOrReplaceTempView("NDescendView")
    NsDesc.show()

    val final_hcell_output = spark.sql("select x,y,z from NDescendView")

    return final_hcell_output
  }
}
