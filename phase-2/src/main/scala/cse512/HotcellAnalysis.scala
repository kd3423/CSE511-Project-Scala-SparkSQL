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

    // Create a temporary View of PickupInfo
    pickupInfo.createOrReplaceTempView("pickupInfoView")

    // Define IsCellBounds function to check if the point is inside the cube boundary.
    spark.udf.register("inBound", (x: Double, y:Double, z:Int) =>  ( (x >= minX) && (x <= maxX) && (y >= minY) && (y <= maxY) && (z >= minZ) && (z <= maxZ) ))

    // Get all x, y, z points which are inside the given Boundary(defined by minX-maxX, minY-maxY, minZ-maxZ)
    val filterDf = spark.sql("select x,y,z from pickupInfoView where inBound(x, y, z) order by z,y,x").persist()
    filterDf.createOrReplaceTempView("filterDfView")

    // Count all the picksups from same cell --> x, y, z represents a cell and numPoints represents the number of pickups in that cell
    val filterpDf = spark.sql("select x,y,z,count(*) as numPoints from filterDfView group by z,y,x order by z,y,x").persist()
    filterpDf.createOrReplaceTempView("filterpDfView")

    // Define square function. This is
    spark.udf.register("square", (x: Int) => (x*x).toDouble)
    val sPoints = spark.sql("select count(*) as countOnePoinCells, sum(numPoints) as totalPoints, sum(square(numPoints)) as squaredCells from filterpDfView")
    sPoints.createOrReplaceTempView("sPoints")

    val countOnePoinCells = sPoints.first().getLong(0)
    val totalPoints = sPoints.first().getLong(1) //sigma xj
    val squaredCells = sPoints.first().getDouble(2) //sigma (xj**2)


    val X = totalPoints / numCells
    val SD = math.sqrt((squaredCells / numCells) - (X * X) )

    spark.udf.register("CountNeighbor", (lx:Int, ly:Int, lz:Int, rx:Int, ry:Int, rz:Int, px:Int, py:Int, pz:Int)
    => ((HotcellUtils.CountNeighbor(lx, ly, lz, rx, ry, rz, px, py, pz))))
    val Neighbours = spark.sql("select " +
                                                "view1.x as x, " +
                                                "view1.y as y, " +
                                                "view1.z as z, " +
                                                "CountNeighbor("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "view1.x,view1.y,view1.z) as totalNeighbours, " +   // function to get the number of neighbours of x,y,z
                                                "count(*) as neighboursWithValidPoints, " +  // count the neighbours with atLeast one pickup point
                                                "sum(view2.numPoints) as sumAllNeighboursPoints " +
                                                "from filterpDfView as view1, filterpDfView as view2 " +    // two tables to join
                                                "where (view2.x = view1.x+1 or view2.x = view1.x or view2.x = view1.x-1) and (view2.y = view1.y+1 or view2.y = view1.y or view2.y = view1.y-1) and (view2.z = view1.z+1 or view2.z = view1.z or view2.z = view1.z-1) " +   //join condition
                                                "group by view1.z, view1.y, view1.x order by view1.z, view1.y, view1.x").persist()

    Neighbours.createOrReplaceTempView("NeighboursView")

    spark.udf.register("ZScore", (x: Int, y: Int, z: Int, n: Int, avg:Double, sd: Double, total: Int, sum: Int) => ((
      HotcellUtils.ZScore(x, y, z, n, avg, sd, total, sum))))
    val NeighboursDesc = spark.sql("select x, y, z, " +
                                            "ZScore(x, y, z," +numCells+ ", " + X + ", " + SD + ", totalNeighbours, sumAllNeighboursPoints) as gi_statistic " +
                                            "from NeighboursView " +
                                            "order by gi_statistic desc")
    NeighboursDesc.createOrReplaceTempView("NeighboursDescView")
    NeighboursDesc.show()

    val hotcells_dec_order = spark.sql("select x,y,z from NeighboursDescView")

    return hotcells_dec_order
  }
}
