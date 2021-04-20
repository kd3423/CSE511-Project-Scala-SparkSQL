package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }
  
    def boundChecker(p: Int, l: Int, r: Int) : Int={

    if (p == l || p == r){
      return 1
    }
    return 0
  }
  

  def ZScore(x: Int, y: Int, z: Int, n: Int, avg:Double, sd: Double, total: Int, sum: Int): Double ={
    val num = (sum.toDouble - (avg*total.toDouble))
    val denom = sd * math.sqrt((((n.toDouble * total.toDouble) - (total.toDouble * total.toDouble)) / (n.toDouble-1.0).toDouble).toDouble).toDouble
    return (num/denom).toDouble
  }
  
    def CountNeighbor(lx:Int, ly:Int, lz:Int, rx:Int, ry:Int, rz:Int, px:Int, py:Int, pz:Int): Int ={

    val locCube: Map[Int, String] = Map(0->"inside", 1 -> "face", 2-> "edge", 3-> "corner")
    val m: Map[String, Int] = Map("inside" -> 26, "face" -> 17, "edge" -> 11, "corner" -> 7)

    var i = 0;

    i += boundChecker(px, lx, rx)
    i += boundChecker(py, ly, ry)
    i += boundChecker(pz, lz, rz)

    var loc = locCube.get(i).get.toString()

    return m.get(loc).get.toInt
  }
}
