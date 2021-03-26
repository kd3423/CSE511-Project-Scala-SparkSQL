package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    if (!queryRectangle.isEmpty && !pointString.isEmpty){
      val recPts = queryRectangle.split(",").map(_.toDouble)
      val pt = pointString.split(",").map(_.toDouble)

      if(pt(0)<=recPts(2) && pt(0)>=recPts(0) && pt(1)>=recPts(1) && pt(1)<=recPts(3)) return true
      else if(pt(0)>=recPts(2) && pt(0)<=recPts(0) && pt(1)<=recPts(1) && pt(1)>=recPts(3)) return true
      else return false
    }

    return false// YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
