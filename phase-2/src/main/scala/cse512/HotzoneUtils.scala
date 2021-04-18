package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    if (queryRectangle == null || pointString == null || queryRectangle.isEmpty || pointString.isEmpty)
         return false

    val recPts = queryRectangle.split(",")
    val pt = pointString.split(",")

    val rectx1 = queryRectangle(0).toDouble
    val recty1 = queryRectangle(1).toDouble
    val rectx2 = queryRectangle(2).toDouble
    val recty2 = queryRectangle(3).toDouble

    val x1 = pt(0).toDouble
    val y1 = pt(1).toDouble

    if(x1<=rectx2 && x1>=rectx1 && y1>=recty1 && y1<=recty2) return true
    else if(x1>=rectx2 && x1<=rectx1 && y1 <=recty1 && y1>=recty2) return true
    else return false

  }
}
