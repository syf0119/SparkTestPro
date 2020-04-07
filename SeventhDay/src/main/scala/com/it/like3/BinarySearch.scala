package com.it.like3

object BinarySearch {

  def binarySearch(ipNum: Long, arr: Array[(Long, Long, String, String)]): (String, String) = {

    var from = 0
    var to: Int = arr.length - 1
    while (from <= to) {
      val mid: Int =from + (to - from) / 2
      val midTuple = arr(mid)
      if (midTuple._1 <= ipNum && ipNum <= midTuple._2) {
         return midTuple._3 -> midTuple._4
      }
     else if (ipNum < midTuple._1) {

        to = mid - 1
      }
     // midTuple._2 < ipNum
      else  {

        from = mid + 1
      }

    }


    null
  }


}
