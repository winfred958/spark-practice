package com.winfred.spark.practice

import org.junit.Test

class CollectionTest {

  @Test
  def testList(): Unit = {
    val list: List[(Int, String, Int)] = List(
      (1, "小明", 28),
      (2, "小花", 20),
      (3, "小壮", 25)
    )

    val max = list
      .max((a: (Int, String, Int), b: (Int, String, Int)) => {
        a._3 - b._3
      })

    val min = list
      .min((a: (Int, String, Int), b: (Int, String, Int)) => {
        a._3 - b._3
      })

    val count = list
      .map(x => {
        x._3.toDouble
      })
      .reduce((a, b) => {
        a + b
      })

    val avg = count / list.size

    println(max)
    println(min)
    println(avg)
  }
}
