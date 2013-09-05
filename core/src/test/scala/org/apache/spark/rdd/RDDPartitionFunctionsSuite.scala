/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import org.scalatest.FunSuite
import org.apache.spark._
import org.apache.spark.SparkContext._

class RDDPartitionFunctionsSuite extends FunSuite with SharedSparkContext {

  test("getPartition") {
    // Test that the data gets through correctly
    val data = sc.makeRDD(Range(0, 8), 2)
    val p0 = data.getPartition(0).collect()
    assert(p0.size == 4)
    assert(p0(0) == 0)
    assert(p0(1) == 1)
    assert(p0(2) == 2)
    assert(p0(3) == 3)
    val p1 = data.getPartition(1).collect()
    assert(p1.size == 4)
    assert(p1(0) == 4)
    assert(p1(1) == 5)
    assert(p1(2) == 6)
    assert(p1(3) == 7)

    intercept[IndexOutOfBoundsException] {
      data.getPartition(2)
    }
    intercept[IndexOutOfBoundsException] {
      data.getPartition(-1)
    }

    // Test that extraneous partitions are removed
    assert(data.getPartition(0).partitions.size == 1)
  }

  test("prepend") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.prepend(Map(-2 -> List(0),
                                  -1 -> List(-1),
                                  0 -> List(0, 0, 0),
                                  1 -> List(1, 1),
                                  2 -> List(2),
                                  3 -> List(0)))

    assert(4 == result.partitions.size)

    val p0 = result.getPartition(0).collect()
    assert(2 == p0.size)
    assert(0 == p0(0))
    assert(-1 == p0(1))

    val p1 = result.getPartition(1).collect()
    assert(7 == p1.size)
    assert(0 == p1(0))
    assert(0 == p1(1))
    assert(0 == p1(2))
    assert(0 == p1(3))
    assert(1 == p1(4))
    assert(2 == p1(5))
    assert(3 == p1(6))

    val p2 = result.getPartition(2).collect()
    assert(6 == p2.size)
    assert(1 == p2(0))
    assert(1 == p2(1))
    assert(4 == p2(2))
    assert(5 == p2(3))
    assert(6 == p2(4))
    assert(7 == p2(5))

    val p3 = result.getPartition(3).collect()
    assert(2 == p3.size)
    assert(2 == p3(0))
    assert(0 == p3(1))
  }

  test("append") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.append(Map(-2 -> List(0),
                                 -1 -> List(-1),
                                 0 -> List(0, 0, 0),
                                 1 -> List(1, 1),
                                 2 -> List(2),
                                 3 -> List(0)))

    assert(4 == result.partitions.size)

    val p0 = result.getPartition(0).collect()
    assert(2 == p0.size)
    assert(0 == p0(0))
    assert(-1 == p0(1))

    val p1 = result.getPartition(1).collect()
    assert(7 == p1.size)
    assert(0 == p1(0))
    assert(1 == p1(1))
    assert(2 == p1(2))
    assert(3 == p1(3))
    assert(0 == p1(4))
    assert(0 == p1(5))
    assert(0 == p1(6))

    val p2 = result.getPartition(2).collect()
    assert(6 == p2.size)
    assert(4 == p2(0))
    assert(5 == p2(1))
    assert(6 == p2(2))
    assert(7 == p2(3))
    assert(1 == p2(4))
    assert(1 == p2(5))

    val p3 = result.getPartition(3).collect()
    assert(2 == p3.size)
    assert(2 == p3(0))
    assert(0 == p3(1))
  }

  test("sliding") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.sliding(3).collect()

    assert(6 == result.size)
    assert(List(0, 1, 2) == result(0))
    assert(List(1, 2, 3) == result(1))
    assert(List(2, 3, 4) == result(2))
    assert(List(3, 4, 5) == result(3))
    assert(List(4, 5, 6) == result(4))
    assert(List(5, 6, 7) == result(5))
  }

  test("index") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.index().collect()
    assert(8 == result.size)
    Range(0, 8).foreach(n => {
      assert(n.toLong == result(n)._1)
      assert(n == result(n)._2)
    })
  }
}
