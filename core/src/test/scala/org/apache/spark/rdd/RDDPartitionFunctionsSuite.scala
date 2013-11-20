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
    assert(p0.toList === List(0, 1, 2, 3))

    val p1 = data.getPartition(1).collect()
    assert(p1.toList == List(4, 5, 6, 7))

    intercept[IndexOutOfBoundsException] {
      data.getPartition(2)
    }
    intercept[IndexOutOfBoundsException] {
      data.getPartition(-1)
    }

    // Test that extraneous partitions are removed
    assert(data.getPartition(0).partitions.size === 1)
  }

  test("prepend") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.prepend(Map(-2 -> List(0),
                                  -1 -> List(-1),
                                  0 -> List(0, 0, 0),
                                  1 -> List(1, 1),
                                  2 -> List(2),
                                  3 -> List(0)))

    assert(4 === result.partitions.size)

    val p0 = result.getPartition(0).collect()
    assert(p0.toList === List(0, -1))

    val p1 = result.getPartition(1).collect()
    assert(p1.toList === List(0, 0, 0, 0, 1, 2, 3))

    val p2 = result.getPartition(2).collect()
    assert(p2.toList === List(1, 1, 4, 5, 6, 7))

    val p3 = result.getPartition(3).collect()
    assert(p3.toList === List(2, 0))
  }

  test("append") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.append(Map(-2 -> List(0),
                                 -1 -> List(-1),
                                 0 -> List(0, 0, 0),
                                 1 -> List(1, 1),
                                 2 -> List(2),
                                 3 -> List(0)))

    assert(4 === result.partitions.size)

    val p0 = result.getPartition(0).collect()
    assert(p0.toList === List(0, -1))

    val p1 = result.getPartition(1).collect()
    assert(p1.toList === List(0, 1, 2, 3, 0, 0, 0))

    val p2 = result.getPartition(2).collect()
    assert(p2.toList === List(4, 5, 6, 7, 1, 1))

    val p3 = result.getPartition(3).collect()
    assert(p3.toList === List(2, 0))
  }

  test("sliding") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.sliding(3).collect()

    assert(result.toList === List(List(0, 1, 2),
                                  List(1, 2, 3),
                                  List(2, 3, 4),
                                  List(3, 4, 5),
                                  List(4, 5, 6),
                                  List(5, 6, 7)))
  }

  test("sliding with tiny partitions") {
    val data = sc.makeRDD(Range(0, 8), 8)
    val result = data.sliding(3).collect()

    assert(result.toList === List(List(0, 1, 2),
                                  List(1, 2, 3),
                                  List(2, 3, 4),
                                  List(3, 4, 5),
                                  List(4, 5, 6),
                                  List(5, 6, 7)))
  }

  test("zipWithIndex") {
    val data = sc.makeRDD(Range(0, 8), 2)
    val result = data.zipWithIndex().collect()

    assert(result.toList === List((0, 0L), (1, 1L), (2, 2L), (3, 3L),
                                  (4, 4L), (5, 5L), (6, 6L), (7, 7L)))
    assert(8 === result.size)
    Range(0, 8).foreach(n => {
      assert(n === result(n)._1)
      assert(n.toLong === result(n)._2)
    })
  }
}
