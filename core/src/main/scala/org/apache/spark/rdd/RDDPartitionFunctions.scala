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

import scala.collection.Map

import org.apache.spark.SparkContext._

/**
 * Extra functions available on all RDDs that work on a partition-by-partition
 * basis.
 */
trait RDDPartitionFunctions[T] {
  self: RDD[T] => 

  implicit def manifest: ClassManifest[T]

  /**
   * Isolate a single partition from our RDD.
   *
   * @param n
   *        The index of the partition to isolate.  If the index isn't a legal
   *        one, an IndexOutOfBoundsException will be thrown
   */
  def getPartition(n: Int): RDD[T] = {
    def numPartitions = self.partitions.size

    if (n<0 || n >= numPartitions) {
      throw new IndexOutOfBoundsException("Attempt to get partition "+n
                                          +" of an RDD with only "
                                          +numPartitions+" partitions")
    } else {
      PartitionPruningRDD.create(self, (partitionIndex => n == partitionIndex))
    }
  }



  /**
   * Prepend the given sequences to the indicated partitions.
   * 
   * @param prefixes The information to prepend.  The keys of this map indicate the partition to
   *                 which to prepend the information, the values, the information to prepend.  Any
   *                 prefixes keyed to a partition number before the first partition (i.e., less
   *                 than zero) will be placed before the first element of the first partition;
   *                 similarly, any prefix keyed to a partition number after the last partition
   *                 will be placed after the last element of the last partition
   */
  def prepend[TT >: T : ClassManifest] (prefixes: Map[Int, Seq[TT]]): RDD[TT] = {
    val beforeFirst =
      prefixes.keys.filter(_ < 0).toSeq.sorted.flatMap(n => prefixes(n));
    val afterLast =
      prefixes.keys.filter(_ >= self.partitions.size).toSeq.sorted.flatMap(n => prefixes(n));

    val broadcastPrefixes = self.context.broadcast(prefixes);

    var result = self.mapPartitionsWithIndex((index, i) => {
      if (broadcastPrefixes.value.contains(index)) {
        broadcastPrefixes.value(index).iterator ++ i
      } else {
        i
      }
    })

    if (!beforeFirst.isEmpty) {
      result = self.context.parallelize(beforeFirst).union(result)
    }
    if (!afterLast.isEmpty) {
      result = result.union(self.context.parallelize(afterLast))
    }

    result
  }



  /**
   * Append the given sequencess to the indicated partitions.
   * 
   * @param suffixes The information to append.  The keys of this map indicate the partition to
   *                 which to append the information, the values, the information to append.  Any
   *                 suffixes keyed to a partition number before the first partition (i.e., less
   *                 than zero) will be placed before the first element of the first partition;
   *                 similarly, any suffix keyed to a partition number after the last partition
   *                 will be placed after the last element of the last partition
   */
  def append[TT >: T : ClassManifest] (suffixes: Map[Int, Seq[TT]]): RDD[TT] = {
    val beforeFirst =
      suffixes.keys.filter(_ < 0).toSeq.sorted.flatMap(n => suffixes(n));
    val afterLast =
      suffixes.keys.filter(_ >= self.partitions.size).toSeq.sorted.flatMap(n => suffixes(n));

    val broadcastSuffixes = self.context.broadcast(suffixes);

    var result = self.mapPartitionsWithIndex((index, i) => {
      if (broadcastSuffixes.value.contains(index)) {
        i ++ broadcastSuffixes.value(index).iterator
      } else {
        i
      }
    })

    if (!beforeFirst.isEmpty) {
      result = self.context.parallelize(beforeFirst).union(result)
    }
    if (!afterLast.isEmpty) {
      result = result.union(self.context.parallelize(afterLast))
    }

    result
  }



  /**
   * Take an RDD, and transform it into sets of adjacent records.
   *
   * The order of the output RDD is the natural order derived from the input RDD.  It is assumed
   * that this input order is meaningful - otherwise, why would one want to do this?
   *
   * It should be noted that this operation takes two passes.  For efficiency's sake, users
   * probably want to cache the RDD before using sliding.
   *
   * @param size The number of input records per output record
   */
  def sliding(size: Int): RDD[Seq[T]] = {
    // Get all windows of maxSize within each partition
    val intraSplitSets:RDD[Seq[T]] =
      self.mapPartitions(_.sliding(size)).map(_.toSeq).filter(_.size == size)

    // Get all windows of size that cross partition boundaries
    // First, calculate the pieces of each partition that might be part of such a window
    val interSplitParts =
      self.mapPartitionsWithIndex((index, i) => {
        val firstN: Seq[T] = Range(0, size-1).flatMap(n => {
          if (i.hasNext) Seq(i.next) else Seq()
        })
        val lastN: Seq[T] = if (i.hasNext) {
          val lastAttempt: Seq[T] = i.scanRight(Seq[T]())((elt, seq) =>
            if (seq.size >= size-1) seq else Seq(elt) ++ seq
          ).next
          if (lastAttempt.size < size) {
            firstN.slice(firstN.size-(size-1-lastAttempt.size), firstN.size) ++ lastAttempt
          } else {
            lastAttempt
          }
        } else {
          firstN
        }.toSeq

        val maxFirstItems = (size+1).min(firstN.size)+1
        val firstParts = Range(1, maxFirstItems).map(n => {
            (n, firstN.slice(0, n))
        })

        val lastSize = lastN.size
        val maxLastItems = (size+1).min(lastSize)+1
        val lastParts = Range(1, maxLastItems).map(n => {
          val items = maxLastItems - n
          (items, lastN.slice(lastSize-items, lastSize))
        })

        List((index, (firstParts, lastParts))).iterator
      }).collect.toMap

    val numPartitions = self.partitions.size
    // Recursive function to gobble more items from other partitions
    // numLeft: the number of items still needed
    // partition: the partition at which to start looking
    def getMore (numLeft: Int, partition: Int): Seq[T] = {
      if (partition >= numPartitions) {
        Seq[T]()
      } else {
        val parts = interSplitParts(partition)._1

        if (parts.size >= numLeft) {
          parts(numLeft-1)._2
        } else if (0 == parts.size) {
          getMore(numLeft, partition+1)
        } else {
          parts.last._2 ++ getMore(numLeft - parts.size, partition+1)
        }
      }
    }
    // Put together our pieces into full windows.
    val interSplitSets = Range(0, numPartitions-1).map(partition => {
      val parts = interSplitParts(partition)._2

      (partition, parts.map{case (n, items) => {
        val rest = getMore(size-n, partition+1)
        items ++ rest
      }}.filter(_.size == size))
    }).toMap

    intraSplitSets.append(interSplitSets)
  }




  /**
   * Take an RDD, and index its records
   * 
   * It should be noted that this operation takes two passes.  For efficiency's sake, users
   * probably want to cache the RDD before using sliding.
   */
  def zipWithIndex(): RDD[(T, Long)] = {
    // We work exclusively with immutable indexed sequences in this method; the
    // default seems to be from scala.collection
    import scala.collection.immutable.IndexedSeq

    // Get the number of records per partition
    val recordsPerPartition = self.mapPartitionsWithIndex((index, i) => {
      List((index, i.size.toLong)).iterator
    }).collect().sortBy(_._1)

    // Get the number of previous records for each partition
    var previousRecords = 0L
    val previousRecordsByPartition = recordsPerPartition.map{case (index, num) => {
      val pr = previousRecords
      previousRecords = previousRecords + num
      (index, pr)
    }}.toMap
    // broadcast that so everyone can use it
    val broadcastPreviousRecordsByPartition =
      self.context.broadcast(previousRecordsByPartition)

    // Index the records of each parition based on the number of records found in 
    // previous partitions
    self.mapPartitionsWithIndex((index, i) => {
      var recordIndex = broadcastPreviousRecordsByPartition.value(index)
      i.map(datum => {
        val thisRecordIndex = recordIndex
        recordIndex = recordIndex + 1L
        (datum, thisRecordIndex)
      })
    })
  }
}

