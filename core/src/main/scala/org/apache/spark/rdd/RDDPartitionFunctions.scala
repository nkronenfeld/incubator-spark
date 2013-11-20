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
class RDDPartitionFunctions[T: ClassManifest] (self: RDD[T]) {
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
  def prepend(prefixes: Map[Int, Seq[T]]): RDD[T] = {
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
  def append(suffixes: Map[Int, Seq[T]]): RDD[T] = {
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
      self.mapPartitions(_.sliding(size)).map(_.toSeq)

    // Get all windows of size that cross partition boundaries
    val interSplitSets =
      self.mapPartitionsWithIndex((index, i) => {
        val firstN: Seq[T] = Range(0, size-1).flatMap(n => {
          if (i.hasNext) Seq(i.next) else Seq()
        })
        val lastN: Seq[T] = if (i.hasNext) {
          val lastAttempt: Seq[T] = i.scanRight(Seq[T]())((elt, seq) =>
            if (seq.size >= size-1) seq else Seq(elt) ++ seq
          ).next
          if (lastAttempt.size > size-1) {
            firstN.slice(firstN.size-(size-1-lastAttempt.size), firstN.size) ++ lastAttempt
          } else {
            lastAttempt
          }
        } else {
          firstN
        }.toSeq

        val firstSubs: Seq[((Int, Int), Map[Int, Seq[T]])] =
          Range(1, size).map(n =>
            ((index-1, n), Map(1 -> firstN.slice(0, n))))
        val lastSubs:Seq[((Int, Int), Map[Int, Seq[T]])] =
            Range(1, size).map(n =>
              ((index, n), Map(0 -> lastN.slice(n-1, lastN.size))))

        (firstSubs ++ lastSubs).iterator
      }).groupByKey(1).flatMap(p => {
        val whichCross = p._1._2
        val subSequences: Map[Int, Seq[T]] = p._2.reduce(_ ++ _)
        val numElts = subSequences.values.map(seq => seq.size).fold(0)(_+_)

        if (numElts < size) {
          Seq()
        } else {
          // Stuff from previous partition
          val start: Seq[T] =
            if (subSequences.contains(0)) subSequences(0) else Seq[T]()
          // Stuff from next partition
          val end: Seq[T] =
              if (subSequences.contains(1)) subSequences(1) else Seq[T]()

          // Combine them, and 
          Seq((p._1, start ++ end))
        }
      })
    // Key each element to the subset to which it should be prepended, and key 
    // the item to be prepended by its order among all elements to be prepended 
    // to the same partition.
    .map(p => (p._1._1, (p._1._2, p._2)))
    // Collect all additions to a given partition
    .groupByKey(1)
    // Sort the additions to each partition, and eliminate the key we used to do so
    .map(p => (p._1, p._2.sortBy(_._1).map(_._2)))
    .collect().toMap


    // TODO: Bring that whole last calculation down locally, and figure out what to do when
    // a partition is smaller than size.


    val intraSplitPartitionedSets =
      new RDDPartitionFunctions(intraSplitSets)

    intraSplitPartitionedSets.append(interSplitSets)
  }




  /**
   * Take an RDD, and index its records
   */
  def zipWithIndex(): RDD[(T, Long)] = {
    // We work exclusively with immutable indexed sequences in this method; the
    // default seems to be from scala.collection
    import scala.collection.immutable.IndexedSeq

    // Index records by partition, and record with partition number
    val partitionIndexedData = self.mapPartitionsWithIndex((partitionIndex, i) => {
      var recordIndex = 0L
      i.map(record => {
        val thisRecordIndex = recordIndex
        recordIndex = recordIndex + 1L
        ((partitionIndex, thisRecordIndex), record)
      })
    })
    partitionIndexedData.cache()

    // count records per partition
    val recordsPerPartition:Seq[Long] = partitionIndexedData.map(p => {      
      val partition = p._1._1
      Range(0, partition+1).map(n => if (n == partition) 1L else 0L)
    }).reduce((a, b) => {
      def addArrays (a: IndexedSeq[Long], b:IndexedSeq[Long]): IndexedSeq[Long] = {
        val maxIdx = a.size.max(b.size)
        for (idx <- Range(0, maxIdx)) yield
          (if (idx < a.size) a(idx) else 0) + (if (idx < b.size) b(idx) else 0)
      }
      addArrays(a, b)
    })
    // Now get the number of previous records to each partition
    val previousRecordsByPartition:Seq[Long] =
      Range(0, recordsPerPartition.size).map(partition =>
        Range(0, partition).map(s1 =>
          recordsPerPartition(s1)
        )
      ).map(_ ++ Seq(0L)).map(_.reduce(_+_))

    // broadcast that so everyone can use it
    val broadcastPreviousRecordsPerPartition =
      self.context.broadcast(previousRecordsByPartition)

    // Now, take each record, and add the previous records for its partition to 
    // its index within its partition
    partitionIndexedData.map(p => {
      val previousRecords = broadcastPreviousRecordsPerPartition.value(p._1._1)
      val indexInPartition = p._1._2
      val record = p._2
      (record, previousRecords + indexInPartition)
    })
  }
}

