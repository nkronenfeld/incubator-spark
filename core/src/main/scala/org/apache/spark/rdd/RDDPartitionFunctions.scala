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
  def getPartition (n: Int) = {
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
   * Prepend the given lists to the indicated partitions.
   * 
   * @param prefixes The information to prepend.  The keys of this map indicate
   *                 the partition to which to prepend the information, the
   *                 values, the information to prepend.
   */
  def prepend (prefixes: Map[Int, Seq[T]]): RDD[T] = {
    val beforeFirst =
      prefixes.keys.filter(_ < 0).toList.sortBy(n =>
        n
      ).flatMap(n => prefixes(n));
    val afterLast =
      prefixes.keys.filter(_ >= self.partitions.size).toList.sortBy(n =>
        n
      ).flatMap(n => prefixes(n));

    val broadcastPrefixes = self.context.broadcast(prefixes);

    var result = self.mapPartitionsWithIndex((index, i) => {
      if (broadcastPrefixes.value.contains(index))
        broadcastPrefixes.value(index).iterator ++ i
      else
        i
    })

    if (!beforeFirst.isEmpty)
      result = self.context.parallelize(beforeFirst) union result
    if (!afterLast.isEmpty)
      result = result union self.context.parallelize(afterLast)

    result
  }



  /**
   * Append the given lists to the indicated partitions.
   * 
   * @param suffixes The information to append.  The keys of this map indicate
   *                 the partition to which to append the information, the
   *                 values, the information to append.
   */
  def append (suffixes: Map[Int, Seq[T]]): RDD[T] = {
    val beforeFirst =
      suffixes.keys.filter(_ < 0).toList.sortBy(n =>
        n
      ).flatMap(n => suffixes(n));
    val afterLast =
      suffixes.keys.filter(_ >= self.partitions.size).toList.sortBy(n =>
        n
      ).flatMap(n => suffixes(n));

    val broadcastSuffixes = self.context.broadcast(suffixes);

    var result = self.mapPartitionsWithIndex((index, i) => {
      if (broadcastSuffixes.value.contains(index))
        i ++ broadcastSuffixes.value(index).iterator
      else
        i
    })

    if (!beforeFirst.isEmpty)
      result = self.context.parallelize(beforeFirst) union result
    if (!afterLast.isEmpty)
      result = result union self.context.parallelize(afterLast)

    result
  }



  /**
   * Take an RDD, and transform it into sets of adjacent records.
   * Each record will contain maxSize entries, though up to
   * (maxSize-minSize) of them may be None.  These will be the first
   * and last few sets of the dataset, and the use of None is so that
   * the user can distinguish the first from the last.  For instance,
   * with an input RDD of the alphabet ("a", "b", ... "z"), and sets
   * of from 2-3 elements, the first element returned will be (None,
   * Some("a"), Some("b")), then (Some("a"), Some("b"), Some("c")),
   * etc, up to (Some("x"), Some("y"), Some("z")), and finally,
   * (Some("y"), Some("z"), None) 
   *
   * The order of the output RDD is the natural order derived from the
   * input RDD.  It is assumed that this input order is meaningful -
   * otherwise, why would one want to do this?
   */
  def sliding (size: Int): RDD[List[T]] = {
    // Get all windows of maxSize within each partition
    val intraSplitSets:RDD[List[T]] =
      self.mapPartitions(_.sliding(size)).map(_.toList)

    // Get all windows of size that cross partition boundaries
    val interSplitSets =
      self.mapPartitionsWithIndex((index, i) => {
        val dupl:(Iterator[T], Iterator[T]) = i.duplicate
        val firstN:List[T] = dupl._1.take(size-1).toList
        val lastN:List[T] = dupl._2.scanRight(List[T]())((elt, list) => 
          if (list.size >= size-1) list else List(elt) ++ list
        ).next
        val firstSubs:List[((Int, Int), Map[Int, List[T]])] =
          Range(1, size).map(n =>
            ((index-1, n), Map(1 -> firstN.slice(0, n)))).toList
        val lastSubs:List[((Int, Int), Map[Int, List[T]])] =
            Range(1, size).map(n =>
              ((index, n), Map(0 -> lastN.slice(n-1, lastN.size)))).toList

        (firstSubs ++ lastSubs).iterator
      }).groupByKey(1).flatMap(p => {
        val whichCross = p._1._2
        val subLists:Map[Int, List[T]] = p._2.reduce(_ ++ _)
        val numElts = subLists.values.map(list => list.size).fold(0)(_+_)

        if (numElts < size) List()
        else {
          // Stuff from previous partition
          val start:List[T] =
            if (subLists.contains(0)) subLists(0)
            else List[T]()
          // Stuff from next partition
          val end:List[T] =
              if (subLists.contains(1)) subLists(1)
              else List[T]()

          // Combine them, and 
          List((p._1, start ++ end))
        }
      })
    // Key each element to the subset to which it should be prepended, and key 
    // the item to be prepended by its order among all elements to be prepended 
    //to the same partition.
    .map(p => (p._1._1, (p._1._2, p._2)))
    // Collect all additions to a given partition
    .groupByKey(1)
    // Sort the additions to each partition, and eliminate the key we used to do so
    .map(p => (p._1, p._2.sortBy(_._1).map(_._2)))
    .collect().toMap

    val intraSplitPartitionedSets =
      new RDDPartitionFunctions(intraSplitSets)

    intraSplitPartitionedSets.append(interSplitSets)
  }




  /**
   * Take an RDD, and index its records
   */
  def index (): RDD[(Long, T)] = {
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
      ).map(_ ++ List(0L)).map(_.reduce(_+_))

    // broadcast that so everyone can use it
    val broadcastPreviousRecordsPerPartition =
      self.context.broadcast(previousRecordsByPartition)

    // Now, take each record, and add the previous records for its partition to 
    // its index within its partition
    partitionIndexedData.map(p => {
      val previousRecords = broadcastPreviousRecordsPerPartition.value(p._1._1)
      val indexInPartition = p._1._2
      val record = p._2
      (previousRecords + indexInPartition, record)
    })
  }
}

