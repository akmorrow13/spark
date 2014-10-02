/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, QueryTest}
import org.apache.spark.sql.test._
import TestSQLContext._

case class RecordData1(start1: Long, end1: Long) extends Serializable
case class RecordData2(start2: Long, end2: Long) extends Serializable

class SQLRangeJoinSuite extends QueryTest {


  val sc = TestSQLContext.sparkContext
  val sqlContext = new SQLContext(sc)
  import sqlContext._

  test("joining non overlappings results into no entries"){

  val rdd1 = sc.parallelize(Seq((1L,5L), (2L,7L))).map(i => RecordData1(i._1, i._2))
  val rdd2 = sc.parallelize(Seq((11L,44L), (23L, 45L))).map(i => RecordData2(i._1, i._2))

  //val rdd1 = sc.parallelize(List((1L,"val1"), (10L,"val10"))).map(i => Record(i._1, i._2))
  //val rdd2 = sc.parallelize(List((50L, "val10"), (523L, "val523"))).map(i => RecordTest(i._1, i._2))
  rdd1.registerAsTable("t1")
  rdd2.registerAsTable("t2")
  checkAnswer(
    sql("select * from t1 RANGEJOIN t2 on OVERLAPS( (start1, end1), (start2, end2))"),
    Nil
  )




    /* val rdd1: RDD[Interval] = sc.parallelize(Seq(
       ReferenceRegion("chr1", 100L, 200L),
       ReferenceRegion("chr1", 400L, 600L),
       ReferenceRegion("chr1", 700L, 800L),
       ReferenceRegion("chr1", 900L, 1000L)))

     val rdd2: RDD[ReferenceRegion] = sc.parallelize(Seq(
       ReferenceRegion("chr2", 100L, 200L),
       ReferenceRegion("chr2", 400L, 600L),
       ReferenceRegion("chr1", 1100L, 1200L),
       ReferenceRegion("chr1", 1400L, 1600L)))

     assert(YetAnotherRegionJoin.overlapJoin(sc, rdd1, rdd2).count == 0)*/
   }

  /* sparkTest("test join with non-perfect overlapping regions") {
     val rdd1: RDD[ReferenceRegion] = sc.parallelize(Seq(
       ReferenceRegion("chr1", 100L, 200L),
       ReferenceRegion("chr1", 400L, 600L),
       ReferenceRegion("chr1", 700L, 800L),
       ReferenceRegion("chr1", 900L, 1000L)), 2)

     val rdd2: RDD[ReferenceRegion] = sc.parallelize(Seq(
       ReferenceRegion("chr1", 150L, 250L),
       ReferenceRegion("chr1", 300L, 500L),
       ReferenceRegion("chr1", 1100L, 1200L),
       ReferenceRegion("chr1", 1400L, 1600L)), 2)

     val j = YetAnotherRegionJoin.overlapJoin(sc, rdd1, rdd2).collect

     assert(j.size === 2)
     assert(j.forall(p => p._2.size == 1))
     assert(j.filter(p => p._1.start == 100L).size === 1)
     assert(j.filter(p => p._1.start == 100L).head._2.size === 1)
     assert(j.filter(p => p._1.start == 100L).head._2.head.start === 150L)
     assert(j.filter(p => p._1.start == 400L).size === 1)
     assert(j.filter(p => p._1.start == 400L).head._2.size === 1)
     assert(j.filter(p => p._1.start == 400L).head._2.head.start === 300L)
   }

   sparkTest("basic multi-join") {
     val rdd1: RDD[ReferenceRegion] = sc.parallelize(Seq(ReferenceRegion("chr1", 100L, 199L),
       ReferenceRegion("chr1", 200L, 299L),
       ReferenceRegion("chr1", 400L, 600L),
       ReferenceRegion("chr1", 10000L, 20000L)))

     val rdd2: RDD[ReferenceRegion] = sc.parallelize(Seq(ReferenceRegion("chr1", 150L, 250L),
       ReferenceRegion("chr1", 300L, 500L),
       ReferenceRegion("chr1", 500L, 700L),
       ReferenceRegion("chr2", 100L, 200L)))

     val j = YetAnotherRegionJoin.overlapJoin(sc, rdd1, rdd2).collect

     assert(j.size === 3)
     assert(j.filter(p => p._1.start == 100L).size === 1)
     assert(j.filter(p => p._1.start == 200L).size === 1)
     assert(j.filter(p => p._1.start <= 200L).forall(p => p._2.size == 1))
     assert(j.filter(p => p._1.start <= 200L).forall(p => p._2.head == ReferenceRegion("chr1", 150L, 250L)))
     assert(j.filter(p => p._1.start == 400L).size === 1)
     assert(j.filter(p => p._1.start == 400L).head._2.size === 2)
     assert(j.filter(p => p._1.start == 400L).head._2.filter(_ == ReferenceRegion("chr1", 300L, 500L)).size === 1)
     assert(j.filter(p => p._1.start == 400L).head._2.filter(_ == ReferenceRegion("chr1", 500L, 700L)).size === 1)
   }*/

 }
