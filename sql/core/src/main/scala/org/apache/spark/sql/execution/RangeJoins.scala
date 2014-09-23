/*
 * Copyright (c) 2014. Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution

import scala.collection.mutable.{ArrayBuffer, BitSet}


import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._



@DeveloperApi
sealed abstract class BuildSide2

@DeveloperApi
case object BuildLeft2 extends BuildSide2

@DeveloperApi
case object BuildRight2 extends BuildSide2

@DeveloperApi
case class RangeJoin(left: SparkPlan, right: SparkPlan, condition: Seq[Expression]) extends BinaryNode{
  def output = left.output ++ right.output

  //This is just a space holder here. I need to come back
  //with an actual implementation
  lazy val (buildPlan, streamedPlan) = (left, right)

  lazy val (buildKeys, streamedKeys) = (List(condition(0),condition(1)), List(condition(0), condition(1)))

  @transient lazy val buildKeyGenerator = new Projection(buildKeys, buildPlan.output)

  def execute() = {

    val v1 = left.execute()
    val currentRow = v1.first()
    println("entire condition: " + condition.toString)
    println("buildKeys: " + buildKeys.toString())
    println("buildPlan.output: " + buildPlan.output)
    println("currentRow " + currentRow)
    val keysOfRow = buildKeyGenerator(currentRow)
    println("keys: " + keysOfRow.toString())
    //v1.map(_.getString(1)).collect().foreach(println)
    val v2 = v1.map(_.copy())
    val t = InterpretedPredicate(Some(condition(0)).map(c => BindReferences.bindReference(c, left.output ++ right.output)).getOrElse(Literal(true)))
    println(t.toString())

    //val keys1 = v2.map(condition(0).eval(_))
    //keys1.collect.foreach(println)

    val v3 = v2.cartesian(right.execute().map(_.copy()))
    val v4 = v3.map {
      case (l: Row, r: Row) => buildRow(l ++ r)
    }
    v4
  }


}
