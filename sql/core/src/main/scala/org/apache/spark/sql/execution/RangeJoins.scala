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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._

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
  def execute() = left.execute().map(_.copy()).cartesian(right.execute().map(_.copy())).map {
    case (l: Row, r: Row) => buildRow(l ++ r)
  }


}
