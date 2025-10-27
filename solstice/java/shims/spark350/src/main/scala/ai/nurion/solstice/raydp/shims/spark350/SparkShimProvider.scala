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

package ai.nurion.solstice.raydp.shims.spark350

import ai.nurion.solstice.raydp.shims.{Spark350Shims, SparkShimDescriptor, SparkShims}

object SparkShimProvider {
  private val SPARK350_DESCRIPTOR = SparkShimDescriptor(3, 5, 0)
  private val SPARK351_DESCRIPTOR = SparkShimDescriptor(3, 5, 1)
  private val SPARK352_DESCRIPTOR = SparkShimDescriptor(3, 5, 2)
  private val SPARK353_DESCRIPTOR = SparkShimDescriptor(3, 5, 3)
  private val SPARK354_DESCRIPTOR = SparkShimDescriptor(3, 5, 4)
  private val SPARK355_DESCRIPTOR = SparkShimDescriptor(3, 5, 5)
  private val SPARK356_DESCRIPTOR = SparkShimDescriptor(3, 5, 6)
  private val DESCRIPTOR_STRINGS = Seq(s"$SPARK350_DESCRIPTOR", s"$SPARK351_DESCRIPTOR", s"$SPARK352_DESCRIPTOR",
    s"$SPARK353_DESCRIPTOR", s"$SPARK354_DESCRIPTOR", s"$SPARK355_DESCRIPTOR", s"$SPARK356_DESCRIPTOR")
  val DESCRIPTOR: SparkShimDescriptor = SPARK350_DESCRIPTOR
}

class SparkShimProvider extends ai.nurion.solstice.raydp.shims.SparkShimProvider {
  def createShim: SparkShims = {
    new Spark350Shims()
  }

  def matches(version: String): Boolean = {
    SparkShimProvider.DESCRIPTOR_STRINGS.contains(version)
  }
}
