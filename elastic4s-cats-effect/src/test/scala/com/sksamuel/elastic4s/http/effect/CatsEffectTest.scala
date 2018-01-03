/*
 * Copyright 2014-2017 SignalPath, LLC - All Rights Reserved
 * Unauthorized dissemination or copying of these materials, in whole or in part and via any medium, is strictly prohibited.
 * Proprietary and confidential.
 */

package com.sksamuel.elastic4s.http.effect

import cats.effect.IO
import cats.effect.IO._
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticDsl
import com.sksamuel.elastic4s.http.effect.CatsEffectInstances._
import com.sksamuel.elastic4s.testkit.{DiscoveryLocalNodeProvider, GenericDiscoveryLocalNodeProvider}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try

class CatsEffectTest
    extends FlatSpec
    with Matchers
    with ElasticDsl
    with GenericDiscoveryLocalNodeProvider[IO] {

  Try {
    http
      .execute {
        deleteIndex("beer")
      }
  }

  http
    .execute {
      createIndex("beer").mappings {
        mapping("lager").fields(
          textField("name").stored(true),
          textField("brand").stored(true),
          textField("ingredients").stored(true)
        )
      }
    }
    .unsafeRunSync()

  val effect = http.execute {
    indexInto("beer/lager") fields (
      "name"        -> "bud lite",
      "brand"       -> "bud",
      "ingredients" -> Seq("hops", "barley", "water", "yeast")
    ) id "8" refresh RefreshPolicy.Immediate
  }

  "An IO Effect" should "be deferred until run" in {
    val doGet = http
      .execute {
        get("8") from "beer"
      }
    val result = doGet.unsafeRunSync().right.get.result

    result.exists shouldBe false

    effect.unsafeRunSync()

    // the effect is not memoized
    val result2 = doGet.unsafeRunSync().right.get.result
    result2.exists shouldBe true
  }
}
