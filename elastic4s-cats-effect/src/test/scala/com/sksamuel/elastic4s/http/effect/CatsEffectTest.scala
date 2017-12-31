/*
 * Copyright 2014-2017 SignalPath, LLC - All Rights Reserved
 * Unauthorized dissemination or copying of these materials, in whole or in part and via any medium, is strictly prohibited.
 * Proprietary and confidential.
 */

package com.sksamuel.elastic4s.http.effect

import cats.effect.IO._
import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.ElasticDsl
import com.sksamuel.elastic4s.http.effect.CatsEffectInstances._
import com.sksamuel.elastic4s.testkit.DiscoveryLocalNodeProvider
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Try

class CatsEffectTest
    extends FlatSpec
    with Matchers
    with ElasticDsl
    with DiscoveryLocalNodeProvider {

  Try {
    http
      .execute {
        deleteIndex("beer")
      }
      .unsafeRunSync()
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
    bulk(
      indexInto("beer/lager") fields (
        "name"        -> "coors light",
        "brand"       -> "coors",
        "ingredients" -> Seq("hops", "barley", "water", "yeast")
      ) id "4",
      indexInto("beer/lager") fields (
        "name"        -> "bud lite",
        "brand"       -> "bud",
        "ingredients" -> Seq("hops", "barley", "water", "yeast")
      ) id "8"
    ).refresh(RefreshPolicy.Immediate)
  }

  "An IO Effect" should "be deferred until executed" in {

    Thread.sleep(1000) // ensure the effect is not being run
    val resp = http
      .execute {
        get("8") from "beer"
      }
      .unsafeRunSync()
      .right
      .get
      .result

    resp.exists shouldBe false
    resp.id shouldBe "8"

    effect.unsafeRunSync()

    val resp2 = http
      .execute {
        get("8") from "beer"
      }
      .unsafeRunSync()
      .right
      .get
      .result

    resp2.exists shouldBe true
    resp2.id shouldBe "8"
  }
}
