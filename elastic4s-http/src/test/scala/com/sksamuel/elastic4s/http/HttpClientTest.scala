package com.sksamuel.elastic4s.http

import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.elasticsearch.client.ResponseListener
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class HttpClientTest extends FlatSpec with Matchers with ElasticDsl {

  "HttpClient" should "throw an error when it cannot connect" in {
    intercept[JavaClientExceptionWrapper] {
      val client = HttpClient[Future](ElasticsearchClientUri("123", 1))
      client.execute {
        indexInto("a-index" / "a-type") id "a-id" fields Map("wibble" -> "foo")
      }.await(Duration.Inf)
    }
  }
}
