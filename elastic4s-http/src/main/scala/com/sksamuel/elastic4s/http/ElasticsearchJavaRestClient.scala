package com.sksamuel.elastic4s.http

import cats.Functor
import org.apache.http.client.config.RequestConfig
import org.apache.http.entity.{ContentType, FileEntity, InputStreamEntity, StringEntity}
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClientBuilder.{HttpClientConfigCallback, RequestConfigCallback}
import org.elasticsearch.client.{ResponseListener, RestClient}

case class JavaClientExceptionWrapper(t: Throwable) extends RuntimeException

// an implementation of the elastic4s HttpRequestClient that wraps the elasticsearch java client
class ElasticsearchJavaRestClient[F[_]](client: RestClient) extends HttpRequestClient[F, ResponseListener] {

  import scala.collection.JavaConverters._


  override def async(method: String,
                     endpoint: String,
                     params: Map[String, Any])(implicit E: FromListener[F, ResponseListener]): F[HttpResponse] = {
    logger.debug(s"Executing elastic request $method:$endpoint?${params.map { case (k, v) => k + "=" + v }.mkString("&")}")
    val callback = client.performRequestAsync(method, endpoint, params.mapValues(_.toString).asJava, _: ResponseListener)
      E.fromListener(callback)
  }

  override def async(method: String,
                     endpoint: String,
                     params: Map[String, Any],
                     entity: HttpEntity)(implicit E: FromListener[F, ResponseListener]): F[HttpResponse] = {
    logger.debug(s"Executing elastic request $method:$endpoint?${params.map { case (k, v) => k + "=" + v }.mkString("&")}")

    val apacheEntity = entity match {
      case e: HttpEntity.StringEntity =>
        logger.debug(e.content)
        new StringEntity(e.content, ContentType.APPLICATION_JSON)
      case e: HttpEntity.InputStreamEntity =>
        logger.debug(e.content.toString)
        new InputStreamEntity(e.content, ContentType.APPLICATION_JSON)
      case e: HttpEntity.FileEntity =>
        logger.debug(e.content.toString)
        new FileEntity(e.content, ContentType.APPLICATION_JSON)
    }

    val callback = client.performRequestAsync(
      method,
      endpoint,
      params.mapValues(_.toString).asJava,
      apacheEntity,
      _: ResponseListener)
    E.fromListener(callback)
  }

  override def close(): Unit = client.close()
}

/**
  * RequestConfigCallback that performs a no-op on the given RequestConfig.Builder.
  *
  * Used as a default parameter to the HttpClient when no custom request
  * configuration is needed.
  *
  */
object NoOpRequestConfigCallback extends RequestConfigCallback {
  override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = requestConfigBuilder
}

/**
  * HttpAsyncClientBuilder that performs a no-op on the given HttpAsyncClientBuilder
  *
  * Used as a default parameter to the HttpClient when no custom HttpAsync
  * configuration is needed.
  *
  */
object NoOpHttpClientConfigCallback extends HttpClientConfigCallback {
  override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = httpClientBuilder
}
