package com.sksamuel.elastic4s.http

import java.nio.charset.Charset

import cats.Functor
import org.elasticsearch.client.{ResponseException, ResponseListener}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.io.{Codec, Source}

/*
 *  A typeclass that can be used to convert from a ResponseListener callback into a generic effect type.
 */
trait FromListener[F[_], E] extends Functor[F] {
  def fromListener(f: E => Unit): F[HttpResponse]
}

object FromListener {
  def fromResponse(r: org.elasticsearch.client.Response): HttpResponse = {
    val entity = Option(r.getEntity).map { entity =>
      val contentEncoding =
        Option(entity.getContentEncoding).map(_.getValue).getOrElse("UTF-8")
      implicit val codec = Codec(Charset.forName(contentEncoding))
      val body           = Source.fromInputStream(entity.getContent).mkString
      HttpEntity.StringEntity(body, Some(contentEncoding))
    }
    val headers = r.getHeaders.map { header =>
      header.getName -> header.getValue
    }.toMap
    HttpResponse(r.getStatusLine.getStatusCode, entity, headers)
  }

  implicit def scalaFutureFromListenerInstance(
      implicit ec: ExecutionContext = ExecutionContext.Implicits.global)
    : FromListener[Future, ResponseListener] =
    new FromListener[Future, ResponseListener] {

      override def map[A, B](fa: Future[A])(f: A => B): Future[B] = fa.map(f)

      override def fromListener(f: ResponseListener => Unit): Future[HttpResponse] = {
        val p = Promise[HttpResponse]()
        f(new ResponseListener {

          override def onSuccess(r: org.elasticsearch.client.Response): Unit =
            p.trySuccess(fromResponse(r))
          override def onFailure(e: Exception): Unit = e match {
            case re: ResponseException => p.trySuccess(fromResponse(re.getResponse))
            case t                     => p.tryFailure(JavaClientExceptionWrapper(t))
          }
        })
        p.future
      }

    }

}
