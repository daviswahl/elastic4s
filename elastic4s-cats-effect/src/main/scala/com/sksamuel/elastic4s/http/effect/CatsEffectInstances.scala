package com.sksamuel.elastic4s.http.effect

import java.nio.charset.Charset

import cats.effect.Effect
import com.sksamuel.elastic4s.http.{
  FromListener,
  HttpEntity,
  HttpResponse,
  JavaClientExceptionWrapper
}
import org.elasticsearch.client.{ResponseException, ResponseListener}

import scala.io.{Codec, Source}

/*
 * Provides a `FromListener` instance for any kind with an `Effect` instance in scope,
 * Cats `IO` kind or (my end goal in all of this nonsense) a Catbird Rerunnable.
 */
object CatsEffectInstances {
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

  implicit def FromListenerForEffect[F[_]: Effect]: FromListener[F] = new FromListener[F] {
    override def fromListener(f: ResponseListener => Unit): F[HttpResponse] = {
      implicitly[Effect[F]].async { callback =>
        f(new ResponseListener {
          override def onSuccess(r: org.elasticsearch.client.Response): Unit =
            callback(Right(fromResponse(r)))

          override def onFailure(e: Exception): Unit = e match {
            case re: ResponseException => callback(Right(fromResponse(re.getResponse)))
            case t                     => callback(Left(JavaClientExceptionWrapper(t)))
          }
        })
      }
    }

    // Obtain our Functor requirement from Effect[F]
    override def map[A, B](fa: F[A])(f: A => B): F[B] = implicitly[Effect[F]].map(fa)(f)
  }
}
