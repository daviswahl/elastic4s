package com.sksamuel.elastic4s.http.effect

import cats.effect.Effect
import com.sksamuel.elastic4s.http.{FromListener, HttpResponse, JavaClientExceptionWrapper}
import org.elasticsearch.client.{ResponseException, ResponseListener}

/*
 * Provides a `FromListener` instance for any kind with an `Effect` instance in scope,
 * Cats `IO` kind or (my end goal in all of this nonsense) a Catbird Rerunnable.
 */
object CatsEffectInstances {
  implicit def FromListenerForEffect[F[_]: Effect]: FromListener[F] = new FromListener[F] {
    override def fromListener(f: ResponseListener => Unit): F[HttpResponse] = {
      Effect[F].async { callback =>
        f(new ResponseListener {
          override def onSuccess(r: org.elasticsearch.client.Response): Unit =
            callback(Right(FromListener.fromResponse(r)))

          override def onFailure(e: Exception): Unit = e match {
            case re: ResponseException => callback(Right(FromListener.fromResponse(re.getResponse)))
            case t                     => callback(Left(JavaClientExceptionWrapper(t)))
          }
        })
      }
    }

    // Obtain our Functor requirement from Effect[F]
    override def map[A, B](fa: F[A])(f: A => B): F[B] = implicitly[Effect[F]].map(fa)(f)
  }
}
