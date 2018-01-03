package com.sksamuel.elastic4s.http.effect

import cats.effect.Effect
import com.sksamuel.elastic4s.http.{AsyncExecutor, HttpResponse}

/**
  * Provides a `FromListener` instance for any kind with an `Effect` instance in scope,
  */
object CatsEffectInstances {
  implicit def AsyncExecutorForEffect[F[_]: Effect]: AsyncExecutor[F] = new AsyncExecutor[F] {
    override def async(k: (Either[Exception, HttpResponse] => Unit) => Unit): F[HttpResponse] =
      Effect[F].async(k)
    override def map[A, B](fa: F[A])(f: A => B): F[B] = Effect[F].map(fa)(f)
  }
}
