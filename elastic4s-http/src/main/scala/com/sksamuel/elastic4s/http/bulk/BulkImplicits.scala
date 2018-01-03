package com.sksamuel.elastic4s.http.bulk

import cats.{Functor, Show}
import com.sksamuel.elastic4s.bulk.BulkDefinition
import com.sksamuel.elastic4s.http._
import com.sksamuel.exts.Logging
import org.apache.http.entity.ContentType

import scala.concurrent.Future

trait BulkImplicits {

  implicit object BulkShow extends Show[BulkDefinition] {
    override def show(f: BulkDefinition): String = BulkBuilderFn(f).mkString("\n")
  }

  implicit object BulkExecutable extends HttpExecutable[BulkDefinition, BulkResponse] with Logging {

    override def execute[F[_], E](client: HttpRequestClient[F, E], bulk: BulkDefinition)(implicit E: FromListener[F, E]): F[HttpResponse] = {

      val rows = BulkBuilderFn(bulk)
      // es seems to require a trailing new line as well
      val entity = HttpEntity(rows.mkString("\n") + "\n", ContentType.APPLICATION_JSON.getMimeType)
      logger.debug("Sending bulk request")
      logger.debug(rows.mkString("\n"))

      val params = scala.collection.mutable.Map.empty[String, String]
      bulk.timeout.foreach(params.put("timeout", _))
      bulk.refresh.map(RefreshPolicyHttpValue.apply).foreach(params.put("refresh", _))

      client.async("POST", "/_bulk", params.toMap, entity)
    }
  }
}
