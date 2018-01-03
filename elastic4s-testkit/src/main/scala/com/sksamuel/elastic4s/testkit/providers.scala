package com.sksamuel.elastic4s.testkit

import java.nio.file.{Path, Paths}
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import com.sksamuel.elastic4s.TcpClient
import com.sksamuel.elastic4s.embedded.{LocalNode, RemoteLocalNode}
import com.sksamuel.elastic4s.http.HttpClient

import scala.util.{Failure, Random, Success, Try}
import scala.util.control.NonFatal
import scala.concurrent.ExecutionContext.Implicits.global
import org.elasticsearch.client.ResponseListener

import scala.concurrent.Future

// LocalNodeProvider provides helper methods to create a local (embedded) node
trait LocalNodeProvider[F[_]] {
  // returns an embedded, started, node
  def getNode: LocalNode[F]
  def node: LocalNode[F] = getNode

  implicit lazy val client: TcpClient = getNode.tcp(false)
  implicit lazy val http: HttpClient[F, ResponseListener] = getNode.http(false)
}

// implementation of LocalNodeProvider that uses a single
// node instance for all classes in the same thread classloader.
trait ClassloaderLocalNodeProvider[F[_]] extends LocalNodeProvider[F] {

  private lazy val dir: Path  = Paths get System.getProperty("java.io.tmpdir")
  private lazy val home: Path = dir resolve UUID.randomUUID().toString

  override lazy val getNode: LocalNode[F] = localNode(0)

  private def localNode(timesTried: Int): LocalNode[F] =
    Try(LocalNode[F]("node_" + ClassLocalNodeProvider.counter.getAndIncrement(), home.toAbsolutePath.toString)) match {
      case Failure(ex: IllegalStateException) if timesTried < 5 =>
        ex.printStackTrace()
        localNode(timesTried + 1)
      case Success(okNode) => okNode
      case Failure(other)  => throw other
    }
}


trait DiscoveryLocalNodeProvider extends GenericDiscoveryLocalNodeProvider[Future]
// implementation of LocalNodeProvider that attempts to find local nodes already started
// and then connects to that, or creates a new one if one cannot be found
trait GenericDiscoveryLocalNodeProvider[F[_]] extends LocalNodeProvider[F] {

  override def getNode: LocalNode[F] = {

    try {

      // assume the local node is running on 9200
      val client = HttpClient[Future]("elasticsearch://localhost:9200")
      import com.sksamuel.elastic4s.http.ElasticDsl._
      val nodeinfo = client.execute(nodeInfo()).await.right.get.result
      val (id, node) = nodeinfo.nodes.head
      println(s"Found local node $id")

      val paths = node.settingsAsMap("path").asInstanceOf[Map[String, AnyRef]]
      val pathData = Paths get paths("data").toString
      val pathHome = Paths get paths("home").toString
      val pathRepo = Paths get paths("repo").toString
      new RemoteLocalNode(nodeinfo.clusterName, id, node.ip, node.http.publishAddress, node.transport.publishAddress, pathData, pathHome, pathRepo)

    } catch {
      case NonFatal(e) =>

        def tempDirectoryPath: Path = Paths get System.getProperty("java.io.tmpdir")
        def pathHome: Path = tempDirectoryPath resolve UUID.randomUUID().toString

        def createLocalNode(timesTried: Int): LocalNode[F] = {
          println(s"Creating new local node")
          Try(LocalNode[F]("localnode-cluster", pathHome.toAbsolutePath.toString)) match {
            case Failure(_: IllegalStateException) if timesTried < 5 => createLocalNode(timesTried + 1)
            case Success(okNode) => okNode
            case Failure(other) => throw other
          }
        }

        val node = createLocalNode(0)
        // allow time for the node to start up
        Thread.sleep(3000)
        node
    }
  }
}

// implementation of LocalNodeProvider that uses a single
// node instance for each class that mixes in this trait.
trait ClassLocalNodeProvider[F[_]] extends LocalNodeProvider[F] {

  private lazy val tempDirectoryPath: Path = Paths get System.getProperty("java.io.tmpdir")
  private lazy val pathHome: Path = tempDirectoryPath resolve UUID.randomUUID().toString

  override lazy val getNode = LocalNode[F](
    "node_" + ClassLocalNodeProvider.counter.getAndIncrement(),
    pathHome.toAbsolutePath.toString
  )
}

object ClassLocalNodeProvider {
  val counter = new AtomicLong(1)
}

trait AlwaysNewLocalNodeProvider[F[_]] extends LocalNodeProvider[F] {

  private def tempDirectoryPath: Path = Paths get System.getProperty("java.io.tmpdir")
  private def pathHome: Path = tempDirectoryPath resolve UUID.randomUUID().toString

  override def getNode: LocalNode[F] = {
    LocalNode(
      "node_" + Random.nextInt(),
      pathHome.toAbsolutePath.toString
    )
  }
}
