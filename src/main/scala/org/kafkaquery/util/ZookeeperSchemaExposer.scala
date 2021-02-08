package org.kafkaquery.util

import java.util.concurrent.CountDownLatch

import org.apache.avro.Schema
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper._

import scala.collection.JavaConverters._

/** Exposes (Avro) schema's to ZooKeeper.
  *
  * @param host the server host of ZooKeeper.
  * @param root the root node of the schema's
  *             NOTE: Must start with / and only contain one '/'
  *             DEFAULT: /codefeedr:schemas
  */
class ZookeeperSchemaExposer(
    host: String,
    root: String = "/codefeedr:schemas"
) {

  private var client: ZooKeeper = _

  private val connectionLatch = new CountDownLatch(1)

  // Connect to zk.
  connect()

  /** Connect with ZK server. */
  private def connect(): Unit = {
    client = new ZooKeeper(
      host,
      5000,
      (event: WatchedEvent) => {
        if (event.getState eq KeeperState.SyncConnected)
          connectionLatch.countDown()
      }
    )

    connectionLatch.await()

    //if parent doesn't exist create it
    val exists = client.exists(root, false)

    if (exists == null) {
      //create parent node
      client.create(
        root,
        Array(),
        ZooDefs.Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT
      )
    }
  }

  def getHost: String = host

  /** Stores a schema bound to a subject.
    *
    * @param schema  The schema belonging to that topic.
    * @param subject The subject belonging to that schema.
    * @return True if correctly saved.
    */
  def put(schema: Schema, subject: String): Boolean = {
    val path = s"$root/$subject"

    //check if already exist, if so update data
    val exists = client.exists(path, false)
    if (exists != null) {
      client.setData(path, schema.toString(true).getBytes(), -1)
      return true
    }

    //create new node and set if it doesn't exist
    val createdPath = client.create(
      path,
      schema.toString(true).getBytes,
      ZooDefs.Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT
    )

    path == createdPath
  }

  /** Get a schema based on a subject.
    *
    * @param subject The subject the schema belongs to.
    * @return None if no schema is found or an invalid schema. Otherwise it returns the schema.
    */
  def get(subject: String): Option[Schema] = {
    try {
      //get the data from ZK
      val data = client.getData(s"$root/$subject", null, null)

      //parse the schema and return
      parse(new String(data))
    } catch {
      case _: Throwable => None //if path is not found
    }
  }

  /** Deletes a Schema.
    *
    * @param subject The subject the schema belongs to.
    * @return True if successfully deleted, otherwise false.
    */
  def delete(subject: String): Boolean = {
    try {
      client.delete(s"$root/$subject", -1)
    } catch {
      case _: Throwable =>
        return false //if path doesn't exist or there is no data
    }

    true
  }

  /** Deletes all the schemas */
  def deleteAll(): Unit = {
    val exists = client.exists(s"$root", false)

    //if not exists then return
    if (exists == null) return

    //get all children
    val children = client.getChildren(s"$root", false)

    //delete children
    children.asScala
      .foreach(x => client.delete(s"$root/$x", -1))

    //delete root afterwards
    client.delete(s"$root", -1)
  }

  /** Getter for all children in zookeeper path.
    * @return list of all children in zookeeper path
    */
  def getAllChildren: List[String] = {
    val exists = client.exists(s"$root", false)

    //if not exists then return
    if (exists == null) return null

    //get all children
    val children = client.getChildren(s"$root", false)

    children.asScala.toList
  }

  /** Tries to parse a String into a Schema.
    *
    * @param schemaString The schema string.
    * @return An option of a Schema.
    */
  def parse(schemaString: String): Option[Schema] = {
    try {
      val schema = new Schema.Parser().parse(schemaString)
      Some(schema)
    } catch {
      case _: Throwable => None
    }
  }
}
