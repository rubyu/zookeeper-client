package com.github.rubyu.zookeeper

import org.apache.log4j.Logger
import org.apache.zookeeper._
import org.apache.zookeeper.data._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.Watcher.Event._
import collection.JavaConversions._
import java.util.concurrent.CountDownLatch


class CallbackWatcher(callback: WatchedEvent => Unit) extends Watcher {
  private val log = Logger.getLogger(this.getClass.getName)

  override def process(event: WatchedEvent): Unit = {
    log.debug("event: class=%s, path=%s, state=%s, type=%s".format(
      getClass.getName, event.getPath, event.getState, event.getType))
    callback(event)
  }
}


class ZooKeeperClient(connectString: String, timeout: Int) {
  private val log = Logger.getLogger(this.getClass.getName)
  @volatile private var zk: ZooKeeper = null
  connect()

  def this(connectString: String) = this(connectString, 5000)
  
  private def connect() {
    val latch = new CountDownLatch(1)
    zk = new ZooKeeper(connectString, timeout, new CallbackWatcher({ event =>
      event.getState match {
        case KeeperState.SyncConnected => latch.countDown()
        case _ => {}
      }
    }))
    latch.await()
    log.info("connected")
  }

  def node(args: String*) = {
    val path = ("/" + args.mkString("/")).replaceAll("/+", "/")
    new ZooKeeperNode(this, path)
  }

  def handle: ZooKeeper = zk

  def close() = zk.close()

  def isAlive = zk.getState.isAlive
}


object ZooKeeperNode {
  implicit def zookeepernode2str(x: ZooKeeperNode) = x.path
}


class ZooKeeperNode(zc: ZooKeeperClient, val path: String) {
  private val log = Logger.getLogger(this.getClass.getName)

  private def zk: ZooKeeper = zc.handle

  lazy val isRoot = path == "/"

  /**
   * Returns the name of the node. When the node is the root, returns empty string.
   */
  lazy val name = {
    if (isRoot)
      ""
    else
      path.split("/").last
  }

  /**
   * Returns the parent of the node. When the node is the root, returns None.
   */
  lazy val parent = {
    val names = path.split("/").drop(1)
    names.length match {
      case n if n == 0 => 
        None
      case n if n == 1 => 
        Some(zc.node("/"))
      case n if n >= 2 =>
        Some(zc.node(names.dropRight(1): _*))
    }
  }

  /**
   * Returns a child that has the given name.
   */
  def child(name: String): ZooKeeperNode = {
    zc.node(this, name)
  }

  /**
   * Sets a watcher on the node.
   *
   * By default, once triggered, the watcher will be disappeared.
   * If 'permanent = true' given, new watcher will be set automatically on the same node
   * when the watcher triggered.
   *
   * By default, the watcher monitors only existing node.
   * If 'allowNoNode = true' given, the watcher will be able to monitor non existing node.
   *
   * This does not catch any exceptions.
   * But then 'allowNoNode = true', NoNodeException will not be thrown.
   */
  def watch(permanent: Boolean = false, allowNoNode: Boolean = false)
           (callback: WatchedEvent => Unit) {
    log.debug("set watch on the node; path=%s, permanent=%s, allowNoNode=%s".format(
      path, permanent, allowNoNode))
    def watcher = {
      new CallbackWatcher({ event =>
        callback(event)
        if (permanent)
          watch(permanent, allowNoNode)(callback)
      })
    }
    if (allowNoNode)
      zk.exists(path, watcher)
    else
      zk.getData(path, watcher, null)
  }

  /**
   * Sets a watcher on the node's children.
   *
   * By default, once triggered, the watcher will be disappeared.
   * If 'permanent = true' given, new watcher will be set automatically on the same node's children
   * when the watcher triggered.
   *
   * This does not catch any exceptions.
   */
  def watchChildren(permanent: Boolean = false)(callback: WatchedEvent => Unit) {
    log.debug("set watch on the node's children; path=%s, permanent=%s".format(path, permanent))
    def watcher = {
      new CallbackWatcher({ event =>
        callback(event)
        if (permanent)
          watchChildren(permanent)(callback)
      })
    }
    zk.getChildren(path, watcher)
  }

  /**
   * Creates a child node with the given name and returns the created node.
   *
   * By default, the node will be created as a persistent node.
   * If 'ephemeral = true' given, the node will be created as a ephemeral node.
   *
   * By default, the node will be created with no suffix.
   * If 'sequential = true' given, the node will be created with sequential
   * suffix, e.g., 'foo-0000000000'.
   *
   * This does not catch any exceptions.
   */
  def createChild(name: String, data: Array[Byte] = null,
                  ephemeral: Boolean = false, sequential: Boolean = false): ZooKeeperNode = {
    val mode = {
      if (ephemeral && sequential)
        CreateMode.EPHEMERAL_SEQUENTIAL
      else if (ephemeral)
        CreateMode.EPHEMERAL
      else if (sequential)
        CreateMode.PERSISTENT_SEQUENTIAL
      else
        CreateMode.PERSISTENT
    }
    val result = zk.create(child(name), data, Ids.OPEN_ACL_UNSAFE, mode)
    zc.node(result)
  }

  /**
   * Creates the node.
   *
   * This does not support to create a node with sequential flag.
   * If you want to do it, use 'createChild(name, data, sequential=true)'.
   *
   * This does not catch any exceptions.
   */
  def create(data: Array[Byte] = null, ephemeral: Boolean = false) {
    parent.get.createChild(name, data, ephemeral)
  }

  /**
   * Creates the node recursively.
   *
   * This catches only NodeExistsException, does not catch any other Exceptions.
   */
  def createRecursive() {
    if (isRoot || exists)
      return
    parent.get.createRecursive()
    log.debug("create recursive; path=%s".format(path))
    try {
      create()
    } catch {
      case e: KeeperException.NodeExistsException => {}
    }
  }

  /**
   * Returns the Stat of node.
   *
   * This does not catch any exceptions.
   */
  def stat = Option(zk.exists(path, false))

  /**
   * Returns true if the node is a ephemeral node.
   *
   * This does not catch any exceptions.
   */
  def isEphemeral = exists && stat.get.getEphemeralOwner != 0

  /**
   * Returns the sequential id of the node, if the node name has the sequential suffix.
   *
   * You are able to obtain sorted children by 'children.sortedBy',
   * e.g., 'children.sortedBy(_.sequentialId.get)'
   */
  lazy val sequentialId = {
    val id = name.takeRight(10)
    if (id matches "^\\d{10}$")
      Some(id)
    else
      None
  }

  def exists = stat.isDefined

  /**
   * Returns the children of the node.
   *
   * This does not catch any exceptions.
   */
  def children: List[ZooKeeperNode] = {
    zk.getChildren(path, false).map(child(_)).toList
  }

  class GetDataResponse(val stat: Stat, val data: Array[Byte])

  /**
   * Returns the data and Stat of the node.
   *
   * This does not catch any exceptions.
   */
  def get: GetDataResponse = {
    val stat = new Stat()
    val data = zk.getData(path, false, stat)
    new GetDataResponse(stat, data)
  }
  
  def set(data: Array[Byte]) = setIf(data, -1)

  /**
   * Sets the data for the node.
   *
   * This will be success only when the given version is equal to the remote version.
   *
   * This does not catch any exceptions.
   */
  def setIf(data: Array[Byte], version: Int) {
    zk.setData(path, data, version)
  }
  
  def delete() = deleteIf(-1)

  /**
   * Deletes the node.
   *
   * This will be success only when the given version is equal to the remote version.
   *
   * This does not catch any exceptions.
   */
  def deleteIf(version: Int) {
    zk.delete(path, version)
  }

  /**
   * Delete the node recursively.
   *
   * This catches only NoNodeException, does not catch any other Exceptions.
   */
  def deleteRecursive() {
    children.foreach{ _.deleteRecursive() }
    log.debug("delete recursive; path=%s".format(path))
    try {
      delete()
    } catch {
      case e: KeeperException.NoNodeException => {}
    }
  }

  /**
   * Returns true when the given instance is a ZooKeeperNode and
   * that's path is equal to this path.
   */
  override def equals(that: Any) = that match {
    case other: ZooKeeperNode => other.path == this.path
    case _ => false
  }
  
  override def toString() = path
}

