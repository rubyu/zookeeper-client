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
   * Return the name.
   * Root node has no name.
   */
  lazy val name = {
    if (isRoot)
      ""
    else
      path.split("/").last
  }

  /**
   * Return the parent.
   * Root node has no parent.
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
   * Set a watcher on the node and return true if the set-watch operation was
   * successful.
   *
   * If 'permanent = true' given, new watch will be set automatically on the same node
   * when the watch triggered.
   * If 'allowNoNode = true' given, the watch will be able to monitor non existing node.
   * If false, the watch is able to monitor only existing node.
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
   * Set a watcher on the node's children and return true if the set-watch operation
   * was successful.
   *
   * If 'permanent = true' given, new watch will be set automatically on the same node's
   * children when the watch triggered.
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
   * Create a child node with the given name and return the created node.
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
    val result = zk.create(zc.node(this, name), data, Ids.OPEN_ACL_UNSAFE, mode)
    zc.node(result)
  }

  /**
   * Create a node.
   * This does not support to create a sequential node,
   * if you want to do it, use 'createChild(name, data, sequential=true)'.
   */
  def create(data: Array[Byte] = null, ephemeral: Boolean = false) {
    parent.get.createChild(name, data, ephemeral)
  }

  /**
   * Create a node recursively.
   * This catches only NodeExistsException, does not catch any other Exception.
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

  def stat = Option(zk.exists(path, false))

  def isEphemeral = exists && stat.get.getEphemeralOwner != 0

  /**
   * Return the sequential id, if node name has the sequential suffix.
   * You are able to obtain sorted children by 'sortedBy',
   * e.g. 'children.sortedBy(_.sequentialId.get)'
   */
  lazy val sequentialId = {
    val id = name.takeRight(10)
    if (id matches "^\\d{10}$")
      Some(id)
    else
      None
  }

  def exists = stat.isDefined

  def children: List[ZooKeeperNode] = {
    zk.getChildren(path, false).map( zc.node(this, _) ).toList
  }

  class GetDataResponse(val stat: Stat, val data: Array[Byte])
  
  def get: GetDataResponse = {
    val stat = new Stat()
    val data = zk.getData(path, false, stat)
    new GetDataResponse(stat, data)
  }
  
  def set(data: Array[Byte]) = setIf(data, -1)

  def setIf(data: Array[Byte], version: Int) {
    zk.setData(path, data, version)
  }
  
  def delete() = deleteIf(-1)

  def deleteIf(version: Int) {
    zk.delete(path, version)
  }

  /**
   * Delete a node recursively.
   * This catches only NoNodeException, does not catch any other Exception.
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
   * Return true when the given instance is a ZooKeeperNode and
   * that's path is equal to this path.
   */
  override def equals(that: Any) = that match {
    case other: ZooKeeperNode => other.path == this.path
    case _ => false
  }
  
  override def toString() = path
}

