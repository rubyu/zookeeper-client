package com.github.rubyu.zookeeper

import org.apache.log4j.Logger
import org.apache.zookeeper._
import org.apache.zookeeper.data._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.Watcher.Event._
import collection.mutable.ListBuffer
import collection.JavaConversions._
import java.util.concurrent.CountDownLatch

/**
 *
 */
class CallbackWatcher(callback: WatchedEvent => Unit) extends Watcher {
  private val log = Logger.getLogger(this.getClass.getName)

  override def process(event: WatchedEvent): Unit = {
    log.debug("event: class=%s, path=%s, state=%s, type=%s",
      getClass.getName, event.getPath, event.getState, event.getType)
    callback(event)
    log.debug("callback applied")
  }
}

/**
 * volatile zk
 * handle
 * node(path, to, node)
 * private connect
 *
 */
class ZooKeeperClient(connectString: String, timeout: Int) {
  private val log = Logger.getLogger(this.getClass.getName)

  @volatile private var zk: ZooKeeper = null

  connect()
  
  private def connect() {
    val latch = new CountDownLatch(1)
    zk = new ZooKeeper(connectString, timeout, new CallbackWatcher({
      event =>
      event.getState match {
        case KeeperState.SyncConnected =>
          latch.countDown()
        case KeeperState.Expired =>
          connect()
        case _ => {}
      }
    }))
    latch.await()
  }

  def node(args: String*) = {
    val path = {
      if (args.head.startsWith("/"))
        args.mkString("/")
      else
        "/" + args.mkString("/")
    }
    new ZooKeeperNode(this, path)
  }

  def handle: ZooKeeper = zk

  def close() = zk.close()

  def isAlive = zk.getState.isAlive
}

/**
 * -private zc
 * -private zk = zc.handle
 * -path
 * -parent
 * -name
 * -watch(permanently=) {}
 * -WatchChildren(permanently=) {}
 * -exists
 * -create
 * -createRecursively
 * -stat
 * -children
 * -get
 * -update/updateIf(version=)
 * -delete/deleteIf(version=)
 * -deleteRecursively
 *
 */
class ZooKeeperNode(zc: ZooKeeperClient, val path: String) {
  private val log = Logger.getLogger(this.getClass.getName)

  private def zk: ZooKeeper = zc.handle

  def isRootNode = path == "/"

  def name = {
    if (isRootNode)
      path
    else
      path.split("/").last
  }
  
  def parent = {
    val arr = path.split("/").drop(1)
    arr.length match {
      case n if n == 0 => 
        None
      case n if n == 1 => 
        Some(zc.node("/"))
      case n if n >= 2 =>
        Some(zc.node(arr.dropRight(1): _*))
    }
  }

  def watch(callback: WatchedEvent => Unit, permanent: Boolean = false) {
    log.debug("set watch to node; path=%s, permanent=%s".format(path, permanent))
    zk.exists(path, new CallbackWatcher({ event =>
      callback(event)
      if (permanent) watch(callback, permanent)
    }))
  }

  def watchChildren(callback: WatchedEvent => Unit, permanent: Boolean = false) {
    log.debug("set watch to node's children; path=%s, permanent=%s".format(path, permanent))
    zk.getChildren(path, new CallbackWatcher({ event =>
      callback(event)
      if (permanent) watch(callback, permanent)
    }))
  }

  def create(data: Array[Byte] = null,
             mode: CreateMode = CreateMode.PERSISTENT): String = {
    zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode)
  }

  def createRecursive() {
    if (parent.isDefined && !parent.get.isRootNode)
      parent.get.createRecursive()
    log.debug("create recursive; path=%s".format(path))
    try {
      create()
    } catch {
      case e: KeeperException.NodeExistsException => {}
    }
  }

  def stat = Option(zk.exists(path, false))

  def exists = stat.isDefined

  def children: List[ZooKeeperNode] = {
    val buf = new ListBuffer[ZooKeeperNode]
    zk.getChildren(path, false) foreach { s =>
      buf += zc.node(path, s)
    }
    buf.toList
  }

  class GetResponse(val stat: Stat, val data: Array[Byte])
  
  def get: GetResponse = {
    val stat = new Stat()
    val data = zk.getData(path, false, stat)
    new GetResponse(stat, data)
  }
  
  def set(data: Array[Byte]) = setIf(data, -1)
  
  def setIf(data: Array[Byte], version: Int) {
    zk.setData(path, data, version)
  }
  
  def delete() = deleteIf(-1)
  
  def deleteIf(version: Int) {
    zk.delete(path, version)
  }
  
  def deleteRecursive() {
    children foreach { _.deleteRecursive() }
    log.debug("delete recursive; path=%s".format(path))
    delete()
  }

  /**
   * Returns true when given a ZooKeeperNode instance and that's path is equal to this path.
   */
  override def equals(that: Any) = that match {
    case other: ZooKeeperNode => other.path == this.path
    case _ => false
  }
  
  override def toString() = path
}

object ZooKeeperNode {
  implicit def zookeepernode2str(x: ZooKeeperNode) = x.path
}

