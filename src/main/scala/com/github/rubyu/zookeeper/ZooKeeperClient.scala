package com.github.rubyu.zookeeper

import org.apache.log4j.Logger
import org.apache.zookeeper._
import org.apache.zookeeper.data._
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.Watcher.Event._
import collection.mutable.ListBuffer
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

  //TODO equals を handleのgetSessionIdで判定？

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

  def isRoot = path == "/"

  def name = {
    if (isRoot)
      ""
    else
      path.split("/").last
  }
  
  def parent = {
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
   * TODO test
   * TODO ノードが存在しない場合のWatcherの設定の有無が曖昧。
   * existsのときは、ノードがなくてもWatcherは設定され、create/delete/updateのいずれでもcallbackが返る。
   * getDataのとき、ノードが無ければWatcherは設定されない。delete/updateでcallbackが返る。
   *
   * 無条件でWatcherを設定 watch
   * ノードが存在すれば設定 watch
   *
   * TODO watch(permanent=true) {} はできる？
   * watch(permanent=false)(callback ...)にする必要あり？
   * できるなら
   * watch(ifExist=true)
   *
   * permanentでNoNodeになった際はどういう扱い？ 例外か？
   *  allowNoNodeで制御？
   *
   * TODO watch(allowNoNode=false, permanent=true) {event => }
   *
   * あるいは、永続をここで制御しない？
   *
   * TODO 設定されたか否かを返す
   *
   * TODO () => Unitとかも？
   *
   */
  def watch(callback: WatchedEvent => Unit, permanent: Boolean = false) {
    log.debug("set watch to node; path=%s, permanent=%s".format(path, permanent))
    zk.exists(path, new CallbackWatcher({ event =>
      callback(event)
      if (permanent) watch(callback, permanent)
    }))
  }

  /**
   *ノードが無ければWatcherは設定されない。ノードのdelete、子のcreate/deleteがcallbackされる。
   *
   * TODO test
   * TODO 設定されたか否かを返す
   */
  def watchChildren(callback: WatchedEvent => Unit, permanent: Boolean = false) {
    log.debug("set watch to node's children; path=%s, permanent=%s".format(path, permanent))
    zk.getChildren(path, new CallbackWatcher({ event =>
      callback(event)
      if (permanent) watchChildren(callback, permanent)
    }))
  }

  /**
   * Create a child node with the given name.
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
   * if you want to do it, use createChild(name, data, sequential=true).
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
  def sequentialId = {
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

