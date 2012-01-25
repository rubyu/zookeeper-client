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
    val path = {
      if (args.head.startsWith("/"))
        args.mkString("/")
      else
        "/" + args.mkString("/")
    }
    new ZooKeeperNode(this, path)
  }

  //TODO equals を handleのgetSessionIdで判定？

  def handle: ZooKeeper = zk

  def close() = zk.close()

  def isAlive = zk.getState.isAlive
}

/**
 *
 */

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

  def create(data: Array[Byte] = null, ephemeral: Boolean = false) {
    val mode = {
      if (ephemeral)
        CreateMode.EPHEMERAL
      else
        CreateMode.PERSISTENT
    }
    zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode)
  }

  def createSequential(data: Array[Byte] = null, ephemeral: Boolean = false): ZooKeeperNode = {
    val mode = {
      if (ephemeral)
        CreateMode.EPHEMERAL_SEQUENTIAL
      else
        CreateMode.PERSISTENT_SEQUENTIAL
    }
    zc.node(zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode))
  }

  def createRecursive() {
    parent match {
      case Some(node) if !node.isRoot => node.createRecursive()
      case _ => {}
    }
    log.debug("create recursive; path=%s".format(path))
    try {
      create()
    } catch {
      case e: KeeperException.NodeExistsException => {}
    }
  }

  def stat = Option(zk.exists(path, false))

  def isEphemeral = exists && stat.get.getEphemeralOwner != 0

  def sequentialId = {
    val id = name.takeRight(10)
    if (id matches  "^\\d+$")
      Some(id)
    else
      None
  }

  def exists = stat.isDefined

  def children: List[ZooKeeperNode] = {
    val buf = new ListBuffer[ZooKeeperNode]
    zk.getChildren(path, false) foreach { buf += zc.node(path, _) }
    buf.toList
  }

  /**
   * TODO children(sort=true)
   * 末尾でソート、全てがsequentialでなければエラー
   */

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

