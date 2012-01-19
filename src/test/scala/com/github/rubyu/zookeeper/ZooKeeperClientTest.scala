package com.github.rubyu.zookeeper

import org.specs2.mutable.Specification
import org.apache.zookeeper._
import org.apache.zookeeper.data._
import org.apache.zookeeper.ZooDefs.Ids
import org.specs2.specification._
import org.apache.log4j.Logger


class ZooKeeperNodeTest extends Specification with BeforeAfterExample {
  private val log = Logger.getLogger(this.getClass.getName)

  val zc = new ZooKeeperClient("192.168.0.100", 1000)
  val root = zc.node("zookeeper-client-test")
  val root_foo = zc.node(root, "foo")
  val root_foo_bar = zc.node(root, "foo", "bar")

  def before {
    root.createRecursive()
  }

  def after {
    if (root.exists)
      root.deleteRecursive()
  }

  "ZooKeeperNode" should {   

    "create recursive a node" in {
      root_foo_bar.createRecursive()
      root_foo_bar.exists must_== true
    }

    "create a node" in {
      val node = zc.node(root, "node")
      node.create()
      node.exists must_== true
    }

    "create a ephemeral node" in {
      val node = zc.node(root, "ephemeral")
      node.create(mode=CreateMode.EPHEMERAL)
      node.exists must_== true
    }

    //sequence
    
    "return the name" in {
      root.name must_== "zookeeper-client-test"
      root_foo.name must_== "foo"
      root_foo_bar.name must_== "bar"
      zc.node("/").name must_== "/"
    }

    "return the parent, the parent ... until the root" in {
      var node = root_foo_bar.parent.get
      node must_== root_foo
      node = node.parent.get
      node must_== root
      node = node.parent.get
      node must_== zc.node("/")
      node.parent must_== None
    }

    //"retrun a Stat" in {}

    /*

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

  def create(data: Array[Byte], acl: java.util.List[ACL], createMode: CreateMode): String = {
    zk.create(path, data, acl, createMode)
  }

  def createRecursive() {
    val nodes = new ListBuffer[ZooKeeperNode]
    var node = this
    do {
      node +=: nodes
      node = node.parent.get
    } while (!node.isRootNode)
    nodes foreach { node =>
      try {
        log.debug("creating %s".format(node))
        node.create(null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      } catch {
        case e: KeeperException.NodeExistsException => {}
      }
    }
  }

  def stat = Option(zk.exists(path, false))

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

     */

    "deleteRecursive a node" in {
      root_foo_bar.createRecursive()
      root_foo_bar.exists must_== true
      root.deleteRecursive()
      root.exists must_== false
    }
  }
}