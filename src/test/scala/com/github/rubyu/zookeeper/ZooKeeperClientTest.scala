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
    log.info("--test--")
    root.createRecursive()

  }

  def after {
    if (root.exists)
      root.deleteRecursive()
    log.info("--------")
  }

  "ZooKeeperNode" should {   

    "create a node recursively" in {
      root_foo_bar.createRecursive()
      root_foo_bar.exists must_== true

    }

    //TODO 普通のノードも sequentialId, isEphemeralあたりのチェック

    "create a persistent node" in {
      val node = zc.node(root, "node")
      node.create()
      node.exists must_== true
      node.isEphemeral must_== false
    }

    "create an ephemeral node" in {
      val node = zc.node(root, "ephemeral")
      node.create(ephemeral = true)
      node.exists must_== true
      node.isEphemeral must_== true
    }

    "create a persistent-sequential node" in {
      val node = zc.node(root, "seq-")
      val seq = node.createSequential()
      node.exists must_== false
      seq.exists must_== true
      seq.parent.get must_== root
      seq.sequentialId.get must_== "0000000000"
      seq.isEphemeral must_== false
    }

    "create an ephemeral-sequential node" in {
      val node = zc.node(root, "seq-")
      val seq = node.createSequential(ephemeral = true)
      node.exists must_== false
      seq.exists must_== true
      seq.parent.get must_== root
      seq.sequentialId.get must_== "0000000000"
      seq.isEphemeral must_== true
    }
    
    "create the node with specified data" in {
      val data = "test".getBytes
      val node = zc.node(root, "node")
      node.create(data)
      node.get.data must_== data
    }
    
    "delete the node if given version is equal to it's version" in {
      val node = zc.node(root, "node")
      node.create()
      node.deleteIf(version=1) must throwA[KeeperException.BadVersionException]
      node.deleteIf(version=0)
      node.exists must_== false
    }
    
    "return children" in {
      val a = zc.node(root, "a")
      val b = zc.node(root, "b")
      a.create()
      b.create()
      root.children.toSet must_== Set(a, b)
    }
    
    "return the name" in {
      root.name must_== "zookeeper-client-test"
      root_foo.name must_== "foo"
      root_foo_bar.name must_== "bar"
      zc.node("/").name must_== ""
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

    "set and get the data" in {
      val data = "test".getBytes
      val node = zc.node(root, "node")
      node.create()
      node.set(data)
      node.get.data must_== data
    }

    "set the data for a node if given version is equal to it's version" in {
      val data = "test".getBytes
      val node = zc.node(root, "node")
      node.create()
      node.setIf(data, version=1) must throwA[KeeperException.BadVersionException]
      node.setIf(data, version=0)
      node.get.data must_== data
    }

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

  def stat = Option(zk.exists(path, false))

     */

    "delete the node recursively" in {
      root_foo_bar.createRecursive()
      root_foo_bar.exists must_== true
      root.deleteRecursive()
      root.exists must_== false
    }
  }
}