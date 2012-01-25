package com.github.rubyu.zookeeper

import org.specs2.mutable.Specification
import org.apache.zookeeper._
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
    "create" in {
      "a node recursively" in {
        root_foo_bar.createRecursive()
        root_foo_bar.exists must_== true

      }
      "a persistent node" in {
        val node = zc.node(root, "node")
        node.create()
        node.exists must_== true
        node.isEphemeral must_== false
      }
      "an ephemeral node" in {
        val node = zc.node(root, "ephemeral")
        node.create(ephemeral = true)
        node.exists must_== true
        node.isEphemeral must_== true
      }
      "a persistent-sequential node" in {
        val node = zc.node(root, "seq-")
        val seq = node.createSequential()
        node.exists must_== false
        seq.exists must_== true
        seq.parent.get must_== root
        seq.sequentialId.get must_== "0000000000"
        seq.isEphemeral must_== false
      }
      "an ephemeral-sequential node" in {
        val node = zc.node(root, "seq-")
        val seq = node.createSequential(ephemeral = true)
        node.exists must_== false
        seq.exists must_== true
        seq.parent.get must_== root
        seq.sequentialId.get must_== "0000000000"
        seq.isEphemeral must_== true
      }
      "a node with specified data" in {
        val data = "test".getBytes
        val node = zc.node(root, "node")
        node.create(data)
        node.get.data must_== data
      }
    }
    "delete" in {
      "a node recursively" in {
        root_foo_bar.createRecursive()
        root_foo_bar.exists must_== true
        root.deleteRecursive()
        root.exists must_== false
      }
      "a node if given version is equal to it's version" in {
        val node = zc.node(root, "node")
        node.create()
        node.deleteIf(version=1) must throwA[KeeperException.BadVersionException]
        node.deleteIf(version=0)
        node.exists must_== false
      }
    }
    "return the sequentialId" in {
      "when node has sequential suffix" in {
        zc.node(root, "seq-0000000000").sequentialId.get must_== "0000000000"
      }
      "throw exception when node does not have sequential suffix" in {
        zc.node(root, "node").sequentialId.get must throwA[NoSuchElementException]
      }
    }
    "return the children" in {
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
    "return the parent, the parent ..." in {
      var node = root_foo_bar
      def goUpper() = node = node.parent.get
      goUpper()
      node must_== root_foo
      goUpper()
      node must_== root
      goUpper()
      node must_== zc.node("/")
      goUpper() must throwA[NoSuchElementException]
    }
    "set and get the data for" in {
      "a node" in {
        val data = "test".getBytes
        val node = zc.node(root, "node")
        node.create()
        node.set(data)
        node.get.data must_== data
      }
      "a node if given version is equal to it's version" in {
        val data = "test".getBytes
        val node = zc.node(root, "node")
        node.create()
        node.setIf(data, version=1) must throwA[KeeperException.BadVersionException]
        node.setIf(data, version=0)
        node.get.data must_== data
      }
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

  }
}