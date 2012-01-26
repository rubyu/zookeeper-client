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
        root_foo.exists must_== false
        root_foo_bar.createRecursive()
        root_foo_bar.exists must_== true
      }
      "a persistent node" in {
        val node = zc.node(root, "node")
        node.create()
        node.parent.get must_== root
        node.exists must_== true
        node.isEphemeral must_== false
      }
      "a ephemeral node" in {
        val node = zc.node(root, "node")
        node.create(ephemeral = true)
        node.parent.get must_== root
        node.exists must_== true
        node.isEphemeral must_== true
      }
      "a node with specified data" in {
        val data = "test".getBytes
        val node = zc.node(root, "node")
        node.create(data)
        node.get.data must_== data
      }
    }

    "createChild" in {
      "a persistent node" in {
        val node = root.createChild("node")
        node.parent.get must_== root
        node.exists must_== true
        node.isEphemeral must_== false
      }
      "an ephemeral node" in {
        val node = root.createChild("node", ephemeral = true)
        node.parent.get must_== root
        node.exists must_== true
        node.isEphemeral must_== true
      }
      "a persistent-sequential node" in {
        val node = root.createChild("seq-", sequential = true)
        node.parent.get must_== root
        node.exists must_== true
        node.sequentialId.get must_== "0000000000"
        node.isEphemeral must_== false
      }
      "an ephemeral-sequential node" in {
        val node = root.createChild("seq-", sequential = true, ephemeral = true)
        node.parent.get must_== root
        node.exists must_== true
        node.sequentialId.get must_== "0000000000"
        node.isEphemeral must_== true
      }
      "a node with specified data" in {
        val data = "test".getBytes
        val node = root.createChild("node", data)
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

    "set watch on" in {
      val node = zc.node(root, "node")
      def create() = {
        node.create()
        Thread.sleep(10)
      }
      def update() = {
        node.set("dummy".getBytes)
        Thread.sleep(10)
      }
      def createChild(name: String) = {
        node.createChild(name)
        Thread.sleep(10)
      }
      "a non-existing node's children" in {
        "with no option" in {
          node.watchChildren() { event =>
          } must_== false
        }
        "with option permanent=true" in {
          node.watchChildren(permanent = true) { event =>
          } must_== false
        }
      }
      "an existing node's children" in {
        "with no option" in {
          node.create()
          var count = 0
          node.watchChildren() { event =>
            count += 1
          } must_== true
          createChild("a")
          createChild("b")
          count must_== 1
        }
        "with option permanent=true" in {
          node.create()
          var count = 0
          node.watchChildren(permanent = true) { event =>
            count += 1
          } must_== true
          createChild("a")
          createChild("b")
          count must_== 2
        }
      }

      "a non existing node" in {
        "with no option" in {
          node.watch() { event =>
          } must_== false
        }
        "with option permanent=true" in {
          node.watch(permanent = true) { event =>
          } must_== false
        }
        "with option allowNoNode=true" in {
          var count = 0
          node.watch(allowNoNode = true) { event =>
            count += 1
          } must_== true
          create()
          update()
          count must_== 1
        }
        "with option permanent=true and allowNoNode=true" in {
          var count = 0
          node.watch(permanent = true, allowNoNode = true) { event =>
            count += 1
          } must_== true
          create()
          update()
          count must_== 2
        }
      }

      "an existing node" in {
        "with no option" in {
          node.create()
          var count = 0
          node.watch() { event =>
            count += 1
          } must_== true
          update()
          update()
          count must_== 1
        }
        "with option permanent=true" in {
          node.create()
          var count = 0
          node.watch(permanent = true) { event =>
            count += 1
          } must_== true
          update()
          update()
          count must_== 2
        }
        "with option allowNoNode=true" in {
          node.create()
          var count = 0
          node.watch(allowNoNode = true) { event =>
            count += 1
          } must_== true
          update()
          update()
          count must_== 1
        }
        "with option permanent=true and allowNoNode=true" in {
          node.create()
          var count = 0
          node.watch(permanent = true, allowNoNode = true) { event =>
            count += 1
          } must_== true
          update()
          update()
          count must_== 2
        }
      }
    }
  }
}