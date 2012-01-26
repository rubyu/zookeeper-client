#Simple ZooKeeper client for Scala.

This library provides two important classes *ZooKeeperClient* for managing
a ZooKeeper instance and *ZooKeeperNode* for managing a node in ZooKeeper.
Almost all function implemented in *ZooKeeperNode*, because most
ZooKeeper's functions are relating to the node.


##ZooKeeperClient
The *ZooKeeperClient* **does not treat** expiration of the ZooKeeper's session,
and so you should manage it in your code. See below:

> Library writers should be conscious of the severity of the expired state
and not try to recover from it. Instead libraries should return a fatal error.
[ZooKeeper/FAQ - Hadoop Wiki](http://wiki.apache.org/hadoop/ZooKeeper/FAQ "ZooKeeper/FAQ - Hadoop Wiki")


##ZooKeeperNode
When a *ZooKeeperNode* created, existence of it's node not guaranteed.
If you create a *ZooKeeperNode* for a path,
it would not check node's existence and would not check even correctness of it's path.
As the *ZooKeeperNode* is just a wrapper of path string.


##Please teach me English!

I'm writing comments, README and others, as part of the English lesson.
Please laugh and point out my mistakes!


##USAGE
###Setup

    val zc = new ZooKeeperClient("localhost")
    
###Get the Wrapper for a Node
    val test = zc.node("zookeeper-client-test")
    test.path
    >> /zookeeper-client-test
    
    val a = test.child("a")
    a.path
    >> /zookeeper-client-test/a
    
    test.exists
    >> false
    a.exists
    >> false

###Create a Node

    test.create()
    a.create()
    
    test.exists
    >> true
    a.exists
    >> true
    
ephemeral node:

    val b = test.child("b")
    b.create(ephemeral = true)
    b.isEphemeral
    >> true
    
sequential node:

    val c = test.createChild("c-", sequential = true)
    c.path
    >> /zookeeper-client-test/c-0000000000
    c.sequentialId.get
    >> 0000000000
     
###Get/Set the data for a Node

    val data = "hoge".getBytes
    a.set(data)
    a.get() == data
    >> true
     
###Set watcher on a Node

    a.watch() { event =>
      println("called")
    }
    a.set(data)
    >> called
    a.set(data)
    // no output
    
permanent watcher:

    a.watch(permanent = true) { event =>
      println("called")
    }
    a.set(data)
    >> called
     a.set(data)
    >> called

###Set watcher on a Node's children

    a.watchChildren() { event =>
      println("called")
    }
    val d = a.createChild("d")
    >> called
    val e = a.createChild("e")
    // no output
    
permanent watcher:

    a.watchChildren(permanent = true) { event =>
      println("called")
    }
    val f = a.createChild("f")
    >> called
    val g = a.createChild("g")
    >> called

###Get a Node's children

    a.children foreach { println(_.name) }
    >> d
    >> e
    >> f
    >> g

###Create/Delete a Node-Tree

    val j = zc.node(d, "h", "i", "j")
    j.path
    >> /zookeeper-client-test/a/d/h/i/j
    j.createRecursive()
    j.exists
    >> true

    test.deleteRecursive()
    test.exists
    >> false




    
