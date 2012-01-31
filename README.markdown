#Simple ZooKeeper client for Scala.

This library provides two classes *ZooKeeperClient* and *ZooKeeperNode*.

*ZooKeeperClient* is a management class of ZooKeeper. When a user requests
the *ZooKeeperNode*, *ZooKeeperClient* creates a instance of *ZooKeeperNode*
that has been given the reference to the *ZooKeeperClient*.

*ZooKeeperNode* is a management class of the node in ZooKeeper.
When a *ZooKeeperNode* is created, existence of it's node is not guaranteed.
If you create a *ZooKeeperNode* for a path,
it would not check existence of the node of the path and would not check even correctness of the path.
As *ZooKeeperNode* is just a wrapper of a path string.

Almost all function implemented in *ZooKeeperNode*, because most
ZooKeeper's functions are relating to the node.

###Notice
*ZooKeeperClient* **does not treat** the expiration of the ZooKeeper's session,
and so you should manage it in your code. See below:

> Library writers should be conscious of the severity of the expired state
and not try to recover from it. Instead libraries should return a fatal error.
[ZooKeeper/FAQ - Hadoop Wiki](http://wiki.apache.org/hadoop/ZooKeeper/FAQ "ZooKeeper/FAQ - Hadoop Wiki")

##USAGE

###Setup

    val zc = new ZooKeeperClient("localhost")
    
###Get the Wrapper for a Node

    val node = zc.node("path", "to", "node")

    node.path
    >> /path/to/node

###Create a Node

    val test = zc.node("test")
    test.exists
    >> false

    test.create()

    test.exists
    >> true

with data:

    val a = test.child("a")
    a.exists
    >> false

    a.create(data = "foo".getBytes)

    a.exists
    >> true
    a.get.data == "foo".getBytes
    >> true

ephemeral node:

    val b = test.child("b")
    b.exists
    >> false

    b.create(ephemeral = true)

    b.exists
    >> true
    b.isEphemeral
    >> true
    
sequential node:

    val c = test.createChild("c-", sequential = true)

    c.exists
    >> true
    c.path
    >> /test/c-0000000000
    c.sequentialId.get
    >> 0000000000
     
###Get/Set the data for a Node

    val d = test.child("d")
    d.create()
    d.get.stat.getVersion
    >> 0

    d.set("foo".getByes)

    d.get.stat.getVersion
    >> 1
    d.get.data == "foo".getBytes
    >> true

specify the version:

    val e = test.child("e")
    e.create()
    e.get.stat.getVersion
    >> 0

    e.setIf("foo".getBytes, version = 99)
    >> KeeperException.BadVersionException will be raised

    e.setIf("foo".getBytes, version = 0)

    e.get.stat.getVersion
    >> 1
    e.get.data == "foo".getBytes
    >> true

###Delete a Node

    val f = test.child("f")
    f.create()
    f.exists
    >> true

    f.delete()

    f.exists
    >> false

specify the version:

    val g = test.child("g")
    g.create()
    g.exists
    >> true
    g.get.stat.getVersion
    >> 0

    g.deleteIf(version = 99)
    >> KeeperException.BadVersionException will be raised

    g.deleteIf(version = 0)

    g.exists
    >> false
     
###Set watcher on a Node

    val h = test.child("h")
    h.create()

    h.watch { event =>
      println("called")
    }

    h.set("foo".getBytes)
    >> called
    h.set("bar".getBytes)
    // no output
    
option permanent:

    val i = test.child("i")
    i.create()

    i.watch(permanent = true) { event =>
      println("called")
    }

    i.set("foo".getBytes)
    >> called
    i.set("bar".getBytes)
    >> called

option allowNoNode:

    val j = test.child("j")
    //do not create

    j.watch { event =>
      println("called")
    }
    >> KeeperException.NoNodeException will be raised

    j.watch(allowNoNode = true) { event =>
      println("called")
    }

    j.create()
    >> called

###Set watcher on a Node's children

    val k = test.child("k")
    k.create()

    k.watchChildren { event =>
      println("called")
    }

    k.createChild("l")
    >> called
    k.createChild("m")
    // no output
    
option permanent:

    val n = test.child("n")
    n.create()

    n.watchChildren(permanent = true) { event =>
      println("called")
    }

    l.createChild("o")
    >> called
    l.createChild("p")
    >> called

###Get a Node's children

    val q = test.child("q")
    q.createChild("r")
    q.createChild("s")
    q.createChild("t")

    m.children foreach { println(_.name) }
    >> r
    >> s
    >> t

###Create a Node-Tree

    val u = test.child("u")
    val v = n.child("v")
    val w = r.child("w")
    u.exists
    >> false

    w.createRecursive()

    w.exists
    >> true

###Delete a Node-Tree

    val x = test.child("x")
    val y = t.child("y")
    val z = u.child("z")
    z.createRecursive()
    z.exists
    >> true

    x.deleteRecursive()

    x.exists
    >> false

##Please teach me English!

I'm writing comments, README and others, as part of the English lesson.
Please laugh at my mistakes and point out it!
