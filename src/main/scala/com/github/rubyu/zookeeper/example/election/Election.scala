package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper.ZooKeeperNode
import com.github.rubyu.zookeeper.example.lock.{PermanentLock, CachedChildren}

object Election {
  implicit def zookeepernode2election(target: ZooKeeperNode) = new Election(target)
}

class Election(protected val target: ZooKeeperNode) extends PermanentLock {
  protected val prefix = "election-%s-".format(target.client.handle.getSessionId)
  protected val entries = new CachedChildren(
    target.children.sortBy(_.sequentialId.get)
  )

  protected def entry = {
    entries.get.find(_.name.startsWith(prefix)) match {
      case Some(node) =>
        node
      case None =>
        create()
    }
  }

  enter()
  
  object election {

    def join(callback: => Unit) = lock(callback)

    def quit() = leave()
  }
}


