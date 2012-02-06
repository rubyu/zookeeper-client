package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode
import java.util.UUID


object SharedLock {
  implicit def zookeepernode2sharedlock(target: ZooKeeperNode) = new SharedLock(target)
}


class SharedLock(target: ZooKeeperNode) {

  object read {
    def lock(callback: => Unit) {
      new ReadLock(target).lock(callback)
    }
  }

  object write {
    def lock(callback: => Unit) {
      new WriteLock(target).lock(callback)
    }
  }
}


class WriteLock(protected val target: ZooKeeperNode) extends NodeAsLock {
  protected val prefix = "lock-write-%s-".format(UUID.randomUUID())

  protected def entries =
    target.children.filter(node =>
      node.name.startsWith("lock-read-") ||
      node.name.startsWith("lock-write-")
    ).sortBy(_.sequentialId.get)
}


class ReadLock(protected val target: ZooKeeperNode) extends NodeAsLock {
  protected val prefix = "lock-read-%s-".format(UUID.randomUUID())

  protected def entries =
    target.children.filter(node =>
      node.name.startsWith("lock-write-")
    ).sortBy(_.sequentialId.get)
}