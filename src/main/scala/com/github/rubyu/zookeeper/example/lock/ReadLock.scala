package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode

class ReadLock(protected val target: ZooKeeperNode) extends TemporaryLock {
  protected val prefix = "lock-read-"
  protected def isEntry(node: ZooKeeperNode) = node.name.startsWith("lock-write-")
}
