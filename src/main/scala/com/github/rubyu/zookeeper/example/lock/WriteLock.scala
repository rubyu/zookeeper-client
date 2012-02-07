package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode

class WriteLock(protected val target: ZooKeeperNode) extends TemporaryLock {
  protected val prefix = "lock-write-"
  protected def isEntry(node: ZooKeeperNode) = node.name.startsWith("lock-")
}