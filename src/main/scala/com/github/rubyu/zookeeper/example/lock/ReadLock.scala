package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode
import java.util.UUID

class ReadLock(protected val target: ZooKeeperNode) extends TemporaryLock {
  protected val prefix = "lock-read-%s-".format(UUID.randomUUID())
  protected def isEntry(node: ZooKeeperNode) = {
    node.name.startsWith("lock-write-")
  }
}
