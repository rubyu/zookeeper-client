package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode

class WriteLock(protected val target: ZooKeeperNode) extends Lockable {
  protected val prefix = "lock-write-"
  protected val entries = new CachedChildren(
    target.children.filter(node =>
      node.name.startsWith("lock-read-") ||
        node.name.startsWith("lock-write-")
    ).sortBy(_.sequentialId.get)
  )
}