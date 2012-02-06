package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode

class ReadLock(protected val target: ZooKeeperNode) extends TemporaryLock {
  protected val prefix = "lock-read-"
  protected val entries = new CachedChildren(
    target.children.filter(node =>
      node.name.startsWith("lock-write-") ||
        (mine.isDefined && node == mine.get)
    ).sortBy(_.sequentialId.get)
  )

  protected def enter() {
    mine = Some(create())
  }
}
