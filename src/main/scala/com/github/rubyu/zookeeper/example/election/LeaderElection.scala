package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper.ZooKeeperNode
import com.github.rubyu.zookeeper.example.lock.PermanentLock

class LeaderElection(protected val target: ZooKeeperNode) extends PermanentLock {
  protected val prefix = "election-"
  protected def isEntry(node: ZooKeeperNode) = node.name.startsWith(prefix)
}
