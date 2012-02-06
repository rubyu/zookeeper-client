package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode

object Lock {
  implicit def zookeepernode2lock(target: ZooKeeperNode) = new Lock(target)
}

class Lock(target: ZooKeeperNode) {
  def lock(callback: => Unit) {
    new WriteLock(target).lock(callback)
  }
}
