package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode

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
