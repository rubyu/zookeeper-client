package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode
import org.apache.zookeeper.KeeperException
import scala.util.control.Exception._
import java.util.concurrent.CountDownLatch

object Lock {
  implicit def zookeepernode2lock(node: ZooKeeperNode) = new Lock(node)
}

class Lock(node: ZooKeeperNode) {
  private def prefix = "lock-%s-".format(node.client.handle.getSessionId)

  private var state: List[ZooKeeperNode] = null

  private def refreshState() {
    state = node.children.sortBy(_.sequentialId.get)
  }
  refreshState()

  /**
   * Provides 'node.lock {}' that makes available each client do a task
   * exclusively for a node.
   *
   * Given call-by-name will be called after the lock is obtained, and finally
   * the lock will always be released.
   *
   * Usage:
   *     node.lock {
   *       println("lock is obtained")
   *     }
   *
   * Notice:
   * When the lock on a node is already obtained, the requests for the lock
   * with the same client are always allowed; these callbacks will be done
   * asynchronously. Hence this algorithm cannot distinguish anything but
   * the client.
   */
  def lock(f: => Unit) {
    if (!exists) {
      create()
      refreshState()
    }
    while (!obtained) {
      val latch = new CountDownLatch(1)
      if ( setCallback { latch.countDown() } ) latch.await()
      refreshState()
    }
    try {
      f
    } finally {
      delete()
    }
  }

  private def mine = state.find(_.name.startsWith(prefix))

  private def exists = mine.isDefined
  
  private def create() = node.createChild(prefix, ephemeral = true, sequential = true)

  private def obtained = index == 0
  
  private def index = state.indexOf(mine.get)
  
  private def setCallback(callback: => Unit): Boolean = {
    if (index > 0) {
      ignoring(classOf[KeeperException.NoNodeException]) {
        state(index - 1).watch { event => callback }
        return true
      }
    }
    return false
  }
  
  private def delete() {
    ignoring(classOf[KeeperException.NoNodeException]) {
      mine.get.delete()
    }
  }
}