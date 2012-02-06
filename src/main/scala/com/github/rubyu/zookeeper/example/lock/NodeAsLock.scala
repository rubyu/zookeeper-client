package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode
import org.apache.zookeeper.KeeperException
import scala.util.control.Exception._
import java.util.concurrent.CountDownLatch


trait NodeAsLock {
  protected val target: ZooKeeperNode
  protected val prefix: String
  protected var mine: ZooKeeperNode = null
  protected var entriesCache: List[ZooKeeperNode] = null

  protected def enter() {
    val result = target.createChild(prefix, ephemeral = true, sequential = true)
    mine = target.client.node(result)
  }

  protected def leave() {
    ignoring(classOf[KeeperException.NoNodeException]) {
      mine.delete()
    }
  }

  protected def entries: List[ZooKeeperNode]

  protected def updateEntries() {
    entriesCache = entries
  }

  protected def prev = {
    entriesCache.indexOf(mine) match {
      case n if n > 0 => Some(entriesCache(n - 1))
      case _ => None
    }
  }

  protected def obtained = prev.isEmpty

  protected def setCallback(callback: => Unit): Boolean = {
    prev match {
      case Some(node) =>
        ignoring(classOf[KeeperException.NoNodeException]) {
          node.watch { event => callback }
          return true
        }
      case _ =>
    }
    return false
  }

  /**
   * Given call-by-name will be called after the lock is obtained, and finally
   * the lock will always be released.
   *
   * Usage:
   *     node.lock {
   *       println("lock is obtained")
   *     }
   *
   */
  def lock(f: => Unit) {
    try {
      enter()
      updateEntries()
      while (!obtained) {
        val latch = new CountDownLatch(1)
        if (setCallback { latch.countDown() }) latch.await()
        updateEntries()
      }
      f
    } finally {
      leave()
    }
  }
}
