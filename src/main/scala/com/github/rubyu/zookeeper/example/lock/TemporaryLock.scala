package com.github.rubyu.zookeeper.example.lock

import java.util.concurrent.CountDownLatch
import com.github.rubyu.zookeeper.ZooKeeperNode

/**
 * The implementation of the temporary lock.
 *
 * API
 *     lock
 */
trait TemporaryLock extends LockImpl {

  /**
   * Waits for the lock on the node and do the given task.
   *
   * Given call-by-name will be called after the lock is obtained, and finally
   * the lock will always be released.
   *
   * Usage:
   *   node.lock {
   *     println("lock is obtained")
   *   }
   *
   */
  def lock(f: => Unit) {
    try {
      enter()
      while (!obtained) {
        val latch = new CountDownLatch(1)
        if (setCallback { latch.countDown() })
          latch.await()
        entries.update()
      }
      f
    } finally {
      leave()
    }
  }

  protected def isMine(node: ZooKeeperNode) = {
    mine.isDefined && node == mine.get
  }

  protected var mine: Option[ZooKeeperNode] = null

  protected def enter() {
    mine = Some(create())
    entries.update()
  }

  protected def leave() = delete()
}
