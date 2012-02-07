package com.github.rubyu.zookeeper.example.lock

import java.util.concurrent.CountDownLatch

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

  /**
   * This function has the same effect as super.leave() except that the entry cache will
   * not be refreshed.
   */
  override protected def leave() {
    if (mine.isDefined) {
      delete()
    }
  }
}
