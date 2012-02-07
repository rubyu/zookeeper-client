package com.github.rubyu.zookeeper.example.lock

import java.util.concurrent.CountDownLatch

/**
 * An implementation of the temporary lock.
 *
 * Usage:
 *   val temporaryLock = ...
 *   temporaryLock.lock {
 *     println("lock is obtained")
 *   }
 *
 */
trait TemporaryLock extends LockImpl {

  /**
   * Waits for the lock on the node and do the given task.
   *
   * Given call-by-name will be done after the lock is obtained, and finally
   * the lock will always be released.
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
}
