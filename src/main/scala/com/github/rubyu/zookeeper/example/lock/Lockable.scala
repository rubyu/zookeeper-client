package com.github.rubyu.zookeeper.example.lock

import java.util.concurrent.CountDownLatch

trait Lockable extends LockBase {

  protected def enter() {
    mine = Some(create())
  }

  /**
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
      entries.update()
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
