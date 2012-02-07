package com.github.rubyu.zookeeper.example.lock

/**
 * The implementation of the permanent lock.
 *
 * API
 *     lock
 *     release
 */
trait PermanentLock extends LockImpl {

  /**
   * Tries to get the lock and to set the callback. The success of one of these
   * operations will be guaranteed.
   *
   * Returns true if the client to be the leader, returns false if the given call-by-name
   * has been set on the previous node.
   *
   * Usage:
   *   node.lock {
   *     //this will be executed when the previous node is deleted
   *   }
   *
   *   release()
   *
   * Notice:
   * Once the lock is obtained, this will not be released until release() is called.
   *
   */
  def lock(callback: => Unit): Boolean = {
    enter()
    do {
      if (obtained)
        return true
      if (setCallback(callback))
        return false
      entries.update()
    } while(true)
    false //suppress the type mismatch error
  }

  def release() = leave()
}
