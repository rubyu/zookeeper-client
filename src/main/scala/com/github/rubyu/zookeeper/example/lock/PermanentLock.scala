package com.github.rubyu.zookeeper.example.lock

trait PermanentLock extends LockImpl {

  /**
   * Returns true if the client to be the leader, returns false if the given call-by-name
   * has been set on the previous node.
   *
   * Usage:
   *   node.lock {
   *     println("sure to be elected")
   *   }
   *
   * Notice:
   * enter() and leave() will not be called. Please call these functions manually in
   * your code.
   *
   */
  def lock(callback: => Unit): Boolean = {
    do {
      entries.update()
      if (obtained)
        return true
      if (setCallback(callback))
        return false
    } while(true)
    false //suppress the type mismatch error
  }
}
