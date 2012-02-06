package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper.ZooKeeperNode
import com.github.rubyu.zookeeper.example.lock.{LockBase, CachedChildren}

object Election {
  implicit def zookeepernode2election(target: ZooKeeperNode) = new Election(target)
}

class Election(protected val target: ZooKeeperNode) extends LockBase {
  protected val prefix = "election-%s-".format(target.client.handle.getSessionId)
  protected val entries = new CachedChildren(
    target.children.sortBy(_.sequentialId.get)
  )

  protected def enter() {
    entries.get.find(_.name.startsWith(prefix)) match {
      case Some(node) =>
        mine = Some(node)
      case None =>
        mine = Some(create())
    }
  }
  
  object election {

    /**
     * Returns true if the client to be the leader, returns false if the given call-by-name
     * has been set on the previous node.
     *
     * Usage:
     *   node.election.join {
     *     println("sure to be elected")
     *   }
     */
    def join(callback: => Unit): Boolean = {
      do {
        enter()
        entries.update()
        if (obtained)
          return true
        if (setCallback(callback))
          return false
      } while(true)
      false //suppress the type mismatch error
    }

    /**
     * Deletes the node of the client.
     *
     * Usage:
     *   node.election.quit()
     */
    def quit() = leave()
  }
}