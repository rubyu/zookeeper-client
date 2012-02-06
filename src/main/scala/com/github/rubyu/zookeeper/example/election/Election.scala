package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper.ZooKeeperNode
import org.apache.zookeeper.KeeperException
import scala.util.control.Exception._

object Election {
  implicit def zookeepernode2election(target: ZooKeeperNode) = new Election(target)
}

class Election(target: ZooKeeperNode) {
  private var entries: List[ZooKeeperNode] = null
  
  private def prefix = "election-%s-".format(target.client.handle.getSessionId)

  private def updateEntries() {
    entries = target.children.sortBy(_.sequentialId.get)
  }

  private def mine = entries.find(_.name.startsWith(prefix))

  private def joined = mine.isDefined
  
  private def create() {
    target.createChild(prefix, ephemeral = true, sequential = true)
  }

  /**
   * Returns true if the client to be the leader, returns false if the given call-by-name
   * has been set on the previous node.
   */
  def join(callback: => Unit): Boolean = {
    do {
      updateEntries()
      if (!joined) {
        create()
      } else {
        if (isLeader) return true
        if (setCallback(callback)) return false
      }
    } while(true)
    false //suppress the type mismatch error
  }

  private def isLeader = order == 0

  private def order = entries.indexOf(mine.get)
  
  private def prev = {
    order match {
      case n if n == 0 => None
      case n if n >= 1 => Some(entries(n - 1))
    }
  }

  private def setCallback(callback: => Unit): Boolean = {
    prev match {
      case Some(node) =>
        ignoring(classOf[KeeperException.NoNodeException]) {
          node.watch { event => callback }
          return true
        }
      case None =>
    }
    return false
  }

  /**
   * Deletes the node of the client.
   */
  def quit() {
    updateEntries()
    if (joined) {
      ignoring(classOf[KeeperException.NoNodeException]) {
        mine.get.delete()
      }  
    }
  }
}