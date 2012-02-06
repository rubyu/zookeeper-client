package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper.ZooKeeperNode
import org.apache.zookeeper.KeeperException
import scala.util.control.Exception._

object Election {
  implicit def zookeepernode2election(node: ZooKeeperNode) = new Election(node)
}

class Election(node: ZooKeeperNode) {
  private def prefix = "election-%s-".format(node.client.handle.getSessionId)

  private var entries: List[ZooKeeperNode] = null
  private def updateEntries() {
    entries = node.children.sortBy(_.sequentialId.get)
  }

  private def mine = entries.find(_.name.startsWith(prefix))

  private def joined = mine.isDefined
  
  private def create() = node.createChild(prefix, ephemeral = true, sequential = true)

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

  private def setCallback(callback: => Unit): Boolean = {
    if (order > 0) {
      ignoring(classOf[KeeperException.NoNodeException]) {
        entries(order - 1).watch { event => callback }
        return true
      }
    }
    return false
  }

  /**
   * Deletes the node of the client.
   */
  def quit() {
    updateEntries()
    ignoring(classOf[KeeperException.NoNodeException]) {
      mine.get.delete()
    }
  }
}