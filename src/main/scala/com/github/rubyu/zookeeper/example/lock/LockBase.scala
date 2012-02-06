package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode
import util.control.Exception._
import org.apache.zookeeper.KeeperException

trait LockBase {
  protected val target: ZooKeeperNode
  protected val prefix: String
  protected val entries: CachedChildren

  protected var mine: Option[ZooKeeperNode] = None

  protected def create() = 
    target.createChild(prefix, ephemeral = true, sequential = true)

  protected def enter(): Unit

  protected def leave() {
    if (mine.isDefined) {
      ignoring(classOf[KeeperException.NoNodeException]) {
        mine.get.delete()
      }
    }
  }

  protected def prev = {
    if (index > 0)
      Some(entries.get(index - 1))
    else
      None
  }

  protected def index = entries.get.indexOf(mine.get)

  protected def obtained = index == 0

  protected def setCallback(callback: => Unit): Boolean = {
    prev match {
      case Some(node) =>
        ignoring(classOf[KeeperException.NoNodeException]) {
          node.watch { event => callback }
          return true
        }
      case None =>
        throw new IllegalStateException("node does not exist")
    }
    return false
  }
}