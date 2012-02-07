package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode
import util.control.Exception._
import org.apache.zookeeper.KeeperException

class CachedChildren(newer: => List[ZooKeeperNode]) {
  protected var cache: List[ZooKeeperNode] = null
  def get = cache
  def update() {
    cache = newer
  }
}

trait LockImpl {
  /**
   * The node that treated as the center of this lock implementation.
   * This should be implemented in the sub-class.
   */
  protected val target: ZooKeeperNode

  /**
   * Prefix of the lock node.
   * This should be implemented in the sub-class.
   */
  protected val prefix: String

  /**
   * Returns true if the given node is regarded as a entry in the lock algorithm.
   * This should be implemented in the sub-class.
   */
  protected def isEntry(node: ZooKeeperNode): Boolean
  
  protected def isMine(node: ZooKeeperNode) = node.name.startsWith(prefix)
  
  protected lazy val entries = new CachedChildren(
    target.children.filter(node =>
      isEntry(node) || isMine(node)
    ).sortBy(_.sequentialId.get)
  )

  protected def mine = entries.get.find(isMine)

  protected def create() = 
    target.createChild(prefix, ephemeral = true, sequential = true)

  protected def delete() = {
    ignoring(classOf[KeeperException.NoNodeException]) {
      mine.get.delete()
    }
  }

  protected def enter() {
    entries.update()
    if (mine.isEmpty) {
      create()
      entries.update()
    }
  }

  protected def leave() {
    entries.update()
    if (mine.isDefined) {
      delete()
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