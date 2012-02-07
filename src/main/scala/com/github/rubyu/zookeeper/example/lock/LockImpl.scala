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

/**
 * An implementation of the lock on ZooKeeper.
 */
trait LockImpl {
  protected val entries = new CachedChildren(
    target.children.filter(node =>
      isEntry(node) || isMine(node)
    ).sortBy(_.sequentialId.get)
  )
  
  /**
   * The node that treated as the parent of all lock nodes.
   */
  protected val target: ZooKeeperNode

  /**
   * Prefix of the lock node.
   */
  protected val prefix: String

  /**
   * Returns true if the given node is regarded as a entry in the lock algorithm.
   */
  protected def isEntry(node: ZooKeeperNode): Boolean

  protected def isMine(node: ZooKeeperNode) = {
    mine.isDefined && node == mine.get
  }

  protected var mine: Option[ZooKeeperNode] = None

  protected def create() = 
    target.createChild(prefix, ephemeral = true, sequential = true)

  protected def delete() {
    ignoring(classOf[KeeperException.NoNodeException]) {
      mine.get.delete()
    }
  }

  protected def enter() {
    if (mine.isDefined)
      throw new IllegalStateException("node already exists")
    mine = Some(create())
    entries.update()
  }

  protected def leave() {
    if (mine.isEmpty)
      throw new IllegalStateException("node does not exist")
    delete()
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
    if (!obtained) {
      prev match {
        case Some(node) =>
          ignoring(classOf[KeeperException.NoNodeException]) {
            node.watch { event => callback }
            return true
          }
        case None =>
          throw new IllegalStateException("node does not exist")
      }
    }
    return false
  }
}