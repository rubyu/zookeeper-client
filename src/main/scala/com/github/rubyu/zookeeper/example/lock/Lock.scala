package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode
import org.apache.log4j.Logger
import org.apache.zookeeper.KeeperException
import scala.util.control.Exception._
import java.util.concurrent.CountDownLatch

object Lock {
  implicit def zookeepernode2lock(node: ZooKeeperNode) = new Lock(node)
}

class Lock(node: ZooKeeperNode) {
  
  private val log = Logger.getLogger("%s sid=%s".format(
    this.getClass.getName, node.client.handle.getSessionId))
  
  private def prefix = "lock-%s-".format(node.client.handle.getSessionId)

  private def lockNodes = node.children.sortBy(_.sequentialId.get)

  /**
   * Provides 'node.lock {}' that make available each client do a task
   * exclusively for a node.
   *
   * Usage:
   *     node.lock {
   *       //do something
   *     }
   */
  def lock(f: => Unit) {
    var latch = new CountDownLatch(1)
    while (
      !get(callback = { latch.countDown() })
    ) {
      latch.await()
      latch = new CountDownLatch(1)
    }
    f
    release()
  }

  /**
   * Returns true if the lock has been obtained.
   * When returns false, creation of a watcher on the previous node is guaranteed.
   */
  private def get(callback: => Unit): Boolean = {
    do {
      val current = lockNodes

      log.debug("lock nodes:\n%s".format(current.mkString("\n")))

      current.find(_.name.startsWith(prefix)) match {
        case None =>
          log.debug("lock node does not exists; creating ...")
          node.createChild(prefix, ephemeral = true, sequential = true)
        case Some(mine) =>
          log.debug("lock node exists")
          current.indexOf(mine) match {
            case x if x == 0 =>
              log.debug("lock has been obtained")
              return true
            case x if x >= 1 =>
              log.debug("lock has not been obtained; setting callback ...")
              ignoring(classOf[KeeperException.NoNodeException]) {
                current(x - 1).watch { event => callback }
                return false
              }
          }
      }
    } while(true)
    throw new RuntimeException("should not reach here")
  }

  /**
   * Deletes the lock node if it exists.
   */
  private def release() {
    lockNodes.find(_.name.startsWith(prefix)) match {
      case Some(mine) =>
        log.debug("lock node has been deleted")
        ignoring(classOf[KeeperException.NoNodeException]) {
          mine.delete()
        }
      case None =>
        log.debug("lock node does not exist")
    }
  }
}