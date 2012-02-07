package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper.ZooKeeperNode

object Election {
  implicit def zookeepernode2election(target: ZooKeeperNode) = new Election(target)
}

/**
 * The example of the leader election.
 *
 * Usage:
 *   val election = node.election()
 *   election.join {
 *
 *   }
 *   election.quit()
 */
class Election(target: ZooKeeperNode) {
  def election() = new ElectionWrapper(target)
}

class ElectionWrapper(target: ZooKeeperNode) {
  private val election = new LeaderElection(target)

  def join(f: => Unit) = election.lock(f)

  def quit() = election.release()
}