package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper.ZooKeeperNode

object Election {
  implicit def zookeepernode2election(target: ZooKeeperNode) = new Election(target)
}

/**
 * The leader election example.
 *
 * Usage:
 *   node.election.join {
 *     println("sure to be elected")
 *   }
 *
 *   node.election.quit()
 */
class Election(target: ZooKeeperNode) {

  object election {

    def join(callback: => Unit) =
      new LeaderElection(target).lock(callback)

    def quit() =
      new LeaderElection(target).release()
  }
}

