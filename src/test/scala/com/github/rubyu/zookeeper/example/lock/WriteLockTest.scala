package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper._
import org.specs2.mutable.Specification
import org.specs2.specification._
import org.apache.log4j.Logger
import java.util.concurrent.atomic.AtomicInteger
import collection.JavaConversions._
import java.util.concurrent.{CountDownLatch, CopyOnWriteArrayList}
import SharedLock._
import concurrent.ops._

class Count {
  val log = new CopyOnWriteArrayList[Int]()
  val value = new AtomicInteger()
  private def loggedAdd(n: Int) = log.add(value.addAndGet(n))
  def incr() = loggedAdd(1)
  def decr() = loggedAdd(-1)
}

class Launcher(n : Int) {
  private val log = Logger.getLogger(this.getClass.getName)
  val readCount = new Count()
  val writeCount = new Count()
  def launch(nodes: ZooKeeperNode*) = {
    val latch = new CountDownLatch(nodes.length * n * 2)
    nodes foreach { node =>
      for (i <- 1 to n) {
        spawn {
          node.read.lock {
            readCount.incr()
            log.debug("Doing a task %s of %s in the read lock".format(i, n))
            Thread.sleep(100)
            readCount.decr()
            latch.countDown()
          }
        }
        spawn {
          node.write.lock {
            writeCount.incr()
            log.debug("Doing a task %s of %s in the write lock".format(i, n))
            Thread.sleep(100)
            writeCount.decr()
            latch.countDown()
          }
        }
      }
    }
    latch
  }
}


class WriteLockTest extends Specification with BeforeAfterExample {
  private val log = Logger.getLogger(this.getClass.getName)

  def testNode(client: ZooKeeperClient) = client.node("zookeeper-client-test", "locktest")
  val user1 = testNode(new ZooKeeperClient("192.168.0.100"))
  val user2 = testNode(new ZooKeeperClient("192.168.0.100"))

  def before {
    log.info("--test--")
    if (user1.exists)
      user1.deleteRecursive()
    user1.createRecursive()
  }

  def after {
    log.info("--------")
  }

  "WriteLock" should {

    "call given call-by-name exclusively" in {
      val launcher = new Launcher(100)
      val latch = launcher.launch(user1, user2)
      latch.await()

      log.debug("readCount log => %s".format(launcher.readCount.log.mkString(", ")))
      log.debug("writeCount log => %s".format(launcher.writeCount.log.mkString(", ")))
      launcher.writeCount.log.forall( _ <= 1 ) must_== true
    }
  }
}

 