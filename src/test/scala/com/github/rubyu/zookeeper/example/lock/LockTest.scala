package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper._
import org.specs2.mutable.Specification
import org.specs2.specification._
import org.apache.log4j.Logger
import java.util.concurrent.atomic.AtomicInteger
import collection.JavaConversions._
import java.util.concurrent.{CountDownLatch, CopyOnWriteArrayList}

class LockTest extends Specification with BeforeAfterExample {
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
        
  "Lock" should {
    import Lock._
    import scala.concurrent.ops._

    "call given call-by-name exclusively" in {
      class Count {
        val log = new CopyOnWriteArrayList[Int]()
        val value = new AtomicInteger()
        private def loggedAdd(n: Int) = log.add(value.addAndGet(n))
        def incr() = loggedAdd(1)
        def decr() = loggedAdd(-1)
      }
      val count = new Count()

      class Launcher(val nodes: ZooKeeperNode*) {
        val latch = new CountDownLatch(nodes.length)
        def launch() = {
          nodes foreach { node =>
            spawn {
              node.lock {
                count.incr()
                log.debug("do something")
                Thread.sleep(100)
                count.decr()
                latch.countDown()
              }
            }
          }
        }
      }
      val launcher = new Launcher(user1, user2)
      launcher.launch()
      launcher.latch.await()

      log.debug("log => %s".format(count.log.mkString(", ")))
      count.log.forall( _ <= 1 ) must_== true
    }
  }
}
