package com.github.rubyu.zookeeper.example.election

import com.github.rubyu.zookeeper._
import org.specs2.mutable.Specification
import org.specs2.specification._
import org.apache.log4j.Logger
import java.util.concurrent.{TimeUnit, CountDownLatch}

class ElectionTest extends Specification with BeforeAfterExample {
  private val log = Logger.getLogger(this.getClass.getName)

  def testNode(client: ZooKeeperClient) = client.node("zookeeper-client-test", "electiontest")
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

  "Election" should {
    import Election._

    "return true if lock is obtained" in {
      val a = user1.election()

      a.join() must_== true
    }

    "return false if lock has been obtained by other client" in {
      val a = user1.election()
      val b = user2.election()
      a.join() must_== true
      b.join() must_== false
    }

    "not call given call-by-name when to be the leader immediately" in {
      val latch = new CountDownLatch(1)
      val a = user1.election()
      a.join { latch.countDown() }
      latch.await(100, TimeUnit.MILLISECONDS)
      latch.getCount must_== 1
    }

    "call given call-by-name when the leader resigned" in {
      val latch = new CountDownLatch(1)
      val a = user1.election()
      val b = user2.election()
      
      a.join {}
      b.join { latch.countDown() }
      a.quit()
      latch.await(100, TimeUnit.MILLISECONDS)
      latch.getCount must_== 0
    }

    "throw an error when quit before join" in {
      val a = user1.election()
      a.quit() must throwA[IllegalStateException]
      a.join()
      a.quit() must not (throwA[Throwable])
    }
  }
}
