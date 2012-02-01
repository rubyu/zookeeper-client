package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper._
import org.specs2.mutable.Specification
import org.specs2.specification._
import org.apache.log4j.Logger
import java.util.concurrent.TimeUnit

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

  "ZooKeeperNode" should {
    "not be a lock" in {
      (new Lock(user1)).lock.release() must_== ()
    }
    "be a Lock when the IC function had been imported" in {
      import Lock._
      user1.lock.release() must_== ()
    }
  }
        
  "Lock" should {
    import Lock._
    
    "be the master" in {
      user1.lock.get() must_== true
    }

    "call callback when the previous lock node has been removed" in {
      import java.util.concurrent.CountDownLatch
      val latch = new CountDownLatch(1)
      user1.lock.get() must_== true
      user2.lock.get { latch.countDown() } must_== false
      user1.lock.release()
      latch.await(100, TimeUnit.MILLISECONDS)
      latch.getCount must_== 0
      user2.lock.get() must_== true
    }
  }
}
