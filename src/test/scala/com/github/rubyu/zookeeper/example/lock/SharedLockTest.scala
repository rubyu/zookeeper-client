package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper._
import org.specs2.mutable.Specification

class SharedLockTest extends Specification {
  "SharedLock" should {
    import SharedLock._

    "be a SharedLock implicitly" in {
      val node = new ZooKeeperNode(null, null)
      val a = node.write.lock(_)
      val b = node.read.lock(_)
      a.isInstanceOf[Function1[_, _]] must_== true
      b.isInstanceOf[Function1[_, _]] must_== true
    }
  }
}
