package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper._
import org.specs2.mutable.Specification
import org.apache.log4j.Logger

class LockTest extends Specification {
  "ZooKeeperNode" should {
    import Lock._
    "be a Lock implicitly" in {
      val node = new ZooKeeperNode(null, null)
      val f = node.lock(_)
      f.isInstanceOf[Function1[_, _]] must_== true
    }
  }
}
