package com.github.rubyu.zookeeper.example.lock

import com.github.rubyu.zookeeper.ZooKeeperNode

class CachedChildren(newer: => List[ZooKeeperNode]) {
  protected var cache = newer
  def get = cache
  def update() {
    cache = newer
  }
}