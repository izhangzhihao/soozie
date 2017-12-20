package com.github.izhangzhihao.soozie.runner

import org.specs2.specification.BeforeAfterAll

trait BeforeAfterAllStackable extends BeforeAfterAll {
  def beforeAll(): Unit = ()

  def afterAll(): Unit = ()
}
