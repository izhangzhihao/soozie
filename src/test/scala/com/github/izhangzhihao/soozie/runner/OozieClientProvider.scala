package com.github.izhangzhihao.soozie.runner

import com.github.sakserv.minicluster.impl.OozieLocalServer
import org.apache.hadoop.conf.Configuration
import org.apache.oozie.client.OozieClient
import org.apache.oozie.local.LocalOozie

trait OozieClientProvider {
  val oozieClient: OozieClient

  def oozieCoordClient: OozieClient
}

trait TestOozieClientProvider extends OozieClientProvider with BeforeAfterAllStackable {
  this: HdfsProvider =>

  lazy val oozieLocalServer = new OozieLocalServer.Builder()
    .setOozieTestDir("embedded_oozie")
    .setOozieHomeDir("oozie_home")
    .setOozieUsername(System.getProperty("user.name"))
    .setOozieGroupname("testgroup")
    .setOozieYarnResourceManagerAddress("localhost")
    .setOozieHdfsDefaultFs(hdfsUri)
    .setOozieConf(new Configuration())
    .setOozieHdfsShareLibDir("/tmp/oozie_share_lib")
    .setOozieShareLibCreate(true)
    .setOozieLocalShareLibCacheDir("share_lib_cache")
    .setOoziePurgeLocalShareLibCache(false)
    .build()

  lazy val oozieClient: OozieClient = oozieLocalServer.getOozieClient

  def oozieCoordClient: OozieClient = {
    LocalOozie.getCoordClient
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    oozieLocalServer.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    oozieLocalServer.stop()
  }
}
