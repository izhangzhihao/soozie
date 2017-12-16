package com.github.izhangzhihao.soozie.runner

import com.github.sakserv.minicluster.impl.HdfsLocalCluster
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

trait HdfsProvider {
  val hdfsUri: String
  val fs: FileSystem
}

trait TestHdfsProvider extends HdfsProvider with BeforeAfterAllStackable {
  lazy val hdfsLocalCluster = new HdfsLocalCluster.Builder()
    .setHdfsNamenodePort(12345)
    .setHdfsNamenodeHttpPort(12222)
    .setHdfsTempDir("embedded_hdfs")
    .setHdfsNumDatanodes(1)
    .setHdfsEnablePermissions(false)
    .setHdfsFormat(true)
    .setHdfsEnableRunningUserAsProxyUser(true)
    .setHdfsConfig(new Configuration())
    .build()

  lazy val hdfsUri = s"hdfs://localhost:${hdfsLocalCluster.getHdfsNamenodePort}"
  lazy val fs: FileSystem = hdfsLocalCluster.getHdfsFileSystemHandle

  override def beforeAll(): Unit = {
    super.beforeAll()
    hdfsLocalCluster.getHdfsConfig.set("fs.default.dir", hdfsUri)
    hdfsLocalCluster.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    hdfsLocalCluster.stop(true)
  }
}
