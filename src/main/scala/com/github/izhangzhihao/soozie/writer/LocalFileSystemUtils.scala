package com.github.izhangzhihao.soozie.writer

import java.io.{ File, PrintWriter }

import scala.util.Try

trait LocalFileSystemUtils extends FileSystemUtils {
  override def writeTextFile(path: String, text: String): Try[Unit] = {
    import com.github.izhangzhihao.soozie.utils.TryImplicits._

    val pw = new PrintWriter(new File(path))
    Try(pw.write(text)).doFinally(pw.close())
  }

  override def makeDirectory(path: String): Try[Unit] = {
    Try(new File(path).mkdir())
  }
}

object LocalFileSystemUtils extends LocalFileSystemUtils
