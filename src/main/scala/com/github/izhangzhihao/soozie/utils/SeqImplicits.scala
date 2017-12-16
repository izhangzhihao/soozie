package com.github.izhangzhihao.soozie.utils

import scala.collection.generic.CanBuildFrom
import scala.util.Try

object SeqImplicits {
  /**
   * Converts a Seq[Try[T]] to a Try[Seq[T]].
   *
   * This method is to stay more in line with Future.sequence. The implicit sequence on RichTrySeq can be used if this
   * is not a concern for consumers.
   */
  def sequenceTry[A, M[X] <: TraversableOnce[X]](in: M[Try[A]])(implicit cbf: CanBuildFrom[M[Try[A]], A, M[A]]): Try[M[A]] = {
    in.foldLeft(Try(cbf(in))) {
      (tr, ta) => for (r <- tr; a <- ta) yield r += a
    } map (_.result())
  }
}