package com.github.izhangzhihao.soozie.utils

import scala.util.Try

object TryImplicits {

  implicit class PimpedTry[T](val underlying: Try[T]) extends AnyVal {

    /**
      * Equivalent to a finally block using the standard syntax
      *
      * Type `Ignored` to avoid implicit conversions to unit when not necessary.
      */
    def doFinally[Ignored](f: => Ignored): Try[T] = {
      val ignore = (_: Any) => {
        f; underlying
      }
      underlying.transform(ignore, ignore)
    }
  }

}