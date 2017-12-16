package com.github.izhangzhihao.soozie.utils

import scala.collection.immutable.Range.Inclusive

/* Taken from https://github.com/vdichev/scuartz/blob/master/src/main/scala/org/scala_libs/scuartz/Scuartz.scala
 */

object CronConverter {

  //  class Range(start: Int, end: Int, step: Int) {
  //    protected def copy(start: Int, end: Int, step: Int): Range = new Range(start, end, step)
  //
  //    /** Create a new range with the `start` and `end` values of this range and
  //      * a new `step`.
  //      *
  //      * @return a new range with a different step
  //      */
  //    def by(step: Int): Range = copy(start, end, step)
  //  }

  trait TimeUnit {
    val minValue: Int
    val maxValue: Int

    def toStringEmpty = "*"
  }

  trait Second extends TimeUnit {
    val minValue = 0
    val maxValue = 59
  }

  trait Minute extends TimeUnit {
    val minValue = 0
    val maxValue = 59
  }

  implicit object Second extends Second

  trait Hour extends TimeUnit {
    val minValue = 0
    val maxValue = 23
  }

  implicit object Minute extends Minute

  trait DayOfMonth extends TimeUnit {
    val minValue = 1
    val maxValue = 31
  }

  implicit object Hour extends Hour

  trait Month extends TimeUnit {
    val minValue = 1
    val maxValue = 12
  }

  implicit object DayOfMonth extends DayOfMonth

  trait DayOfWeek extends TimeUnit {
    val minValue = 1
    val maxValue = 7

    // quartz doesn't support both day of week and day of month with * wildcard
    override def toStringEmpty = "?"
  }

  implicit object Month extends Month

  trait Year extends TimeUnit {
    // according to quartz documentation, years can range from 1970 to 2099
    val minValue = 1970
    val maxValue = 2099

    override def toStringEmpty = ""
  }

  implicit object DayOfWeek extends DayOfWeek

  class CronSubExpr[T <: TimeUnit](val rangeSet: Set[Range])(implicit val timeUnit: T) {

    override def toString =
      if (rangeSet isEmpty)
        timeUnit.toStringEmpty
      else
        rangeSet map { r =>
          if (r.end > timeUnit.maxValue)
            throw new IllegalArgumentException("Maximum value for time unit " + timeUnit.getClass +
              " exceeded: " + r.end)
          if (r.start < timeUnit.minValue)
            throw new IllegalArgumentException("Minimum value for time unit " + timeUnit.getClass +
              " exceeded: " + r.start)
          val step = if (r.step == 1) "" else "/" + r.step
          // technically omitting the end means the max value for Quartz
          val end = if (r.start == r.end) "" else "-" + r.end
          r.start + end + step
        } mkString ","
  }

  implicit object Year extends Year

  //  class WeekRange(start: Int, end: Int, step: Int) extends Range.Inclusive(start, end, step) {
  //    def /(step: Int) = by(step)
  //  }

  case class Cron(seconds: CronSubExpr[Second] = Set[Int](),
                  minutes: CronSubExpr[Minute] = Set[Int](),
                  hours: CronSubExpr[Hour] = Set[Int](),
                  dayOfMonth: CronSubExpr[DayOfMonth] = Set[Int](),
                  month: CronSubExpr[Month] = Set[Int](),
                  dayOfWeek: CronSubExpr[DayOfWeek] = Set[Int](),
                  year: CronSubExpr[Year] = Set[Int]()) {

    override def toString = productIterator mkString " "
  }

  //  class MonthRange(start: Int, end: Int, step: Int) extends Range.Inclusive(start, end, step) {
  //    def /(step: Int) = by(step)
  //  }

  object WeekDay extends Enumeration(1) {

    val Sun, Mon, Tue, Wed, Thu, Fri, Sat = WeekVal

    // custom builder method- can't use Value
    private def WeekVal = new WeekVal

    // add custom methods to the enum type to return a week-specific range
    class WeekVal extends Val(nextId) {
      def -(end: WeekVal) = to(end)

      def to(end: WeekVal) = new Inclusive(id, end.id, 1)

      def /(step: Int) = by(step)

      def by(step: Int) = new Inclusive(id, id, step)
    }
  }

  object MonthName extends Enumeration(1) {

    val Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec = MonthVal

    // custom builder method- can't use Value
    private def MonthVal = new MonthVal

    // add custom methods to the enum type to return a month-specific range
    class MonthVal extends Val(nextId) {
      def -(end: MonthVal) = to(end)

      def to(end: MonthVal) = new Inclusive(id, end.id, 1)

      def /(step: Int) = by(step)

      def by(step: Int) = new Inclusive(id, id, step)
    }
  }

  implicit def intToCronSubExpr[T <: TimeUnit](i: Int)(implicit timeUnit: T): CronSubExpr[T] =
    new CronSubExpr[T](Set(i to i))(timeUnit)

  implicit def intToRange[T <% Int](i: T): Range = Range.inclusive(i, i)

  implicit def intSetToCronSubExpr[T <: TimeUnit](s: Set[Int])(implicit timeUnit: T): CronSubExpr[T] =
    new CronSubExpr[T](s map (i => i to i))(timeUnit)

  implicit def rangeToCronSubExpr[T <: TimeUnit](r: Range)(implicit timeUnit: T): CronSubExpr[T] =
    new CronSubExpr[T](Set(r))(timeUnit)

  implicit def rangeSetToCronSubExpr[T <: TimeUnit, R <: Range](s: Set[R])(implicit timeUnit: T): CronSubExpr[T] =
  // since this is an immutable set we can coerce subclasses of Range
  // like Range.Inclusive as the set type parameter
    new CronSubExpr[T](s.asInstanceOf[Set[Range]])(timeUnit)

  implicit def weekDayToCronSubExpr(wd: WeekDay.WeekVal): CronSubExpr[DayOfWeek] =
    new CronSubExpr[DayOfWeek](Set(wd.id to wd.id))(DayOfWeek)

  implicit def weekDaySetToCronSubExpr(s: Set[WeekDay.WeekVal]): CronSubExpr[DayOfWeek] =
    new CronSubExpr[DayOfWeek](s map (wd => wd.id to wd.id))(DayOfWeek)

  implicit def monthToCronSubExpr(m: MonthName.MonthVal): CronSubExpr[Month] =
    new CronSubExpr[Month](Set(m.id to m.id))(Month)

  implicit def monthSetToCronSubExpr(s: Set[MonthName.MonthVal]): CronSubExpr[Month] =
    new CronSubExpr[Month](s map (m => m.id to m.id))(Month)
}