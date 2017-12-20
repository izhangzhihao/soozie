package com.github.izhangzhihao.soozie.utils

import com.github.izhangzhihao.soozie.utils.CronConverter.MonthName._
import com.github.izhangzhihao.soozie.utils.CronConverter.WeekDay._
import com.github.izhangzhihao.soozie.utils.CronConverter._
import org.specs2.mutable._

class CronConverterSpec extends Specification {

  "CronConverter" should {
    "implicitly convert an Int to a Cron sub-expression" in {
      Cron(minutes = 15).toString must_== "* 15 * * * ? "
    }

    "implicitly convert a Set of Int to a Cron sub-expression" in {
      Cron(minutes = Set(15, 30, 45)).toString must_== "* 15,30,45 * * * ? "
    }


    "implicitly convert a Range to a Cron sub-expression" in {
      Cron(hours = 8 to 17).toString must_== "* * 8-17 * * ? "
    }

    "implicitly convert a Set of Ranges to a Cron sub-expression" in {
      Cron(month = Set(4 to 5, 9 to 10)).toString must_== "* * * * 4-5,9-10 ? "
    }

    "implicitly convert a Range by interval to Cron sub-expression" in {
      Cron(seconds = 15 to 30 by 5).toString must_== "15-30/5 * * * * ? "
    }

    "implicitly convert Int with interval to Cron sub-expression" in {
      Cron(year = 2010 by 2).toString must_== "* * * * * ? 2010/2"
    }

    "implicitly convert a Set of Int with interval to Cron sub-expression" in {
      Cron(seconds = Set(5 by 15, 10 by 3)).toString must_== "5/15,10/3 * * * * ? "
    }

    "reject sub-expression exceeding maximum range" in {
      Cron(seconds = 70).toString must throwAn[IllegalArgumentException]
    }

    "reject sub-expression exceeding minimum range" in {
      Cron(month = 0).toString must throwAn[IllegalArgumentException]
    }

    "implicitly convert week day to Cron sub-expression" in {
      Cron(dayOfWeek = Mon).toString must_== "* * * * * 2 "
    }

    "implicitly convert week day range to Cron sub-expression" in {
      Cron(dayOfWeek = Mon to Wed).toString must_== "* * * * * 2-4 "
    }

    "implicitly convert week day with interval to Cron sub-expression" in {
      Cron(dayOfWeek = Mon by 2).toString must_== "* * * * * 2/2 "
    }

    "implicitly convert week set to Cron sub-expression" in {
      Cron(dayOfWeek = Set(Mon, Wed)).toString must_== "* * * * * 2,4 "
    }

    "implicitly convert a set of week ranges to Cron sub-expression" in {
      Cron(dayOfWeek = Set(Mon to Wed, Fri to Sat)).toString must_== "* * * * * 2-4,6-7 "
    }

    "implicitly convert a set of week days with interval to Cron sub-expression" in {
      Cron(dayOfWeek = Set(Mon by 2)).toString must_== "* * * * * 2/2 "
    }

    "implicitly convert a set of week day ranges with interval to Cron sub-expression" in {
      Cron(dayOfWeek = Set(Mon to Fri by 2)).toString must_== "* * * * * 2-6/2 "
    }

    "implicitly convert month to Cron sub-expression" in {
      Cron(month = Feb).toString must_== "* * * * 2 ? "
    }

    "implicitly convert month range to Cron sub-expression" in {
      Cron(month = Apr to Jun).toString must_== "* * * * 4-6 ? "
    }

    "implicitly convert month with interval to Cron sub-expression" in {
      Cron(month = Mar by 3).toString must_== "* * * * 3/3 ? "
    }

    "implicitly convert month set to Cron sub-expression" in {
      Cron(month = Set(Apr, May, Jul)).toString must_== "* * * * 4,5,7 ? "
    }

    "implicitly convert a set of month ranges to Cron sub-expression" in {
      Cron(month = Set(Jan to Mar, Aug to Oct)).toString must_== "* * * * 1-3,8-10 ? "
    }

    "implicitly convert a set of months with interval to Cron sub-expression" in {
      Cron(month = Set(Jun by 6, Feb by 4)).toString must_== "* * * * 6/6,2/4 ? "
    }

    "implicitly convert a set of month ranges with interval to Cron sub-expression" in {
      Cron(month = Set(Feb to Aug by 3)).toString must_== "* * * * 2-8/3 ? "
    }

    "use alternative notation for week days" in {
      Cron(dayOfWeek = (Mon - Fri) by 2).toString must_== "* * * * * 2-6/2 "
    }

    "use alternative notation for months" in {
      Cron(month = (Apr - Oct) by 2).toString must_== "* * * * 4-10/2 ? "
    }
  }
}