package play.datetime

import scala.language.implicitConversions
import org.joda.time._
import org.joda.time.format._

/**
 * @author zepeng.li@gmail.com
 */
object Calc {

  def last(period: Period)(implicit yesterday: LocalDate) = period.till(yesterday)

  implicit class Int2DateTime(val i: Int) extends AnyVal {
    def day = days

    def days = Period.days(i)

    def month = months

    def months = Period.months(i)

    def week = weeks

    def weeks = Period.weeks(i)

    def year = years

    def years = Period.years(i)
  }

  implicit class String2DateTime(val str: String) extends AnyVal {

    def toDate = toDateTime.map(_.toDate)

    def toDateTime: Option[DateTime] =
      try {
        Some(DateTime.parse(str))
      } catch {
        case e: Exception => None
      }

    def toDateMidnight = toDateTime.map(_.toDateMidnight)

    def toLocalDate = toDateTime.map(_.toLocalDate)
  }

  implicit class TimeStamp2DateTime(val ms: Long) extends AnyVal {
    def toDate = new java.util.Date(ms)

    def toDateTime = new DateTime(ms)

    def toDateMidnight = new DateMidnight(ms)

    def toLocalDate = new LocalDate(ms)

    def toYearMonth = new YearMonth(ms)

    def reversedDate = new DateTime(Long.MaxValue - ms)
  }

  implicit class JDKDate2DateTime(val date: java.util.Date) extends AnyVal {
    def toDateTime = new DateTime(date)

    def toDateMidnight = new DateMidnight(date)

    def toLocalDate = new LocalDate(date)
  }

  implicit class JodaDateTimeCalc(val dt: DateTime) extends AnyVal {
    def ~(that: DateTime): Interval = createInterval(dt, that)

    def reversedMillis = Long.MaxValue - dt.getMillis

    def format(fmt: String = "yyyy-MM-dd"): String = DateTimeFormat.forPattern(fmt).print(dt)

    def format(fmt: DateTimeFormatter): String = fmt.print(dt)
  }

  implicit class JodaDateMidnightCalc(val dtm: DateMidnight) extends AnyVal {
    def reversedMillis = Long.MaxValue - dtm.getMillis
  }

  implicit class JodaLocalDateCalc(val dt: LocalDate) extends AnyVal {

    def to(that: LocalDate): Seq[LocalDate] = genRange(dt, that)

    def until(that: LocalDate): Seq[LocalDate] = genRange(dt, that.minusDays(1))

    def ===(that: LocalDate): Boolean = Days.daysBetween(dt, that).getDays == 0

    def -(p: Period): LocalDate = dt.minus(p)

    def format(fmt: String = "yyyy-MM-dd"): String = DateTimeFormat.forPattern(fmt).print(dt)

    def format(fmt: DateTimeFormatter): String = fmt.print(dt)
  }

  implicit class JodaYearMonth2DateTime(val ym: YearMonth) extends AnyVal {

    def toDateTime = new DateTime(ym)

    def format(fmt: String = "yyyy-MM"): String = DateTimeFormat.forPattern(fmt).print(ym)

    def format(fmt: DateTimeFormatter): String = fmt.print(ym)
  }

  implicit class JodaIntervalCalc(val interval: Interval) extends AnyVal {
    def eachDay = genRange(interval.getStart.toLocalDate, interval.getEnd.toLocalDate)

    def eachMonth = genRange(getStartMonth, getEndMonth)

    def getStartMonth = new YearMonth(interval.getStart)

    def getEndMonth = new YearMonth(interval.getEnd)

    def plusEndMonths(i: Int) = createInterval(interval.getStart, interval.getEnd.plusMonths(i))

    def minusEndMonths(i: Int) = createInterval(interval.getStart, interval.getEnd.minusMonths(i))

  }

  implicit class JodaPeriodCalc(val p: Period) extends AnyVal {
    def since(s: LocalDate) = new Interval(s.toDateMidnight, p)

    def till(t: LocalDate) = new Interval(p.minusDays(1), t.minusDays(1).toDateMidnight)

    def since(s: YearMonth) = new Interval(s.toLocalDate(1).toDateMidnight, p)

    def till(t: YearMonth) = new Interval(p.minusMonths(1), t.minusMonths(1).toLocalDate(31).toDateMidnight) //TODO test 28
  }

  private def genRange(start: YearMonth, end: YearMonth): Seq[YearMonth] = {
    val months = Months.monthsBetween(start, end).getMonths
    for (i <- 0 to months) yield start.plusMonths(i)
  }

  private def genRange(start: LocalDate, end: LocalDate): Seq[LocalDate] = {
    val days = Days.daysBetween(start, end).getDays //may be minus
    for (i <- 0 to days) yield start.plusDays(i)
  }

  private def createInterval(start: ReadableInstant, end: ReadableInstant): Interval = {
    if (!start.isAfter(end))
      new Interval(start, end)
    else {
      new Interval(start, start)
    }
  }
}