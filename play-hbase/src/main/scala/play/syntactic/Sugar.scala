package play.syntactic

/**
 * @author zepeng.li@gmail.com
 */
object Sugar {

  def iff[A](con: Boolean)(f: => A): Boolean = { if (con) f; con }

  implicit class SugarOption[A](val op: Option[A]) extends AnyVal {

    def whenEmpty(bk: => Unit) = { if (op.isEmpty) bk; op }

    def whenDefined(bk: A => Unit) = { if (op.isDefined) bk(op.get); op }

  }

  implicit class SugarBoolean(val b: Boolean) extends AnyVal {

    def option[A](f: => A): Option[A] = if (b) Some(f) else None

    def flatOption[A](f: => Option[A]): Option[A] = if (b) f else None

    def otherwise[A](f: => Option[A]): Option[A] = if (!b) f else None
  }

  implicit class SugarArray[T](val arr: Array[T]) extends AnyVal {

    def getOrElse(idx: Int, t: T) = if (arr.isDefinedAt(idx)) arr(idx) else t

  }

  /*
   * Ruby Style Grammar
   */
  implicit class IntToTimes(n: Int) {

    def times[A](block: => A) = for (i <- 1 to n) yield block

  }

  implicit class BlockToUnless[T](left: => T) {

    def iff(right: => Boolean): Option[T] = if (right) Some(left) else None

    def unless(right: => Boolean): Option[T] = if (!right) Some(left) else None

  }

}