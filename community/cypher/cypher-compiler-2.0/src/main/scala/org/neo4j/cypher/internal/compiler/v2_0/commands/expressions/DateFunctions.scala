/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_0._
import pipes.QueryState
import symbols._
import java.util.{Calendar, TimeZone, Date}
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
import org.neo4j.cypher.CypherTypeException

class DateFunctions {

}

trait DateHelper {
  protected def asDate(a: Any): Date = a match {
    case null    => null
    case x: Date => x
    case _       => throw new CypherTypeException("Expected a date value for %s, but got: %s.".format(toString, a.toString))
  }
}

case class ToDateFunction(value: Expression) extends NullInNullOutExpression(value)  {
  def compute(value: Any, ctx: ExecutionContext)(implicit state: QueryState): Any = {
    value match {
      case value: Long => new Date(value)
    }
  }

  def rewrite(f: (Expression) => Expression) = f(ToDateFunction(value.rewrite(f)))

  def arguments = Seq(value)

  def calculateType(symbols: SymbolTable) = DateType()

  def symbolTableDependencies = value.symbolTableDependencies
}

case class DateformatFunction(date: Expression, pattern: Expression, timezone: Option[Expression]) extends NullInNullOutExpression(date) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val formatVal = asString(pattern(m))

    val dateVal = date(m) match {
      case value: Date => value
      case value: Long => new Date(value)
    }
    val dateFormat = new SimpleDateFormat(formatVal)

    if (timezone != None) {
      val timezoneInstance = TimeZone.getTimeZone(asString(timezone.get(m)))
      dateFormat.setTimeZone(timezoneInstance)
    }

    dateFormat.format(dateVal)
  }

  def innerExpectedType = StringType()

  def arguments = Seq(date, pattern) ++ timezone

  def rewrite(f: (Expression) => Expression) = f(DateformatFunction(date.rewrite(f), pattern.rewrite(f), timezone.map(_.rewrite(f))))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = {
    val m = date.symbolTableDependencies ++
            pattern.symbolTableDependencies

    val o = timezone.toSeq.flatMap(_.symbolTableDependencies.toSeq).toSet

    m ++ o
  }
}

case class DaterangeFunction(start: Expression, end: Expression, format: Option[Expression]) extends Expression with StringHelper with DateHelper {

  private val DEFAULT_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd")

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    val dateFormat = format match {
      case None => DEFAULT_DATE_FORMAT
      case Some(format) => new SimpleDateFormat(asString(format(ctx)))
    }

    val startDate = start(ctx) match {
      case start: String => dateFormat.parse(start)
      case start: Date   => asDate(start)
    }

    val startCalendar = Calendar.getInstance()
    startCalendar.setTime(startDate)

    val endDate = end(ctx) match {
      case end: String => dateFormat.parse(end)
      case end: Date   => asDate(end)
    }

    val endCalendar = Calendar.getInstance()
    endCalendar.setTime(endDate)

    val dateRange = ListBuffer.empty[Long]

    while (!startCalendar.after(endCalendar)) {
      dateRange.append(startCalendar.getTime.getTime)
      startCalendar.add(Calendar.DATE, 1)
    }

    dateRange.toSeq
  }

  def arguments = Seq(start, end)

  def rewrite(f: (Expression) => Expression) = f(DaterangeFunction(start.rewrite(f), end.rewrite(f), format.map(_.rewrite(f))))

  def calculateType(symbols: SymbolTable) = CollectionType(LongType())

  def symbolTableDependencies = {
    val m = start.symbolTableDependencies ++
            end.symbolTableDependencies

    val o = format.toSeq.flatMap(_.symbolTableDependencies.toSeq).toSet

    m ++ o
  }
}