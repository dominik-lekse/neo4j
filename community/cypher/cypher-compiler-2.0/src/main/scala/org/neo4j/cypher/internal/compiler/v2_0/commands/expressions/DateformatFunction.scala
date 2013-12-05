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
import java.util.Date
import java.text.SimpleDateFormat

case class DateformatFunction(timestamp: Expression, format: Expression) extends NullInNullOutExpression(timestamp) with StringHelper with NumericHelper {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): Any = {
    val timestampVal = asLong(timestamp(m))
    val formatVal = asString(format(m))

    val timestampDate = new Date(timestampVal)
    val dateFormat = new SimpleDateFormat(formatVal)

    dateFormat.format(timestampDate)
  }

  def innerExpectedType = StringType()

  def arguments = Seq(timestamp, format)

  def rewrite(f: (Expression) => Expression) = f(DateformatFunction(timestamp.rewrite(f), format.rewrite(f)))

  def calculateType(symbols: SymbolTable) = StringType()

  def symbolTableDependencies = timestamp.symbolTableDependencies ++
                                format.symbolTableDependencies
}