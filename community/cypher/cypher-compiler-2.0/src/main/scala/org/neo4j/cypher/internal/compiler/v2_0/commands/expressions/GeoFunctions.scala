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

import org.neo4j.cypher.internal.compiler.v2_0.symbols.{DoubleType, SymbolTable}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import scala.math.{sin, cos, acos}

class GeoFunctions {

}

case class GeoDistanceFunction(lat1: Expression, lng1: Expression, lat2: Expression, lng2: Expression) extends Expression with NumericHelper {

  private val EARTH_RADIUS = 6378135.toDouble

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    // Calculation according to http://www.movable-type.co.uk/scripts/latlong.html
    val lat1Val = asDouble(lat1(ctx))
    val lng1Val = asDouble(lng1(ctx))

    val lat2Val = asDouble(lat2(ctx))
    val lng2Val = asDouble(lng2(ctx))

    // TODO implement radius as optional parameter
    val radiusVal = EARTH_RADIUS;

    acos(sin(lat1Val) * sin(lat2Val) + cos(lat1Val) * cos(lat2Val) * cos(lng2Val - lng1Val)) * radiusVal;
  }

  def calculateType(symbols: SymbolTable) = DoubleType()

  def arguments = Seq(lat1, lng1, lat2, lng2)

  def rewrite(f: (Expression) => Expression) = f(GeoDistanceFunction(lat1.rewrite(f), lng1.rewrite(f), lat2.rewrite(f), lng2.rewrite(f)))

  def symbolTableDependencies = lat1.symbolTableDependencies ++
                                lng1.symbolTableDependencies ++
                                lat2.symbolTableDependencies ++
                                lng2.symbolTableDependencies
}
