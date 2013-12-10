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
import scala.math.{sin, cos, acos, toRadians}

class GeoFunctions {

}

case class GeoDistanceFunction(lat1: Expression, lng1: Expression, lat2: Expression, lng2: Expression) extends Expression with NumericHelper {

  private val EARTH_RADIUS = 6378135.toDouble

  def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
    // Calculation according to http://www.movable-type.co.uk/scripts/latlong.html
    // Also refer to https://code.google.com/p/geojs/source/browse/trunk/src/math/earth.js
    val lat1Rad = toRadians(asDouble(lat1(ctx)))
    val lng1Rad = toRadians(asDouble(lng1(ctx)))

    val lat2Rad = toRadians(asDouble(lat2(ctx)))
    val lng2Rad = toRadians(asDouble(lng2(ctx)))

    // TODO implement radius as optional parameter
    val radiusVal = EARTH_RADIUS;

    acos(sin(lat1Rad) * sin(lat2Rad) + cos(lat1Rad) * cos(lat2Rad) * cos(lng2Rad - lng1Rad)) * radiusVal;
  }

  def calculateType(symbols: SymbolTable) = DoubleType()

  def arguments = Seq(lat1, lng1, lat2, lng2)

  def rewrite(f: (Expression) => Expression) = f(GeoDistanceFunction(lat1.rewrite(f), lng1.rewrite(f), lat2.rewrite(f), lng2.rewrite(f)))

  def symbolTableDependencies = lat1.symbolTableDependencies ++
                                lng1.symbolTableDependencies ++
                                lat2.symbolTableDependencies ++
                                lng2.symbolTableDependencies
}
