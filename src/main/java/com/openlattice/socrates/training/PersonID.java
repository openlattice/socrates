/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
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
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.socrates.training;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public enum PersonID {
    LEFT_INT_ID( lhs( Person::getintId ) ),
    RIGHT_INT_ID( rhs( Person::getintId ) );

    private static final PersonID[] metrics = PersonID.values();
    private static final StructType schema;

    static {
        List<StructField> fields = new ArrayList<>();
        for ( PersonID pm : metrics ) {
            StructField field = DataTypes.createStructField( pm.name(), DataTypes.IntegerType, true );
            fields.add( field );
        }
        schema = DataTypes.createStructType( fields );
    }

    private final MetricExtractor metric;

    PersonID( MetricExtractor metric ) {
        this.metric = metric;
    }

    private double extract( Person lhs, Person rhs ) {
        return this.metric.extract( lhs, rhs );
    }

    public static double[] pDistance( Person lhs, Person rhs ) {
        double[] result = new double[ metrics.length ];
        for ( int i = 0; i < result.length; ++i ) {
            result[ i ] = metrics[ i ].extract( lhs, rhs );
        }
        return result;
    }

    public static MetricExtractor lhs( Function<Person, Integer> extractor ) {
        return ( lhs, rhs ) -> extractor.apply( lhs );
    }

    public static MetricExtractor rhs( Function<Person, Integer> extractor ) {
        return ( lhs, rhs ) -> extractor.apply( rhs );
    }

}
