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
import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public enum PersonLabel {
    MATCH( PersonLabel::isMatch );
    //    ;
//    ,
//    NOT_MATCH( PersonLabel::isNotMatch );

    private static final PersonMetric[] metrics = PersonMetric.values();
    private static final PersonLabel[]  labels  = PersonLabel.values();
    private static final StructType schema;


    static {
        List<StructField> fields = new ArrayList<>();
        for ( PersonMetric pm : metrics ) {
            StructField field = DataTypes.createStructField( pm.name(), DataTypes.DoubleType, true );
            fields.add( field );
        }

        for ( PersonLabel pm : labels ) {
            StructField field = DataTypes.createStructField( pm.name(), DataTypes.DoubleType, true );
            fields.add( field );
        }

        schema = DataTypes.createStructType( fields );
    }

    private final MetricExtractor label;

    PersonLabel( MetricExtractor label ) {
        this.label = label;
    }

    private double extract( Person lhs, Person rhs ) {
        return this.label.extract( lhs, rhs );
    }

    public static double isMatch( Person lhs, Person rhs ) {
        return lhs.isMatch( rhs );
    }

    public static double[] pDistance( Person lhs, Person rhs ) {
        double[] result = new double[ labels.length ];
        for ( int i = 0; i < result.length; ++i ) {
            result[ i ] = labels[ i ].extract( lhs, rhs );
        }
        return result;
    }
}
