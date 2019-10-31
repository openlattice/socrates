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

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public enum PersonMetric {
    FIRST_NAME_STRING( jaroWinkler( Person::getFirstName ) ),
    FIRST_NAME_METAPHONE( metaphone( Person::getFirstName ) ),
    FIRST_NAME_METAPHONE_ALT( metaphoneAlternate( Person::getFirstName ) ),
    FIRST_NAME_LHS_PRESENCE( lhs( Person::getHasFirstName ) ),
    FIRST_NAME_RHS_PRESENCE( rhs( Person::getHasFirstName ) ),
    FIRST_NAME_LHS_PROBA( lhsdouble( Person::getFirstProba ) ),
    FIRST_NAME_RHS_PROBA( rhsdouble( Person::getFirstProba ) ),
    FIRST_NAME_LHS_LENGTH( lhs( Person::getFirstNameLength ) ),
    FIRST_NAME_RHS_LENGTH( rhs( Person::getFirstNameLength ) ),

    MIDDLE_NAME_STRING( jaroWinkler( Person::getMiddleName ) ),
    MIDDLE_NAME_METAPHONE( metaphone( Person::getMiddleName ) ),
    MIDDLE_NAME_METAPHONE_ALT( metaphoneAlternate( Person::getMiddleName ) ),
    MIDDLE_NAME_LHS_PRESENCE( lhs( Person::getHasMiddleName ) ),
    MIDDLE_NAME_RHS_PRESENCE( rhs( Person::getHasMiddleName ) ),
    MIDDLE_NAME_LHS_LENGTH( lhs( Person::getMiddleNameLength ) ),
    MIDDLE_NAME_RHS_LENGTH( rhs( Person::getMiddleNameLength ) ),

    LAST_NAME_STRINGG( jaroWinkler( Person::getLastName ) ),
    LAST_NAME_METAPHONE( metaphone( Person::getLastName ) ),
    LAST_NAME_METAPHONE_ALT( metaphoneAlternate( Person::getLastName ) ),
    LAST_NAME_LHS_PRESENCE( lhs( Person::getHasLastName ) ),
    LAST_NAME_RHS_PRESENCE( rhs( Person::getHasLastName ) ),
    LAST_NAME_LHS_PROBA( lhsdouble( Person::getLastProba ) ),
    LAST_NAME_RHS_PROBA( rhsdouble( Person::getLastProba ) ),
    LAST_NAME_LHS_LENGTH( lhs( Person::getLastNameLength ) ),
    LAST_NAME_RHS_LENGTH( rhs( Person::getLastNameLength ) ),

    SEX_STRING( jaroWinkler( Person::getSex ) ),
    SEX_LHS_PRESENCE( lhs( Person::getHasSex ) ),
    SEX_RHS_PRESENCE( rhs( Person::getHasSex ) ),

    DOB_STRING( jaroWinkler( Person::getDob ) ),
    DOB_LHS_PRESENCE( lhs( Person::getHasDob ) ),
    DOB_RHS_PRESENCE( rhs( Person::getHasDob ) ),
    DOB_DIFF( dobStr() ),

    RACE_STRING( jaroWinkler( Person::getRace ) ),
    RACE_LHS_PRESENCE( lhs( Person::getHasRace ) ),
    RACE_RHS_PRESENCE( rhs( Person::getHasRace ) ),

    ETHNICITY_STRING( jaroWinkler( Person::getEthnicity ) ),
    ETHNICITY_LHS_PRESENCE( lhs( Person::getHasEthnicity ) ),
    ETHNICITY_RHS_PRESENCE( rhs( Person::getHasEthnicity ) ),

    SSN_STRING( jaroWinkler( Person::getSsn ) ),
    SSN_LHS_PRESENCE( lhs( Person::getHasSsn ) ),
    SSN_RHS_PRESENCE( rhs( Person::getHasSsn ) ),

    INTID_1( lhs( Person::getintId ) ),
    INTID_2( rhs( Person::getintId ) );

    private static final PersonMetric[] metrics = PersonMetric.values();
    private static final DoubleMetaphone doubleMetaphone = new DoubleMetaphone();

    static {
        List<StructField> fields = new ArrayList<>();
        for ( PersonMetric pm : metrics ) {
            StructField field = DataTypes.createStructField( pm.name(), DataTypes.DoubleType, true );
            fields.add( field );
        }
    }

    private final MetricExtractor metric;

    PersonMetric( MetricExtractor metric ) {
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

    public static MetricExtractor lhsdouble( Function<Person, Double> extractor ) {
        return ( lhs, rhs ) -> extractor.apply( lhs );
    }

    public static MetricExtractor rhsdouble( Function<Person, Double> extractor ) {
        return ( lhs, rhs ) -> extractor.apply( rhs );
    }

    public static MetricExtractor jaroWinkler( Function<Person, String> extractor ) {
        return ( lhs, rhs ) -> getStringDistance( extractor.apply( lhs ), extractor.apply( rhs ), false, false );
    }

    public static MetricExtractor metaphone( Function<Person, String> extractor ) {
        return ( lhs, rhs ) -> getStringDistance( extractor.apply( lhs ), extractor.apply( rhs ), true, false );
    }

    public static MetricExtractor metaphoneAlternate( Function<Person, String> extractor ) {
        return ( lhs, rhs ) -> getStringDistance( extractor.apply( lhs ), extractor.apply( rhs ), true, true );
    }

    public static MetricExtractor dobStr() {
        return ( lhs, rhs ) -> ( 8 - StringUtils.getLevenshteinDistance( lhs.getDobStr(), rhs.getDobStr() ) ) / 8.0;
    }

    public static double getStringDistance( String lhs, String rhs, boolean useMetaphone, boolean alternate ) {
        if ( lhs == null ) {
            lhs = "";
        }

        if ( rhs == null ) {
            rhs = "";
        }
        if ( useMetaphone ) {
            if ( StringUtils.isNotBlank( lhs ) ) {
                lhs = doubleMetaphone.doubleMetaphone( lhs, alternate );
            }
            if ( StringUtils.isNotBlank( rhs ) ) {
                rhs = doubleMetaphone.doubleMetaphone( rhs, alternate );
            }
        }

        return StringUtils.getJaroWinklerDistance( lhs, rhs );
    }
}
