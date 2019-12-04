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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Optional;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Person implements Serializable {
    private static DateTimeHelper   dtHelper  = new DateTimeHelper( DateTimeZone.UTC, "MM/dd/yyyy" );
    private static DateTimeHelper   dtHelper2 = new DateTimeHelper( DateTimeZone.UTC, "yyyy-MM-dd" );
    private static DecimalFormat    dd        = new DecimalFormat( "00" );
    private final  UUID             trainingId;
    private final  Optional<String> firstName;
    private final  Optional<String> lastName;
    private final  Optional<String> sex;
    private final  Optional<String> ssn;
    private final  Optional<String> dob;
    private final  Optional<String> ethnicity;
    private final  Optional<String> race;
    private final  Optional<String> dobStr;

    public Person( Row row ) {
        this( row, false );
    }

    public Person( Row row, boolean perturb ) {
        this(
                UUID.fromString( row.getAs( "trainingId" ) ),
                row.getAs( "firstName" ),
                row.getAs( "lastName" ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "sex" ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "ssn" ),
                tryParseDob( row ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "race" ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "ethnicity" )

        );

        //        this(
        //                UUID.fromString( row.getString( 0 ) ),
        //                perturb && RandomUtils.nextBoolean() ? null : row.getString(1),
        //                perturb && RandomUtils.nextBoolean() ? null : row.getString(2),
        //                perturb && RandomUtils.nextBoolean() ? null : row.getString(3),
        //                perturb && RandomUtils.nextBoolean() ? null : row.getString(4),
        //                perturb && RandomUtils.nextBoolean() ? null : tryParseDob( row ),
        //                perturb && RandomUtils.nextBoolean() ? null : row.getString(6)
        //
        //        );
    }

    public Person( Person p, boolean perturb ) {
        this.trainingId = UUID.fromString( p.getTrainingId() );
        this.firstName = Optional.ofNullable( p.getFirstName() );
        this.lastName = Optional.ofNullable( p.getLastName() );
        this.sex = Optional.ofNullable( perturb && RandomUtils.nextBoolean() ? null : p.getSex() );
        this.ssn = Optional.ofNullable( perturb && RandomUtils.nextBoolean() ? null : p.getSsn() );
        this.dob = Optional.ofNullable( p.getDob() );
        this.ethnicity = Optional.ofNullable( perturb && RandomUtils.nextBoolean() ? null : p.getRace() );
        this.race = Optional.ofNullable( perturb && RandomUtils.nextBoolean() ? null : p.getEthnicity() );

        if ( dob.isPresent() ) {
            dobStr = p.dobStr;
        } else {
            dobStr = Optional.of( "" );
        }
    }

    public Person(
            UUID trainingId,
            String firstName,
            String lastName,
            String sex,
            String ssn,
            DateTime dob,
            String race,
            String ethnicity ) {
        this.trainingId = trainingId;
        this.firstName = Optional.ofNullable( firstName );
        this.lastName = Optional.ofNullable( lastName );
        this.sex = Optional.ofNullable( sex );
        this.ssn = Optional.ofNullable( ssn );
        this.dob = dob == null ? Optional.empty() : Optional.ofNullable( dob.toString() );
        this.race = Optional.ofNullable( race );
        this.ethnicity = Optional.ofNullable( ethnicity );
        if ( this.dob.isPresent() ) {
            this.dobStr = Optional.of(
                    dd.format( dob.getDayOfMonth() )
                            + dd.format( dob.getMonthOfYear() )
                            + dob.getYear() );
        } else {
            dobStr = Optional.of( "" );
        }
    }

    public String getDobStr() {
        return dobStr.orElse( "" );
    }

    public UUID getId() {
        return trainingId;
    }

    public String getTrainingId() {
        return trainingId.toString();
    }

    public String getFirstName() {
        return firstName.orElse( null );
    }

    public String getLastName() {
        return lastName.orElse( null );
    }

    public String getSex() {
        return sex.orElse( null );
    }

    public String getSsn() {
        return ssn.orElse( null );
    }

    public String getDob() {
        return dob.orElse( null );
    }

    public String getRace() {
        return race.orElse( null );
    }

    public String getEthnicity() {
        return ethnicity.orElse( null );
    }

    public int getHasFirstName() {
        return firstName.isPresent() ? 1 : 0;
    }

    public int getHasLastName() {
        return lastName.isPresent() ? 1 : 0;
    }

    public int getHasSex() {
        return sex.isPresent() ? 1 : 0;
    }

    public int getHasSsn() {
        return ssn.isPresent() ? 1 : 0;
    }

    public int getHasDob() {
        return dob.isPresent() ? 1 : 0;
    }

    public int getHasRace() {
        return race.isPresent() ? 1 : 0;
    }

    public int getHasEthnicity() {
        return ethnicity.isPresent() ? 1 : 0;
    }

    public int isMatch( Person other ) {
        return trainingId.equals( other.trainingId ) ? 1 : 0;
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof Person ) ) { return false; }

        Person person = (Person) o;

        if ( !firstName.equals( person.firstName ) ) { return false; }
        if ( !lastName.equals( person.lastName ) ) { return false; }
        if ( !ssn.equals( person.ssn ) ) { return false; }
        if ( !dob.equals( person.dob ) ) { return false; }
        return ethnicity.equals( person.ethnicity );
    }

    @Override public int hashCode() {
        int result = firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + ssn.hashCode();
        result = 31 * result + dob.hashCode();
        result = 31 * result + ethnicity.hashCode();
        return result;
    }

    private static DateTime tryParseDob( Row row ) {
        String dob = row.getAs( "dob" );
        if ( StringUtils.isBlank( dob ) ) { return null; }
        if ( dob.contains( "-" ) ) {
            return dtHelper2.parseLDT( dob );
        }
        return dtHelper.parseLDT( dob );
    }
}
