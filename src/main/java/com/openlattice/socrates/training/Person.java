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

import com.google.common.base.Optional;

import java.io.IOException;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.UUID;
import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Person implements Serializable {
    private static DateTimeHelper dtHelper  = new DateTimeHelper( DateTimeZone.UTC, "MM/dd/yyyy" );
    private static DateTimeHelper dtHelper2 = new DateTimeHelper( DateTimeZone.UTC, "yyyy-MM-dd" );
    private static DecimalFormat  dd        = new DecimalFormat( "00" );
    private final UUID             trainingId;
    private final int intId;
    private final Optional<String> firstName;
    private final Optional<String> lastName;
    private final Optional<String> sex;
    private final Optional<String> ssn;
    private final Optional<String> dob;
    private final Optional<String> ethnicity;
    private final Optional<String> race;
    private final Optional<String> dobStr;

    public Person( Row row ) {
        this( row, false );
    }

    public Person( Row row, boolean perturb ) {
        this(
                UUID.fromString( row.getAs( "trainingId" ) ),
                tryParseID( row ),
                row.getAs( "firstName" ),
                row.getAs( "lastName" ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "sex" ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "ssn" ),
                tryParseDob( row ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "race" ),
                perturb && RandomUtils.nextBoolean() ? null : row.getAs( "ethnicity" )

        );
    }

    public Person( Person p, boolean perturb ) {
        this.trainingId = UUID.fromString( p.getTrainingId() );
        this.intId = p.getintId() ;
        this.firstName = Optional.fromNullable( p.getFirstName() );
        this.lastName = Optional.fromNullable( p.getLastName() );
        this.sex = Optional.fromNullable( perturb && RandomUtils.nextBoolean() ? null : p.getSex() );
        this.ssn = Optional.fromNullable( perturb && RandomUtils.nextBoolean() ? null : p.getSsn() );
        this.dob = Optional.fromNullable( p.getDob() );
        this.ethnicity = Optional.fromNullable( perturb && RandomUtils.nextBoolean() ? null : p.getRace() );
        this.race = Optional.fromNullable( perturb && RandomUtils.nextBoolean() ? null : p.getEthnicity() );

        if ( dob.isPresent() ) {
            dobStr = p.dobStr;
        } else {
            dobStr = Optional.of( "" );
        }
    }

    public Person(
            UUID trainingId,
            int intId,
            String firstName,
            String lastName,
            String sex,
            String ssn,
            DateTime dob,
            String race,
            String ethnicity ) {
        this.trainingId = trainingId;
        this.firstName = Optional.fromNullable( firstName );
        this.lastName = Optional.fromNullable( lastName );
        this.intId =  intId ;
        this.sex = Optional.fromNullable( sex );
        this.ssn = Optional.fromNullable( ssn );
        this.dob = dob == null ? Optional.absent() : Optional.fromNullable( dob.toString() );
        this.race = Optional.fromNullable( race );
        this.ethnicity = Optional.fromNullable( ethnicity );
        if ( this.dob.isPresent() ) {
            this.dobStr = Optional.of(
                    dd.format( dob.getDayOfMonth() )
                            + dd.format( dob.getMonthOfYear() )
                            + Integer.valueOf( dob.getYear() ) );
        } else {
            dobStr = Optional.of( "" );
        }
    }

    public String getDobStr() {
        return dobStr.or( "" );
    }

    public UUID getId() {
        return trainingId;
    }

    public int getintId() {
        return Integer.valueOf(intId);
    }

    public String getTrainingId() {
        return trainingId.toString();
    }

    public String getFirstName() {
        return firstName.orNull();
    }

    public String getLastName() {
        return lastName.orNull();
    }

    public String getSex() {
        return sex.orNull();
    }

    public String getSsn() {
        return ssn.orNull();
    }

    public String getDob() {
        return dob.orNull();
    }

    public String getRace() {
        return race.orNull();
    }

    public String getEthnicity() {
        return ethnicity.orNull();
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

    private static int tryParseID( Row row ) {
        String id = row.getAs( "intId" );
        int integerid = Integer.parseInt( id );

        return integerid;
    }

    public double getProba(boolean first) {
        double prob = 0;
        try {
            String splitBy = ",";
            String namefile = "";
            if (first == true) {
                namefile = "/Users/jokedurnez/Documents/projects/socrates/data/firstnames.csv";
            } else {
                namefile = "/Users/jokedurnez/Documents/projects/socrates/data/lastnames.csv";

            }
            BufferedReader br = new BufferedReader(new FileReader(namefile));
            String line;
            while ((line = br.readLine()) != null) {
                String[] b = line.split(splitBy);
                if ( this.firstName.isPresent() && this.firstName.get().toLowerCase().equals(b[1].toLowerCase())) {
                    prob = Double.parseDouble(b[2]);
                    break;
                }
            }
            br.close();
        } catch (IOException e){
            throw new RuntimeException(e);
        }
        return prob;
    }

    public double getFirstProba() {
        return getProba(true);
    }

    public double getLastProba() {
        return getProba(false);
    }
}
