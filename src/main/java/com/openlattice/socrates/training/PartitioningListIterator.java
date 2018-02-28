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

import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.List;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class PartitioningListIterator implements Iterator<DataSet> {
    private final int          partitionSize;
    private final Person       person;
    private final List<Person> people;
    private final int index = 0;
    private final int maxIndex;

    public PartitioningListIterator( Person person, List<Person> people, int partitionSize ) {
        this.partitionSize = partitionSize;
        this.person = person;
        this.people = people;
        this.maxIndex = people.size() - 1;
    }

    @Override public boolean hasNext() {
        return index < people.size();
    }

    @Override public DataSet next() {
        checkState( hasNext() );
        List<Person> nextPeople = people.subList( index, Math.min( index + partitionSize, maxIndex ) );
        double[][] features = new double[ nextPeople.size() ][ 0 ];
        double[][] labels = new double[ nextPeople.size() ][ 0 ];

        for ( int i = 0; i < nextPeople.size(); ++i ) {
            Person other = nextPeople.get( i );
            features[ i ] = PersonMetric.pDistance( person, other );
            labels[ i ] = PersonLabel.pDistance( person, other );
        }
        return new DataSet( Nd4j.create( features ), Nd4j.create( labels ) );
    }
}
