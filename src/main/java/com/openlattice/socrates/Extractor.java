package com.openlattice.socrates;

import com.google.common.base.Preconditions;
import com.openlattice.socrates.training.Person;
import com.openlattice.socrates.training.PersonLabel;
import com.openlattice.socrates.training.PersonMetric;
import java.util.List;
import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import com.openlattice.socrates.training.SocratesCli;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

public class Extractor {

    public static void main( String[] args ) {

        final CommandLine cl;
        final String peopleCSV;
        final String featdir;
        final int workers;
        final Optional<Integer> limit;
        try {
            cl = SocratesCli.parseCommandLine( args );

            Preconditions.checkArgument( cl.hasOption( "people" ), "People input file must be specified!" );
            Preconditions.checkArgument( cl.hasOption( "featdir" ), "Feature directory must be specified!" );

            peopleCSV = cl.getOptionValue( SocratesCli.PEOPLE );
            featdir = cl.getOptionValue( SocratesCli.FEATDIR );

            if ( cl.hasOption( SocratesCli.WORKERS ) ) {
                workers = Integer.parseInt( cl.getOptionValue( SocratesCli.WORKERS ) );
            } else {
                workers = Runtime.getRuntime().availableProcessors();
            }

            if ( cl.hasOption( SocratesCli.SAMPLES ) ) {
                limit = Optional.of( Integer.parseInt( cl.getOptionValue( SocratesCli.SAMPLES ) ) );
            } else {
                limit = Optional.empty();
            }
        } catch ( ParseException e ) {
            SocratesCli.printHelp();
            throw new IllegalArgumentException( "Invalid command line.", e );
        } catch ( NumberFormatException | IllegalStateException e ) {
            SocratesCli.printHelp();
            throw new IllegalArgumentException( "Invalid command line.", e );
        }

        // MANUAL ARGUMENTS
        final Boolean writeLabels = true;

        // INITIATE SPARK SESSION, LOAD DATA

        SparkSession sparkSession = SparkSession.builder()
                .master( "local[" + Integer.toString( workers ) + "]" )
                .appName( "test" )
                .getOrCreate();
        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( peopleCSV );

        // GET LIST OF PERSONS

        if ( limit.isPresent() ) {
            payload = payload.limit( limit.get() );
        }

        final JavaRDD<Person> prdd;
        prdd = payload.toJavaRDD().map( Person::new );
        List<Person> personList = prdd.collect();
        sparkSession.close();

        // GET FEATURES

        getFeatures(personList, featdir, writeLabels);

        System.exit( 0 );
    }

    public static void getFeatures(List<Person> personList,String featuredir, Boolean writeLabels) {
        AtomicInteger index = new AtomicInteger();
        System.out.println(personList.size());

        final int numfiles = personList
                .parallelStream()
                .mapToInt(person -> toBigDataSet(person, personList, featuredir, writeLabels))
                .sum();
        System.out.println("_________________________");
        System.out.println(numfiles);
    }

    public static int toBigDataSet( Person person, List<Person> people, String featuredir, Boolean writeLabels) {

        int intId = person.getintId();
        String ft = featuredir+"features_"+intId;
        File ftfile = new File(ft);
        if (ftfile.exists()){
            System.out.println(ft+" already exists ! yay !");
            return 1;
        }
        List<Person> smallist = people.subList(intId,people.size());

        double[][] features = new double[ smallist.size() ][ 0 ];

        for ( int i = 0; i < features.length; ++i) {
            features[ i ] = PersonMetric
                    .pDistance( person, i < smallist.size() ? smallist.get( i ) : new Person( person, true ) );
                    }

        INDArray arr_feat = Nd4j.create( features );

        System.out.println(intId);

        Nd4j.writeTxt(arr_feat, ft);

        if (writeLabels) {
            String lbl = featuredir + "labels_" + intId;
            double[][] labels = new double[smallist.size()][0];
            for ( int i = 0; i < features.length; ++i) {
                labels[i] = PersonLabel
                        .pDistance(person, i < smallist.size() ? smallist.get(i) : new Person(person, true));
            }
            INDArray arr_lbl = Nd4j.create( labels );
            Nd4j.writeTxt(arr_lbl, lbl);
        }


        return 1;
    }

}
