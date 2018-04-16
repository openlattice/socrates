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

package com.openlattice.socrates;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.ListenableFuture;
import com.openlattice.socrates.training.Person;
import com.openlattice.socrates.training.PersonLabel;
import com.openlattice.socrates.training.PersonMetric;
import com.openlattice.socrates.training.SocratesCli;
import com.openlattice.socrates.training.ThreadpoolHelper;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileExistsException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.deeplearning4j.api.storage.StatsStorage;
import org.deeplearning4j.datasets.iterator.IteratorDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.deeplearning4j.nn.weights.WeightInit;
import org.deeplearning4j.optimize.listeners.PerformanceListener;
import org.deeplearning4j.optimize.listeners.ScoreIterationListener;
import org.deeplearning4j.ui.api.UIServer;
import org.deeplearning4j.ui.stats.StatsListener;
import org.deeplearning4j.ui.storage.InMemoryStatsStorage;
import org.deeplearning4j.util.ModelSerializer;
//import org.nd4j.jita.conf.CudaEnvironment;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Socrates {
    public static final  Random r              = new Random();
    private static final Logger logger         = LoggerFactory.getLogger( Socrates.class );
    private static       int    processorCount = Runtime.getRuntime().availableProcessors();

    public static void main( String[] args ) throws IOException {

        // GET ARGUMENTS

        final CommandLine cl;
        final String peopleCSV;
        final String modelFile;
        final int workers;
        final Optional<Integer> limit;
        try {
            cl = SocratesCli.parseCommandLine( args );

            Preconditions.checkArgument( cl.hasOption( "model" ), "Model output file must be specified!" );
            Preconditions.checkArgument( cl.hasOption( "people" ), "People input file must be specified!" );

            peopleCSV = cl.getOptionValue( SocratesCli.PEOPLE );
            modelFile = cl.getOptionValue( SocratesCli.MODEL );

            if ( cl.hasOption( SocratesCli.WORKERS ) ) {
                workers = Integer.parseInt( cl.getOptionValue( SocratesCli.WORKERS ) );
            } else {
                workers = processorCount;
            }

            if ( cl.hasOption( SocratesCli.SAMPLES ) ) {
                limit = Optional.of( Integer.parseInt( cl.getOptionValue( SocratesCli.SAMPLES ) ) );
            } else {
                limit = Optional.empty();
            }
        } catch ( ParseException e ) {
            logger.error( "Unable to parse command line", e );
            SocratesCli.printHelp();
            throw new IllegalArgumentException( "Invalid command line.", e );
        } catch ( NumberFormatException | IllegalStateException e ) {
            logger.error( "Invalid argument: {}", e.getMessage() );
            SocratesCli.printHelp();
            throw new IllegalArgumentException( "Invalid command line.", e );
        }

        File saveFile = new File( modelFile );
        if ( !saveFile.createNewFile() ) {
            logger.error( "File {} already exists!", saveFile.getCanonicalPath() );
            throw new FileExistsException( "File already exists." );
        } else {
            saveFile.delete();
        }

        // INITIATE SPARK SESSION, LOAD DATA

        SparkSession sparkSession = SparkSession.builder()
                .master( "local[" + Integer.toString( workers ) + "]" )
                .appName( "test" )
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext( sparkSession.sparkContext() );
        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( peopleCSV );

        if ( limit.isPresent() ) {
            payload = payload.limit( limit.get() );
        }

        // SET UP CROSS VALIDATION

        payload.cache();
        Dataset<String>[] idSets = payload
                .select( "trainingId" )
                .as( Encoders.STRING() )
                .distinct()
                .randomSplit( new double[] { .10, .9 } );
        long uniqueTestIdCount = idSets[ 0 ].count();
        long uniqueTrainingIdCount = idSets[ 1 ].count();

        logger.info( "Total number of distinct individuals: {}", uniqueTestIdCount + uniqueTrainingIdCount );
        logger.info( "Unique Test Id Count: {}", uniqueTestIdCount );
        logger.info( "Unique Training Id Count: {}", uniqueTrainingIdCount );
        Dataset<Row> test = idSets[ 0 ].join( payload, "trainingId" );
        Dataset<Row> training = idSets[ 1 ].join( payload, "trainingId" );
        logger.info( "Test size: {}", test.count() );
        logger.info( "Training size: {}", training.count() );
        logger.info( "# of training partitions: {}", training.javaRDD().getNumPartitions() );
        logger.info( "# of testing partitions: {}", test.javaRDD().getNumPartitions() );
        final JavaRDD<Person> prdd;
        final JavaRDD<Person> trdd;

        prdd = training.toJavaRDD().map( Person::new );
        trdd = test.toJavaRDD().map( Person::new );

        logger.info( "# of training partitions: {}", prdd.getNumPartitions() );
        logger.info( "# of testing partitions: {}", trdd.getNumPartitions() );

        List<Person> trainingSet = prdd.collect();
        List<Person> testingSet = trdd.collect();

        sparkSession.close();

        // TRAIN MODEL

        MultiLayerNetwork model = trainModel( trainingSet, testingSet );

        saveModel( saveFile, model );
        System.exit( 0 );
    }

    public static void saveModel( File file, MultiLayerNetwork model ) throws IOException {
        logger.info( "\n\n\n*************  SAVING MODEL *************\n\n\n" );
        ModelSerializer.writeModel( model, file, true );
    }

    public static MultiLayerNetwork trainModel(
            List<Person> training,
            List<Person> testing
    ) {
        int numInputs = PersonMetric.values().length;
        int outputNum = PersonLabel.values().length;
        int iterations = 1;
        int exampleCount = 10000;
        int epochCount = 1;
        int dataIterations = iterations * exampleCount;

        final ListMultimap<UUID, Person> peopleById = ArrayListMultimap.create();
        final Map<UUID, List<Person>> mm = Multimaps.asMap( peopleById );

        training.forEach( person -> peopleById.put( person.getId(), person ) );

//        CudaEnvironment.getInstance().getConfiguration()
//                .setMaximumDeviceCacheableLength( 2 * 1024 * 1024 * 1024L )
//                .setMaximumDeviceCache( 9L * 1024 * 1024 * 1024L )
//                .setMaximumHostCacheableLength( 2 * 1024 * 1024 * 1024L )
//                .setMaximumHostCache( 9L * 1024 * 1024 * 1024L );

        UIServer uiServer = UIServer.getInstance();

        //Configure where the network information (gradients, score vs. time etc) is to be stored. Here: store in memory.
        StatsStorage statsStorage = new InMemoryStatsStorage();         //Alternative: new FileStatsStorage(File), for saving and loading later

        //Attach the StatsStorage instance to the UI: this allows the contents of the StatsStorage to be visualized
        uiServer.attach( statsStorage );
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .iterations( iterations )
                .weightInit( WeightInit.RELU )
                .activation( Activation.RELU )
                .miniBatch( true )
                .optimizationAlgo( OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT )
                //.regularization( true ).l2( 1 )
                //                .gradientNormalization( GradientNormalization.ClipL2PerLayer )
                //                .gradientNormalizationThreshold( .1 )
                .learningRate( .000201 )
                .list()
                .backprop( true )
                .layer( 0, new DenseLayer.Builder().nIn( numInputs ).nOut( 2 )
                        .build() )
//                .layer( 1, new DenseLayer.Builder().nIn( 500 ).nOut( 500 )
//                        .build() )
//                .layer( 2, new DenseLayer.Builder().nIn( 500 ).nOut( 500 )
//                        .build() )
//                .layer( 3, new DenseLayer.Builder().nIn( 500 ).nOut( 500 )
//                        .build() )
                .layer( 1, new OutputLayer.Builder( LossFunction.NEGATIVELOGLIKELIHOOD )
                        .activation( Activation.SOFTMAX )
                        .nIn( 2 ).nOut( outputNum ).build() )
                .build();
        MultiLayerNetwork model = new MultiLayerNetwork( conf );
        model.init();

        model.setListeners( new PerformanceListener( dataIterations ),
                new ScoreIterationListener( dataIterations ),
                new StatsListener( statsStorage ) );

        AtomicInteger i = new AtomicInteger( 0 );
        logger.info( "\n\n\n*************  STARTING ITERATIVE TRAINING  *************\n\n\n" );
        for ( int epoch = 0; epoch < epochCount; ++epoch ) {
            final Evaluation eval = new Evaluation( 2 );
            logger.info( "\n\n\n*************  STARTING EPOCH {}  *************\n\n\n", epoch );
            final Iterator<Person> testData = testing.iterator();
            Iterator<DataSet> ds = mm
                    .values()
                    .parallelStream()
                    .flatMap( matching -> buildDataSetStream( matching, mm ) )
                    .peek( d -> {
                        if ( i.incrementAndGet() % 1000000 == 0 ) {
                            logger.info(
                                    "\n\n\n*************  Intermediate Model Evaluation  *************\n\n\n" );
                            DataSet testDataSet = toBigDataSet( testData.next(), testing );
                            INDArray output = model.output( testDataSet.getFeatureMatrix() );

                            eval.eval( testDataSet.getLabels(), output );
                            logger.info( eval.stats() );
                            try {
                                saveModel( new File( "model" + Integer.toString( i.get() ) + ".bin" ),
                                        model );
                            } catch ( IOException e ) {
                                logger.error( "Unable to save model!" );
                            }
                        }
                    } )
                    .iterator();

            model.fit( new IteratorDataSetIterator( ds, exampleCount ) );
            while ( testData.hasNext() ) {
                DataSet testDataSet = toBigDataSet( testData.next(), testing );
                INDArray output = model.output( testDataSet.getFeatureMatrix() );

                eval.eval( testDataSet.getLabels(), output );
                logger.info( eval.stats() );
            }

            logger.info( "\n\n\n*************  FINISHED EPOCH {} *************\n\n\n", epoch );
        }
        return model;
    }

    public static Stream<DataSet> buildDataSetStream( List<Person> matchingPeople, Map<UUID, List<Person>> everyone ) {
        return
                matchingPeople
                        .stream()
                        .flatMap( person ->
                                everyone
                                        .values()
                                        .stream()
                                        .map( others ->
                                                toBalancedDataSet( person, matchingPeople, others ) ) );

    }

    public static DataSet toBigDataSet( Person person, List<Person> people ) {
        double[][] features = new double[ 2 * people.size() ][ 0 ];
        double[][] labels = new double[ 2 * people.size() ][ 0 ];

        int increment = features.length / ThreadpoolHelper.procs;
        List<ListenableFuture> futures = new ArrayList( ThreadpoolHelper.procs );
        for ( int base = 0; base < features.length; ) {
            final int start = base;
            final int limit = Math.min( base + increment, features.length );
            futures.add( ThreadpoolHelper.executor.submit( () -> {
                        for ( int i = start; i < limit; ++i ) {
                            features[ i ] = PersonMetric
                                    .pDistance( person, i < people.size() ? people.get( i ) : new Person( person, true ) );
                        }

                        for ( int i = start; i < limit; ++i ) {
                            labels[ i ] = PersonLabel
                                    .pDistance( person, i < people.size() ? people.get( i ) : new Person( person, true ) );
                        }
                    }
            ) );
            base = limit;
        }
        futures.forEach( StreamUtil::getUninterruptibly );
        return new DataSet( Nd4j.create( features ), Nd4j.create( labels ) );
    }

    public static DataSet toBalancedDataSet( Person a, List<Person> matchingExamples, List<Person> otherPeople ) {
        Stopwatch w = Stopwatch.createStarted();
        double[][] features = new double[ 2 * otherPeople.size() ][ 0 ];
        double[][] labels = new double[ 2 * otherPeople.size() ][ 0 ];

        for ( int i = 0, j = 0; i < features.length; i += 2, ++j ) {
            Person matchingPerson = matchingExamples.get( j % matchingExamples.size() );
            Person otherPerson = otherPeople.get( j );
            features[ i ] = PersonMetric.pDistance( a, otherPerson );
            labels[ i ] = PersonLabel.pDistance( a, otherPerson );
            features[ i + 1 ] = PersonMetric.pDistance( a, matchingPerson );
            labels[ i + 1 ] = PersonLabel.pDistance( a, matchingPerson );
        }
        logger.debug( "Generated {} sample dataset in {} ms", features.length, w.elapsed( TimeUnit.MILLISECONDS ) );
        INDArray arr1 = Nd4j.create( features );
        INDArray arr2 = Nd4j.create( labels );

        return new DataSet( arr1 , arr2 );
    }

}
