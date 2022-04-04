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

import com.geekbeast.streams.StreamUtil;
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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import org.nd4j.jita.conf.CudaEnvironment;
import org.nd4j.linalg.activations.Activation;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.dataset.DataSet;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.linalg.lossfunctions.LossFunctions.LossFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class Socrates {
    public static final  Random r              = new Random();
    private static final Logger logger         = LoggerFactory.getLogger( Socrates.class );
    static               int    featureCount   = PersonMetric.values().length;
    private static       int    numMixes       = 10;
    private static       int    processorCount = Runtime.getRuntime().availableProcessors();

    public static void main( String[] args ) throws IOException {
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
        //        assert training.intersect( test ).count() == 0;
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

        MultiLayerNetwork model = trainModel( trainingSet, testingSet );

        saveModel( saveFile, model );
        System.exit( 0 );
    }

    public static List<List<Person>> partition( List<Person> people, int partitionSize ) {
        Stopwatch w = Stopwatch.createStarted();
        int numPartitions = ( people.size() / partitionSize ) + 1;
        List<List<Person>> partitions = new ArrayList<>( numPartitions );
        for ( int i = 0; i < people.size(); ) {
            List<Person> partition = new ArrayList( partitionSize );
            partitions.add( partition );
            for ( int j = i; j < partitionSize && i < partitions.size(); ++j ) {
                partition.add( people.get( i ) );
            }
        }
        logger.info( "Local partitioning took: {}", w.elapsed( TimeUnit.MILLISECONDS ) );
        w.stop();
        return partitions;
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

    public static Pair<double[][], double[][]> fAndL( Person person, List<Person> people ) {
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
        return Pair.of( features, labels );
    }

    public static Iterator<DataSet> buildSingle( Person person, Broadcast<List<Person>> p ) {
        return p.value().stream().map( other -> {
            double[][] features = new double[][] { PersonMetric.pDistance( person, other ) };
            double[][] labels = new double[][] { PersonLabel.pDistance( person, other ) };
            return new DataSet( Nd4j.create( features ), Nd4j.create( labels ) );
        } ).iterator();
    }

    public static Row computeFeaturesAndLabel( Tuple2<Person, Person> t ) {
        return computeFeaturesAndLabel( t._1(), t._2() );
    }

    public static Row computeFeaturesAndLabel( Person a, Person b ) {
        double[] features = PersonMetric.pDistance( a, b );
        double[] labels = PersonLabel.pDistance( a, b );
        Double[] r = new Double[ features.length + labels.length ];
        int i;
        for ( i = 0; i < features.length; ++i ) {
            r[ i ] = features[ i ];
        }

        for ( int j = 0; j < labels.length; ++j, ++i ) {
            r[ i ] = labels[ j ];
        }
        return RowFactory.create( r );
    }

    public static Row computeDistance( Tuple2<Person, Person> t ) {
        return RowFactory.create( PersonMetric.distance( t._1(), t._2() ) );
    }

    public static DataSet toDataSet( Tuple2<Person, Person> t ) {
        double[][] features = new double[][] { PersonMetric.pDistance( t._1(), t._2() ) };
        double[][] labels = new double[][] { PersonLabel.pDistance( t._1(), t._2() ) };

        //        double[] features = PersonMetric.pDistance( t._1(), t._2() ) ;
        //        double[] labels = PersonLabel.pDistance( t._1(), t._2() );

        return new DataSet( Nd4j.create( features ), Nd4j.create( labels ) );
    }

    public static DataSet toBalancedDataSet( Person a, List<Person> people ) {
        Stopwatch w = Stopwatch.createStarted();
        double[][] features = new double[ 2 * people.size() ][ 0 ];
        double[][] labels = new double[ 2 * people.size() ][ 0 ];

        for ( int i = 0; i < features.length; i += 2 ) {
            Person aPrime = new Person( a, true );
            features[ i ] = PersonMetric.pDistance( a, people.get( i >>> 1 ) );
            labels[ i ] = PersonLabel.pDistance( a, people.get( i >>> 1 ) );
            features[ i + 1 ] = PersonMetric.pDistance( a, aPrime );
            labels[ i + 1 ] = PersonLabel.pDistance( a, aPrime );
        }
        logger.info( "Generated {} sample dataset in {} ms", features.length, w.elapsed( TimeUnit.MILLISECONDS ) );
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
        return new DataSet( Nd4j.create( features ), Nd4j.create( labels ) );
    }

    public static DataSet toBalancedDataSet( Person a, Person b ) {
        double[][] features = new double[ 2 ][ 0 ];
        double[][] labels = new double[ 2 ][ 0 ];

        Person aPrime = new Person( a, true );
        features[ 0 ] = PersonMetric.pDistance( a, b );
        labels[ 0 ] = PersonLabel.pDistance( a, b );
        features[ 1 ] = PersonMetric.pDistance( a, aPrime );
        labels[ 1 ] = PersonLabel.pDistance( a, aPrime );
        return new DataSet( Nd4j.create( features ), Nd4j.create( labels ) );
    }

    public static Iterator<Person> explode( Row row ) {
        Person[] persons = new Person[ numMixes ];
        for ( int i = 0; i < persons.length; ++i ) {
            persons[ i ] = new Person( row, true );
        }

        return Arrays.asList( persons ).iterator();
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
        int exampleCount = 512;
        int partitionSize = 2048;
        int epochCount = 10;
        int dataIterations = iterations * exampleCount;
        int iteratorsPerPerson = ( training.size() / exampleCount ) * iterations;

        final ListMultimap<UUID, Person> peopleById = ArrayListMultimap.create();
        final Map<UUID, List<Person>> mm = Multimaps.asMap( peopleById );

        training.forEach( person -> peopleById.put( person.getId(), person ) );

        CudaEnvironment.getInstance().getConfiguration()
                .setMaximumDeviceCacheableLength( 2 * 1024 * 1024 * 1024L )
                .setMaximumDeviceCache( 9L * 1024 * 1024 * 1024L )
                .setMaximumHostCacheableLength( 2 * 1024 * 1024 * 1024L )
                .setMaximumHostCache( 9L * 1024 * 1024 * 1024L );

        UIServer uiServer = UIServer.getInstance();

        //Configure where the network information (gradients, score vs. time etc) is to be stored. Here: store in memory.
        StatsStorage statsStorage = new InMemoryStatsStorage();         //Alternative: new FileStatsStorage(File), for saving and loading later

        //Attach the StatsStorage instance to the UI: this allows the contents of the StatsStorage to be visualized
        uiServer.attach( statsStorage );
        MultiLayerConfiguration conf = new NeuralNetConfiguration.Builder()
                .weightInit( WeightInit.RELU )
                .activation( Activation.RELU )
                .miniBatch( true )
                .optimizationAlgo( OptimizationAlgorithm.STOCHASTIC_GRADIENT_DESCENT )
                //.regularization( true ).l2( 1 )
                //                .gradientNormalization( GradientNormalization.ClipL2PerLayer )
                //                .gradientNormalizationThreshold( .1 )
                .list()
                .backprop( true )
                .layer( 0, new DenseLayer.Builder().nIn( numInputs ).nOut( 2 * numInputs )
                        .build() )
//                .layer( 1, new DenseLayer.Builder().nIn( 2 * numInputs ).nOut( numInputs )
//                        .build() )
//                .layer( 2, new DenseLayer.Builder().nIn( numInputs ).nOut( numInputs )
//                        .build() )
                .layer( 1, new DenseLayer.Builder().nIn( 2*numInputs ).nOut( numInputs )
                        .build() )
                .layer( 2, new OutputLayer.Builder( LossFunction.NEGATIVELOGLIKELIHOOD )
                        .activation( Activation.SOFTMAX )
                        .nIn( numInputs ).nOut( outputNum ).build() )
                .build();
        MultiLayerNetwork model = new MultiLayerNetwork( conf );
        model.init();

        model.setListeners( new PerformanceListener( 4 * dataIterations ),
                new ScoreIterationListener( 4 * dataIterations ),
                new StatsListener( statsStorage ) );  //Print score every 10 parameter updates
        //        ParallelWrapper wrapper = new ParallelWrapper.Builder( model )
        //                // DataSets prefetching options. Set this value with respect to number of actual devices
        //                .prefetchBuffer( Runtime.getRuntime().availableProcessors() )
        //                // set number of workers equal to number of available devices. x1-x2 are good values to start with
        //                .workers( Runtime.getRuntime().availableProcessors() )
        //                // rare averaging improves performance, but might reduce model accuracy
        //                //                .workspaceMode( WorkspaceMode.SINGLE )
        //                //                .workspaceMode( WorkspaceMode.SEPARATE )
        //                //                .trainerFactory( new ParameterServerTrainerContext )
        //                //                .trainerFactory( new SymmetricTrainerContext() )
        //                .averagingFrequency( 16 )
        //                .reportScoreAfterAveraging( true )
        //                //                .trainingMode( ParallelWrapper.TrainingMode.CUSTOM )
        //                //                .gradientsAccumulator( new EncodedGradientsAccumulator( Runtime.getRuntime().availableProcessors(),
        //                //                        1e-3 ) )
        //
        //                .build();

        //List<List<Person>> partitions = Lists.partition( training, partitionSize );
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
                            INDArray output = model.output( testDataSet.getFeatures() );

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

            //            Iterator<DataSet> ds = training
            //                    .parallelStream()
            //                    .flatMap( person -> partitions.parallelStream()
            //                            .map( people -> toBalancedDataSet( person, people ) ) )
            //                    .peek( d -> {
            //                        if ( i.incrementAndGet() % 50000 == 0 ) {
            //                            logger.info( "\n\n\n*************  Intermediate Model Evaluation  *************\n\n\n" );
            //                            DataSet testDataSet = toBigDataSet( testData.next(), testing );
            //                            INDArray output = model.output( testDataSet.getFeatureMatrix() );
            //
            //                            eval.eval( testDataSet.getLabels(), output );
            //                            logger.info( eval.stats() );
            //                            try {
            //                                saveModel( new File( "model" + Integer.toString( i.get() ) + ".bin" ),
            //                                        model );
            //                            } catch ( IOException e ) {
            //                                logger.error( "Unable to save model!" );
            //                            }
            //                        }
            //                    } )
            //                    .iterator();
            model.fit( new IteratorDataSetIterator( ds, exampleCount ) );
            //            model.fit( new IteratorDataSetIterator( ds,exampleCount ) );
            while ( testData.hasNext() ) {
                DataSet testDataSet = toBigDataSet( testData.next(), testing );
                INDArray output = model.output( testDataSet.getFeatures() );

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

    public static Function<List<Person>, Stream<DataSet>> buildDataSetStream( Collection<List<Person>> groupsOfOtherPeople ) {
        return ( people ) ->
                people
                        .parallelStream()
                        .flatMap(
                                person -> groupsOfOtherPeople
                                        .stream()
                                        .map( otherPeople -> toBalancedDataSet( person,
                                                people,
                                                otherPeople ) ) );
    }

    public static DataSet buildRandom( Person person, List<Person> people, int numExamples ) {
        //        List<Person> people = bPeople.value();
        double[][] features = new double[ numExamples ][ 0 ];
        double[][] labels = new double[ numExamples ][ 0 ];
        List<ListenableFuture> fs = new ArrayList<>( numExamples );
        for ( int i = 0; i < numExamples; ++i ) {
            int index = i;
            fs.add(
                    ThreadpoolHelper.executor.submit( () -> {
                        features[ index ] = PersonMetric.pDistance( person, people.get( r.nextInt( people.size() ) ) );
                        labels[ index ] = PersonLabel.pDistance( person, people.get( r.nextInt( people.size() ) ) );
                    } ) );
        }
        fs.forEach( StreamUtil::getUninterruptibly );
        return new DataSet( Nd4j.create( features ), Nd4j.create( labels ) );
    }

}
