package com.openlattice.socrates;

import com.dataloom.streams.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.openlattice.socrates.training.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileExistsException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.deeplearning4j.eval.Evaluation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.deeplearning4j.api.storage.StatsStorage;
import org.deeplearning4j.datasets.iterator.IteratorDataSetIterator;
import org.deeplearning4j.eval.Evaluation;
import org.deeplearning4j.nn.modelimport.keras.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.api.OptimizationAlgorithm;
import org.deeplearning4j.nn.conf.MultiLayerConfiguration;
import org.deeplearning4j.nn.conf.NeuralNetConfiguration;
import org.deeplearning4j.nn.conf.layers.DenseLayer;
import org.deeplearning4j.nn.conf.layers.OutputLayer;
import org.deeplearning4j.nn.modelimport.keras.UnsupportedKerasConfigurationException;
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

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class Predictor {

    private static final Logger logger         = LoggerFactory.getLogger( Socrates.class );

    public static void main( String[] args ) throws IOException,InvalidKerasConfigurationException,UnsupportedKerasConfigurationException {

        // MANUAL ARGUMENTS
        final String peopleCSV = "/Users/jokedurnez/Documents/projects/socrates/data/Fake_original/people_newlist.csv";
        final String keras_model = "/Users/jokedurnez/Documents/projects/socrates/data/keras_model.json";
        final String keras_weights = "/Users/jokedurnez/Documents/projects/socrates/data/keras_weights.h5";
        final int limit = 100;
        MultiLayerNetwork model = KerasModelImport.importKerasSequentialModelAndWeights(keras_model,keras_weights);

        // INITIATE SPARK SESSION, LOAD DATA

        SparkSession sparkSession = SparkSession.builder()
                .master( "local[" + Integer.toString( 4 ) + "]" )
                .appName( "test" )
                .getOrCreate();
        JavaSparkContext sc = JavaSparkContext.fromSparkContext( sparkSession.sparkContext() );
        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( peopleCSV );

        payload = payload.limit( limit );

        // SET UP CROSS VALIDATION

        final JavaRDD<Person> trdd;
        final Evaluation eval = new Evaluation( 2 );

        trdd = payload.toJavaRDD().map( Person::new );

        List<Person> testingSet = trdd.collect();

        sparkSession.close();

        final Iterator<Person> testData = testingSet.iterator();

        while ( testData.hasNext() ) {
            DataSet testDataSet = toBigDataSet( testData.next(), testingSet );
            INDArray output = model.output( testDataSet.getFeatureMatrix() );

            eval.eval( testDataSet.getLabels(), output );
            logger.info( eval.stats() );
        }

    }


    public static DataSet toBigDataSet( Person person, List<Person> people ) {

        int intId = person.getintId();
        List<Person> smallist = people.subList(intId,people.size());

        double[][] features = new double[ smallist.size() ][ 0 ];
        double[][] labels = new double[smallist.size()][0];

        for ( int i = 0; i < features.length; ++i) {
            labels[i] = PersonLabel
                    .pDistance(person, i < smallist.size() ? smallist.get(i) : new Person(person, true));
            features[ i ] = PersonMetric
                    .pDistance( person, i < smallist.size() ? smallist.get( i ) : new Person( person, true ) );
        }

        INDArray arr_feat = Nd4j.create( features );
        INDArray arr_lbl = Nd4j.create( labels );

        System.out.println(intId);

        return new DataSet( arr_feat,arr_lbl );
    }



}
