package com.openlattice.socrates;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtil {
    private static final Logger logger = LoggerFactory.getLogger( StreamUtil.class );

    private StreamUtil() {
    }

    public static <T> Stream<T> stream(Iterable<T> rs ) {
        return StreamSupport.stream( rs.spliterator(), false );
    }

    public static <T> Stream<T> parallelStream( Iterable<T> rs ) {
        return StreamSupport.stream( rs.spliterator(), true );
    }


    public static <V> V safeGet( ListenableFuture<V> f ) {
        try {
            return f.get();
        } catch ( InterruptedException | ExecutionException e ) {
            logger.error( "Error retrieving future value.", e );
            return null;
        }
    }

    public static <T> T getUninterruptibly( ListenableFuture<T> f ) {
        try {
            return Uninterruptibles.getUninterruptibly( f );
        } catch ( ExecutionException e ) {
            logger.error( "Unable to get future!", e );
            return null;
        }
    }
}
