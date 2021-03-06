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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public final class SocratesCli {
    public static final String            HELP    = "help";
    public static final String            MODEL   = "model";
    public static final String            PEOPLE  = "people";
    public static final String            WORKERS = "workers";
    public static final String            SAMPLES = "samples";

    private static final Options           options = new Options();
    private static final CommandLineParser clp     = new DefaultParser();
    private static final HelpFormatter     hf  = new HelpFormatter();

    static {
        options.addOption( HELP, "Print help message." );
        options.addOption( MODEL,
                true,
                "File in which the final model will be saved. Also used as prefix for intermediate saves of the model." );
        options.addOption( PEOPLE, true, "CSV file containing all the people for training." );
        options.addOption( SAMPLES,
                true,
                "Number of samples to use from people file. If not specified all samples will be used." );
        options.addOption( WORKERS, true, "Number of worker threads to use. Defaults to number of processors." );

    }

    private SocratesCli() {
    }

    public static CommandLine parseCommandLine( String[] args ) throws ParseException {
        return clp.parse( options, args );
    }

    public static void printHelp() {
        hf.printHelp( "socrates", options );
    }
}
