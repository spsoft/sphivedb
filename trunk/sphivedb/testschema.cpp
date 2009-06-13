/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <stdlib.h>

#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"

#include "sphiveschema.hpp"
#include "sphiveconfig.hpp"

#include "sqlite3.h"

int main( int argc, char * argv[] )
{
	if( argc < 4 ) {
		printf( "Usage: %s <dbfile> <config> <dbname>\n", argv[0] );
		exit( -1 );
	}

	const char * dbfile = argv[1];
	const char * configFile = argv[2];
	const char * dbname = argv[3];

	int logopt = LOG_CONS | LOG_PID | LOG_PERROR;

	SP_NKLog::setLogLevel( 7 );
	openlog( "testschema", logopt, LOG_USER );

	sqlite3 * handle = NULL;

	sqlite3_open( dbfile, &handle );

	SP_HiveConfig config;
	config.init( configFile );

	SP_HiveSchemaManager manager;
	manager.init( &config );
	manager.ensureSchema( handle, dbname );

	sqlite3_close( handle );

	return 0;
}

