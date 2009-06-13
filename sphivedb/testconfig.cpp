/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <stdlib.h>

#include "sphiveconfig.hpp"

#include "spnetkit/spnklog.hpp"
#include "spserver/spporting.hpp"

int main( int argc, char * argv[] )
{
	if( argc < 2 ) {
		printf( "Usage: %s <config file>\n", argv[0] );
		exit( -1 );
	}

	const char * configFile = argv[1];

	int logopt = LOG_CONS | LOG_PID | LOG_PERROR;

	SP_NKLog::setLogLevel( 7 );
	sp_openlog( "testconfig", logopt, LOG_USER );

	SP_HiveConfig config;

	config.init( configFile );

	return 0;
}

