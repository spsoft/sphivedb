/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>

#include "sphivemanager.hpp"
#include "sphivemsg.hpp"

#include "spjson/spjsonnode.hpp"
#include "spjson/spjsonrpc.hpp"
#include "spjson/spjsonutils.hpp"

void testExecute( const char * buffer, int len )
{
	SP_JsonRpcReqObject rpcReq( buffer, len );

	if( NULL != rpcReq.getPacketError() ) {
		printf( "packet.error %s\n", rpcReq.getPacketError() );
		return;
	}

	SP_HiveManager manager;

	manager.init( "./data" );

	SP_JsonArrayNode result;

	manager.execute( &rpcReq, &result );

	SP_JsonStringBuffer respBuffer;
	SP_JsonRpcUtils::toRespBuffer( rpcReq.getID(), &result,
			NULL, &respBuffer );

	printf( "%s\n", respBuffer.getBuffer() );
}

int main( int argc, char * argv[] )
{
	if( argc < 2 ) {
		printf( "Usage: %s <req file>\n", argv[0] );
		exit( -1 );
	}

	const char * filename = argv[1];

	FILE * fp = fopen ( filename, "r" );
	if( NULL == fp ) {
		printf( "cannot not open %s\n", filename );
		exit( -1 );
	}

	struct stat aStat;
	char * source = NULL;
	stat( filename, &aStat );
	source = ( char * ) malloc ( aStat.st_size + 1 );
	fread ( source, aStat.st_size, sizeof ( char ), fp );
	fclose ( fp );
	source[ aStat.st_size ] = '\0';

	testExecute( source, aStat.st_size );

	free( source );

	return 0;
}

