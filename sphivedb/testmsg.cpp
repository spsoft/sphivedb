/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>

#include "sphivemsg.hpp"
#include "sphivecomm.hpp"

#include "spjson/spjsonrpc.hpp"

void testReq( const char * buffer, int len )
{
	SP_JsonRpcReqObject inner( buffer, len );

	if( NULL != inner.getPacketError() ) {
		printf( "packet.error %s\n", inner.getPacketError() );
		return;
	}

	SP_HiveReqObject req( &inner );

	printf( "path %s\n", req.getPath() );
	printf( "sql.count %d\n", req.getSqlCount() );

	for( int i = 0; i < req.getSqlCount(); i++ ) {
		printf( "sql#%d %s\n", i, req.getSql( i ) );
	}
}

void printResultSet( SP_HiveResultSet * rs )
{
	printf( "row.count %d\n", rs->getRowCount() );

	int column = rs->getColumnCount();

	for( int i = 0; i < column; i++ ) {
		printf( "\t%s(%s)", rs->getName(i), rs->getType(i) );
	}

	printf( "\n" );

	for( int i = 0; i < rs->getRowCount(); i++ ) {
		rs->moveTo( i );
		for( int j = 0; j < column; j++ ) {
			char tmp[ 32 ] = { 0 };
			const char * val = rs->getAsString( j, tmp, sizeof( tmp ) );

			printf( "\t[%s]", val );
		}

		printf( "\n" );
	}
}

void testResp( const char * buffer, int len )
{
	SP_JsonRpcRespObject inner( buffer, len );

	if( NULL != inner.getPacketError() ) {
		printf( "packet.error %s\n", inner.getPacketError() );
		return;
	}

	SP_HiveRespObject resp( &inner );

	printf( "result.count %d\n", resp.getResultCount() );

	for( int i = 0; i < resp.getResultCount(); i++ ) {
		SP_HiveResultSet * rs = resp.getResultSet( i );

		printResultSet( rs );

		delete rs;
	}
}

int main( int argc, char * argv[] )
{
	if( argc < 3 ) {
		printf( "Usage: %s <req/resp> <file>\n", argv[0] );
		exit( -1 );
	}

	const char * type = argv[1];
	const char * filename = argv[2];

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

	if( 0 == strcasecmp( type, "req" ) ) {
		testReq( source, aStat.st_size );
	} else {
		testResp( source, aStat.st_size );
	}

	free( source );

	return 0;
}

