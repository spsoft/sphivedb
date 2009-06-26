/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>

#include "spnetkit/spnksocket.hpp"
#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkgetopt.h"
#include "spnetkit/spnklist.hpp"

#include "sphivedbcli.hpp"
#include "sphivemsg.hpp"
#include "sphivecomm.hpp"

void showUsage( const char * program )
{
	printf( "\n\n%s [-h host] [-p port] [-i dbfile] [-u user] [-n dbname] [-s sql] [-o] [-v]\n", program );

	printf( "\t-h http host\n" );
	printf( "\t-p http port\n" );
	printf( "\t-i dbfile\n" );
	printf( "\t-u user\n" );
	printf( "\t-n dbname\n" );
	printf( "\t-s sql script\n" );
	printf( "\t-o log socket io\n" );
	printf( "\t-v show this usage\n" );
	printf( "\n\n" );

	exit( 0 );
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

int main( int argc, char * argv[] )
{
	SP_NKLog::init4test( "testcli" );
	SP_NKLog::setLogLevel( LOG_DEBUG );

	char * host = NULL, * port = NULL;
	char * dbfile = NULL, * user = NULL, * dbname = NULL, * sql = NULL;

	extern char *optarg ;
	int c ;

	while( ( c = getopt( argc, argv, "h:p:i:u:n:s:ov" ) ) != EOF ) {
		switch ( c ) {
			case 'h' : host = optarg; break;
			case 'p' : port = optarg; break;
			case 'i' : dbfile = optarg; break;
			case 'u' : user = optarg; break;
			case 'n' : dbname = optarg; break;
			case 's' : sql = optarg; break;
			case 'o' : SP_NKSocket::setLogSocketDefault( 1 ); break;
			case 'v' :
			default: showUsage( argv[ 0 ] ); break;
		}
	}

	if( NULL == host || NULL == host || NULL == dbfile
			|| NULL == user || NULL == dbname || NULL == sql ) {
		showUsage( argv[ 0 ] );
	}

	SP_NKStringList sqlList;
	{
		sql = strdup( sql );

		for( char * pos = sql; NULL != pos && '\0' != *pos; ) {
			char * next = strchr( pos, ';' );
			if( NULL != next ) *next = '\0';

			for( ; isspace( *pos ); ) pos++;

			if( '\0' != *pos ) sqlList.append( pos );

			pos = next ? next + 1 : NULL;
		}

		free( sql );
	}

	SP_NKTcpSocket socket( host, atoi( port ) );

	SP_HiveDBProtocol protocol( &socket, 0 );

	SP_HiveRespObject * resp = protocol.execute( atoi( dbfile ), user, dbname, &sqlList );

	if( NULL != resp ) {
		if( 0 == resp->getErrorCode() ) {
			printf( "result.count %d\n", resp->getResultCount() );

			for( int i = 0; i < resp->getResultCount(); i++ ) {
				SP_HiveResultSet * rs = resp->getResultSet( i );

				printResultSet( rs );

				delete rs;
			}
		} else {
			printf( "execute fail, errcode %d, %s\n",
					resp->getErrdataCode(), resp->getErrdataMsg() );
		}

		delete resp;
	} else {
		printf( "socket error\n" );
	}

	return 0;
}

