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
#include <stdio.h>

#include "spnetkit/spnksocket.hpp"
#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkgetopt.h"
#include "spnetkit/spnklist.hpp"

#include "sphivedbcli.hpp"
#include "sphivemsg.hpp"
#include "sphivejson.hpp"

void showUsage( const char * program )
{
	printf( "\n\n%s [-h host] [-p port] [-i dbfile] [-u user] [-n dbname] [-o] [-v]\n", program );

	printf( "\t-h http host\n" );
	printf( "\t-p http port\n" );
	printf( "\t-i dbfile\n" );
	printf( "\t-u user\n" );
	printf( "\t-n dbname\n" );
	printf( "\t-o log socket io\n" );
	printf( "\t-v show this usage\n" );
	printf( "\n\n" );

	exit( 0 );
}

int main( int argc, char * argv[] )
{
	SP_NKLog::init4test( "testremove" );
	SP_NKLog::setLogLevel( LOG_DEBUG );

	char * host = NULL, * port = NULL;
	char * dbfile = NULL, * user = NULL, * dbname = NULL, * sql = NULL;

	extern char *optarg ;
	int c ;

	while( ( c = getopt( argc, argv, "h:p:i:u:n:ov" ) ) != EOF ) {
		switch ( c ) {
			case 'h' : host = optarg; break;
			case 'p' : port = optarg; break;
			case 'i' : dbfile = optarg; break;
			case 'u' : user = optarg; break;
			case 'n' : dbname = optarg; break;
			case 'o' : SP_NKSocket::setLogSocketDefault( 1 ); break;
			case 'v' :
			default: showUsage( argv[ 0 ] ); break;
		}
	}

	if( NULL == host || NULL == host || NULL == dbfile
			|| NULL == user || NULL == dbname ) {
		showUsage( argv[ 0 ] );
	}

	SP_NKTcpSocket socket( host, atoi( port ) );

	SP_HiveDBProtocol protocol( &socket, 0, SP_HiveDBClientConfig::eProtoBufRpc );

	int result = -1;
	int ret = protocol.remove( atoi( dbfile ), user, dbname, &result );

	printf( "remove ret %d, result %d\n", ret, result );

	return 0;
}

