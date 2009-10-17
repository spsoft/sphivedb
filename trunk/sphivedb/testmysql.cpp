/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <pthread.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <signal.h>

#include <mysql.h>

#include "spnetkit/spnkini.hpp"
#include "spnetkit/spnktime.hpp"
#include "spnetkit/spnklist.hpp"

static int gClients = 8;
static int gReadTimes = 100;
static int gWriteTimes = 100;
static const char * gConfigFile = "sphivedbcli.ini";
static int gTableKeyMax = 10;

static int gTotalReadTimes = 0;
static int gTotalWriteTimes = 0;
static int gTotalFailTimes = 0;

static pthread_mutex_t gBeginMutex = PTHREAD_MUTEX_INITIALIZER;

void showUsage( const char * program )
{
	printf( "\n" );
	printf( "Usage: %s [-f <config file>] [-c <clients>] [-r <read times>] [-w <write times>] [-o] [-v]\n", program );
	printf( "\t-f specify sphivedbcli.ini\n" );
	printf( "\t-c how many clients\n" );
	printf( "\t-r how many read actions per client\n" );
	printf( "\t-w how many write actions per client\n" );
	printf( "\t-o log socket IO\n" );
	printf( "\t-v show this usage\n" );
	printf( "\n" );

	exit ( 0 );
}

void * threadFunc( void * args )
{
	MYSQL mysql;
	mysql_init( &mysql );

	if( NULL == mysql_real_connect( &mysql, "localhost", "root", "root",
			"addrbook", 3306, NULL, 0 ) ) {
		printf( "mysql_real_connect fail\n" );
		return NULL;
	}

	// waiting for start mutex
	pthread_mutex_lock( &gBeginMutex );
	pthread_mutex_unlock( &gBeginMutex );

	int loops = gReadTimes + gWriteTimes;

	int maxUid = gTableKeyMax * 100;

	SP_NKClock clock;
	int writeTimes = 0, failTimes = 0;

	int tid = (int)pthread_self();

	for( int i = 0; i < loops; i++ ) {
		int isWrite = ( random() % loops ) >= gReadTimes;
		int uid = random() % maxUid;

		char user[ 32 ] = { 0 };
		snprintf( user, sizeof( user ), "%d", uid );

		char sql[ 1024 ] = { 0 };

		if( isWrite ) {
			//snprintf( sql, sizeof( sql ), "insert into addrbook_%d ( user, gid, addr, freq ) "
					//"values ( '%d', 0, '%d.%d.%d', 0 );", uid / 100, uid, i, tid, (int)time( NULL ) );
			snprintf( sql, sizeof( sql ), "insert into addrbook ( user, gid, addr, freq ) "
					"values ( '%d', 0, '%d.%d.%d', 0 );", uid, i, tid, (int)time( NULL ) );
			writeTimes++;
		} else {
			//snprintf( sql, sizeof( sql ), "select * from addrbook_%d where user = '%d' limit 100;",
					//uid / 100, uid );
			snprintf( sql, sizeof( sql ), "select * from addrbook where user = '%d' limit 100;", uid );
		}

		int ret = mysql_query( &mysql, sql );

		if( 0 == ret ) {
			if( isWrite ) {
				// noop
			} else {
				MYSQL_RES * res = mysql_store_result( &mysql );
				if( NULL != res ) {
					mysql_free_result( res );
				} else {
					failTimes++;
				}
			}
		} else {
			printf( "execute fail, errcode %d, %s\n", ret, mysql_error( &mysql ) );
			failTimes++;
		}
	}

	int usedTime = clock.getAge();
	int readTimes = loops - writeTimes;

	float writePerSeconds = ( writeTimes * 1000.0 ) / usedTime;
	float readPerSeconds = ( readTimes * 1000.0 ) / usedTime;

	printf( "Used Time: %d (ms), Write %d (%.2f), Read %d (%.2f), Fail %d\n",
			usedTime, writeTimes, writePerSeconds, readTimes, readPerSeconds, failTimes );

	gTotalReadTimes += readTimes;
	gTotalWriteTimes += writeTimes;
	gTotalFailTimes += failTimes;

	return NULL;
}

int main( int argc, char * argv[] )
{
	signal( SIGPIPE, SIG_IGN );

	extern char *optarg ;
	int c ;

	while( ( c = getopt ( argc, argv, "f:c:r:w:ov" ) ) != EOF ) {
		switch ( c ) {
			case 'f':
				gConfigFile = optarg;
				break;
			case 'c':
				gClients = atoi( optarg );
				break;
			case 'r':
				gReadTimes = atoi( optarg );
				break;
			case 'w':
				gWriteTimes = atoi( optarg );
				break;
			case 'o':
				break;
			case '?' :
			case 'v' :
				showUsage( argv[0] );
		}
	}

	SP_NKIniFile iniFile;
	iniFile.open( gConfigFile );

	gTableKeyMax = iniFile.getValueAsInt( "EndPointTable", "TableKeyMax" );

	printf( "TableKeyMax %d\n", gTableKeyMax );

	pthread_t * thrlist = (pthread_t*)calloc( sizeof( pthread_t ), gClients );

	pthread_mutex_lock( &gBeginMutex );

	printf( "begin to create thread ...\n" );

	for( int i = 0; i < gClients; i++ ) {
		if( 0 != pthread_create( &(thrlist[i]), NULL, threadFunc, NULL ) ) {
			printf( "pthread_create fail, index %d, errno %d, %s\n", i, errno, strerror( errno ) );
			exit( -1 );
		}
	}

	printf( "begin to execute ...\n" );

	pthread_mutex_unlock( &gBeginMutex );

	SP_NKClock clock;

	for( int i = 0; i < gClients; i++ ) {
		pthread_join( thrlist[i], NULL );
	}

	free( thrlist );

	printf( "\n" );

	int usedTime = clock.getAge() > 0 ? clock.getAge() : 1;

	float writePerSeconds = ( gTotalWriteTimes * 1000.0 ) / usedTime;
	float readPerSeconds = ( gTotalReadTimes * 1000.0 ) / usedTime;

	printf( "Total Used Time: %d (ms), Write %d (%.2f), Read %d (%.2f), Fail %d\n",
			usedTime, gTotalWriteTimes, writePerSeconds, gTotalReadTimes,
			readPerSeconds, gTotalFailTimes );

	return 0;
}

