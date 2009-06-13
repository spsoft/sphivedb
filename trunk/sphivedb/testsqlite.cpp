/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <assert.h>
#include <string.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>

#include "spsqlite.h"

#include "sqlite3.h"

void getname( sqlite3 * handle, char * name, int size )
{
	char tmp[ 512 ] = { 0 };
	snprintf( tmp, sizeof( tmp ),
			"select tbl_name from sqlite_master where type='table'" );

	sqlite3_stmt * stmt = NULL;

	int dbRet = sqlite3_prepare( handle, tmp, strlen( tmp ), &stmt, NULL );

	if( SQLITE_OK == dbRet ) {
		for( ; ; ) {
			int stepRet = sqlite3_step( stmt );

			if( SQLITE_DONE != stepRet && SQLITE_ROW != stepRet ) {
				dbRet = -1;
				break;
			}

			if( SQLITE_DONE == stepRet ) break;

			const char * value = (char*)sqlite3_column_text( stmt, 0 );

			strncpy( name, value, size );

			dbRet = 0;
		}
	}

	sqlite3_finalize( stmt );
}

void testschema( sqlite3 * handle )
{
	char name[ 128 ] = { 0 };
	getname( handle, name, sizeof( name ) );

	printf( "table %s\n", name );

	int count = 0;
	count = spsqlite_table_columns_count( handle, name );
	printf( "count %d\n", count );

	spsqlite_column_t * columns = NULL;

	spsqlite_table_columns_get( handle, name, &columns );

	spsqlite_column_t * iter = columns;
	for( ; NULL != iter; iter = iter->next ) {
		printf( "%s : %s", iter->name, iter->type );
		if( iter->notNull ) {
			printf( " not null" );
		} else if( iter->defval ) {
			if( '\0' == *iter->defval ) {
				printf( " default \"\"" );
			} else {
				printf( " default %s", iter->defval );
			}
		}

		printf( "\n" );
	}

	spsqlite_table_columns_free( &columns );
}

void testsql( const char * ddl )
{
	sqlite3 * handle = NULL;
	char errcode = 0;
	char * errmsg = NULL;

	sqlite3_open( ":memory:", &handle );

	errcode = sqlite3_exec( handle, ddl, NULL, NULL, &errmsg );

	if( SQLITE_OK != errcode ) {
		printf( "sqlite3_exec %d, %s\n", errcode, errmsg ? errmsg : "NULL" );
		if( NULL != errmsg ) sqlite3_free( errmsg );
		sqlite3_close( handle );
		return;
	}

	testschema( handle );

	sqlite3_close( handle );
}

void testfile( const char * file )
{
	sqlite3 * handle = NULL;

	sqlite3_open( file, &handle );

	testschema( handle );

	sqlite3_close( handle );
}

int main( int argc, char * argv[] )
{
	const char * sql = NULL;
	const char * dbfile = NULL;

	extern char *optarg ;
	int c ;

	while( ( c = getopt ( argc, argv, "s:f:v" )) != EOF ) {
		switch ( c ) {
			case 's':
				sql = optarg;
				break;
			case 'f':
				dbfile = optarg;
				break;
			default:
				printf( "Usage: %s [-s <ddl>] [-f <dbfile>]\n", argv[0] );
				exit( -1 );
		}
	}

	if( NULL == sql && NULL == dbfile ) {
		printf( "Usage: %s [-s <ddl>] [-f <dbfile>]\n", argv[0] );
		exit( -1 );
	}

	if( NULL != sql ) testsql( sql );
	if( NULL != dbfile ) testfile( dbfile );

	return 0;
}

