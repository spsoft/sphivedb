/*
 * Copyright 2009 Stephen Liu
 *
 */

#include <assert.h>
#include <string.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

#include "spmemvfs.h"

#include "sqlite3.h"

void test( const char * path )
{
	sqlite3 * dbHandle = NULL;
	char errcode = 0;
	int i = 0;
	int count = 0;
	sqlite3_stmt * stmt = NULL;
	const char * sql = NULL;

	errcode = sqlite3_open_v2( path, &dbHandle,
		SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, SPMEMVFS_NAME );

	printf( "sqlite3_open_v2 %d\n", errcode );

	errcode = sqlite3_exec( dbHandle,
		"CREATE TABLE user ( name, age )", NULL, NULL, NULL );

	printf( "sqlite3_exec %d\n", errcode );

	sql = "insert into user values ( 'abc', 12 );";
	errcode = sqlite3_exec( dbHandle, sql, 0, 0, 0 );

	count = sqlite3_changes( dbHandle );
	printf( "sqlite3_changes %d\n", count );

	sql = "select * from user;";
	errcode = sqlite3_prepare( dbHandle, sql, strlen( sql ), &stmt, NULL );

	printf( "sqlite3_prepare %d, stmt %p\n", errcode, stmt );

	count = sqlite3_column_count( stmt );

	printf( "column.count %d\n", count );

	for( i = 0; i < count; i++ ) {
		const char * name = sqlite3_column_name( stmt, i );
		printf( "\t%s", name );
	}

	printf( "\n" );

	for( ; ; ) {
		errcode = sqlite3_step( stmt );

		if( SQLITE_ROW != errcode ) break;

		for( i = 0; i < count; i++ ) {
			unsigned const char * value = sqlite3_column_text( stmt, i );

			printf( "\t%s", value );
		}

		printf( "\n" );
	}

	errcode = sqlite3_finalize( stmt );

	errcode = sqlite3_close( dbHandle );
}

static void * load( void * arg, const char * path, int * len )
{
	spmembuffer_map_t * themap = (spmembuffer_map_t*)arg;

	char * ret = spmembuffer_map_take( themap, path, len );

	return ret;
}

static int save( void * arg, const char * path, char * buffer, int len )
{
	spmembuffer_map_t * themap = (spmembuffer_map_t*)arg;

	return spmembuffer_map_put( themap, path, buffer, len );
}

int main( int argc, char * argv[] )
{
	spmembuffer_map_t * themap = spmembuffer_map_new();

	spmemvfs_cb_t cb = { themap, load, save };

	const char * path = "abc.db";

	spmemvfs_init( &cb );

	// load membuffer from file, put it into themap
	{
		char * buffer = NULL;
		int len = 0;

		FILE * fp = fopen( path, "r" );
		if( NULL != fp ) {
			struct stat filestat;
			if( 0 == stat( path, &filestat ) ) {
				len = filestat.st_size;
				buffer = (char*)malloc( filestat.st_size + 1 );
				fread( buffer, filestat.st_size, 1, fp );
				buffer[ filestat.st_size ] = '\0';
			} else {
				printf( "cannot stat file %s\n", path );
			}
			fclose( fp );
		} else {
			printf( "cannot open file %s\n", path );
		}

		if( NULL != buffer ) {
			spmembuffer_map_put( themap, path, buffer, len );
		}
	}

	test( path );

	// get membuffer from themap, save it to file
	{
		int len = 0;
		char * buffer = (char*)spmembuffer_map_take( themap, path, &len );

		if( NULL != buffer ) {
			FILE * fp = fopen( path, "w" );
			if( NULL != fp ) {
				fwrite( buffer, len, 1, fp );
				fclose( fp );
			}

			free( buffer );
		}
	}

	spmembuffer_map_del( themap );

	return 0;
}

