/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <stdio.h>
#include <stdlib.h>

#include "sphiveconfig.hpp"

int main( int argc, char * argv[] )
{
	if( argc < 2 ) {
		printf( "Usage: %s <ddl>\n", argv[0] );
		exit( -1 );
	}

	const char * sql = argv[1];

	int ret = SP_HiveDDLConfig::computeColumnCount( sql );

	printf( "column.count %d\n", ret );

	SP_HiveDDLConfig ddlConfig;

	ret = ddlConfig.init( "test", sql );

	printf( "init %d\n", ret );

	if( 0 == ret ) {
		printf( "name %s, table %s, column.count %d\n",
				ddlConfig.getName(), ddlConfig.getTable(),
				ddlConfig.getColumnCount() );

		for( int i = 0; i < ddlConfig.getColumnCount(); i++ ) {
			printf( "%s -> %s\n", ddlConfig.getColumnName( i ), ddlConfig.getColumnType( i ) );
		}
	}

	return 0;
}

