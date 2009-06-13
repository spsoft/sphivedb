/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include "spsqlite.h"

#include "sqlite3.c"

int spsqlite_table_columns_count( void * handle, const char * name )
{
	int count = -1;

	sqlite3 * db = (sqlite3*)handle;

	int ret = sqlite3Init( db, NULL );

	if( SQLITE_OK == ret ) {
		Table * table = sqlite3FindTable( db, name, NULL );

		if( NULL == table ) {
			count = 0;
		} else {
			count = table->nCol;
		}
	}

	return count;
}

int spsqlite_table_columns_get( void * handle,
		const char * name, spsqlite_column_t ** columns )
{
	int i = 0, ret = -1;

	sqlite3_value * defval = NULL;
	Table * table = NULL;

	sqlite3 * db = (sqlite3*)handle;

	ret = sqlite3Init( db, NULL );

	if( SQLITE_OK != ret ) return -1;

	table = sqlite3FindTable( db, name, NULL );

	if( NULL == table ) return -1;

	for( i = 0; i < table->nCol; i++ ) {
		Column * column = table->aCol + i;

		spsqlite_column_t * iter = (spsqlite_column_t*)calloc( sizeof( spsqlite_column_t ), 1 );
		iter->name = strdup( column->zName );
		iter->type = strdup( column->zType );
		iter->notNull = column->notNull;

		if( NULL != column->pDflt ) {
			sqlite3ValueFromExpr( db, column->pDflt, SQLITE_UTF8, SQLITE_AFF_NONE, &defval );
			if( NULL != defval ) {
				iter->defval = strdup( sqlite3_value_text( defval ) );
				sqlite3ValueFree( defval );
			}
		}

		*columns = iter;
		columns = &( iter->next );
	}

	return 0;
}

int spsqlite_table_columns_free( spsqlite_column_t ** columns )
{
	spsqlite_column_t * iter = NULL, * next = NULL;

	if( NULL == columns ) return 0;

	for( iter = *columns; NULL != iter; ) {
		next = iter->next;

		free( iter->name );
		free( iter->type );

		if( iter->defval ) free( iter->defval );

		free( iter );

		iter = next;
	}

	*columns = NULL;

	return 0;
}


