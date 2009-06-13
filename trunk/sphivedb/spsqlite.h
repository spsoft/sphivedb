/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __spsqlite_h__
#define __spsqlite_h__

#ifdef __cplusplus
extern "C" {
#endif

#if 0

struct Column {
  char *zName;     /* Name of this column */
  Expr *pDflt;     /* Default value of this column */ 
  char *zType;     /* Data type for this column */
  char *zColl;     /* Collating sequence.  If NULL, use the default */
  u8 notNull;      /* True if there is a NOT NULL constraint */
  u8 isPrimKey;    /* True if this column is part of the PRIMARY KEY */
  char affinity;   /* One of the SQLITE_AFF_... values */
#ifndef SQLITE_OMIT_VIRTUALTABLE
  u8 isHidden;     /* True if this column is 'hidden' */
#endif
};

#endif

/* currently we need (name/type) only */
typedef struct spsqlite_column_t {
	char * name;
	char * type;
	char * defval;
	char notNull;
	struct spsqlite_column_t * next;
} spsqlite_column_t;

extern int spsqlite_table_columns_count( void * handle, const char * name );

extern int spsqlite_table_columns_get( void * handle,
		const char * name, spsqlite_column_t ** columns );

extern int spsqlite_table_columns_free( spsqlite_column_t ** columns );

#ifdef __cplusplus
}
#endif

#endif

