/*
 * Copyright 2009 Stephen Liu
 *
 */

#ifndef __spmemvfs_h__
#define __spmemvfs_h__

#ifdef __cplusplus
extern "C" {
#endif

#define SPMEMVFS_NAME "spmemvfs"

typedef struct spmemvfs_cb_t spmemvfs_cb_t;

struct spmemvfs_cb_t {
	/* arg -- the first argument of load/save */
	void * arg;

	/* load buffer for db, return the full buffer */
	void * ( * load ) ( void * arg, const char * path, int * len );

	/* save buffer for db */
	int ( * save ) ( void * arg, const char * path, char * buffer, int len );
};

int spmemvfs_init( spmemvfs_cb_t * cb );

//===========================================================================

typedef struct spmembuffer_map_t spmembuffer_map_t;

spmembuffer_map_t * spmembuffer_map_new();

void spmembuffer_map_del( spmembuffer_map_t * themap );

/* @return 0 : insert, 1 : update */
int spmembuffer_map_put( spmembuffer_map_t * themap, const char * key, void * buffer, int len );

void * spmembuffer_map_take( spmembuffer_map_t * themap, const char * key, int * len );

int spmembuffer_map_has( spmembuffer_map_t * themap, const char * key );

#ifdef __cplusplus
}
#endif

#endif

