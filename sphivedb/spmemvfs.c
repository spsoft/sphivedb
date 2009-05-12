/*
 * Copyright 2009 Stephen Liu
 *
 */

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <search.h>
#include <pthread.h>

#include "spmemvfs.h"

#include "sqlite3.h"

/* Useful macros used in several places */
#define SPMEMVFS_MIN(x,y) ((x)<(y)?(x):(y))
#define SPMEMVFS_MAX(x,y) ((x)>(y)?(x):(y))

static void spmemvfsDebug(const char *format, ...){

#if defined(SPMEMVFS_DEBUG)

	char logTemp[ 1024 ] = { 0 };

	va_list vaList;
	va_start( vaList, format );
	vsnprintf( logTemp, sizeof( logTemp ), format, vaList );
	va_end ( vaList );

	if( strchr( logTemp, '\n' ) ) {
		printf( "%s", logTemp );
	} else {
		printf( "%s\n", logTemp );
	}
#endif

}

//===========================================================================

typedef struct spmemfile_t spmemfile_t;

struct spmemfile_t {
	sqlite3_file base;
	spmemvfs_cb_t cb;
	char * path;
	int flags;
	int total;
	int len;
	char * buffer;
};

static int spmemfileClose( sqlite3_file * file );
static int spmemfileRead( sqlite3_file * file, void * buffer, int len, sqlite3_int64 offset );
static int spmemfileWrite( sqlite3_file * file, const void * buffer, int len, sqlite3_int64 offset );
static int spmemfileTruncate( sqlite3_file * file, sqlite3_int64 size );
static int spmemfileSync( sqlite3_file * file, int flags );
static int spmemfileFileSize( sqlite3_file * file, sqlite3_int64 * size );
static int spmemfileLock( sqlite3_file * file, int type );
static int spmemfileUnlock( sqlite3_file * file, int type );
static int spmemfileCheckReservedLock( sqlite3_file * file, int * result );
static int spmemfileFileControl( sqlite3_file * file, int op, void * arg );
static int spmemfileSectorSize( sqlite3_file * file );
static int spmemfileDeviceCharacteristics( sqlite3_file * file );

static sqlite3_io_methods g_spmemfile_io_memthods = {
	1,                                  /* iVersion */
	spmemfileClose,                     /* xClose */
	spmemfileRead,                      /* xRead */
	spmemfileWrite,                     /* xWrite */
	spmemfileTruncate,                  /* xTruncate */
	spmemfileSync,                      /* xSync */
	spmemfileFileSize,                  /* xFileSize */
	spmemfileLock,                      /* xLock */
	spmemfileUnlock,                    /* xUnlock */
	spmemfileCheckReservedLock,         /* xCheckReservedLock */
	spmemfileFileControl,               /* xFileControl */
	spmemfileSectorSize,                /* xSectorSize */
	spmemfileDeviceCharacteristics      /* xDeviceCharacteristics */
};

int spmemfileClose( sqlite3_file * file )
{
	spmemfile_t * memfile = (spmemfile_t*)file;

	spmemvfsDebug( "call %s( %p )", __func__, memfile );

	if( SQLITE_OPEN_MAIN_DB & memfile->flags ) {
		memfile->cb.save( memfile->cb.arg, memfile->path, memfile->buffer, memfile->len );
		memfile->buffer = NULL;
	}

	if( NULL != memfile->buffer ) free( memfile->buffer );
	free( memfile->path );

	return SQLITE_OK;
}

int spmemfileRead( sqlite3_file * file, void * buffer, int len, sqlite3_int64 offset )
{
	spmemfile_t * memfile = (spmemfile_t*)file;

	spmemvfsDebug( "call %s( %p, ..., %d, %lld ), len %d",
		__func__, memfile, len, offset, memfile->len );

	if( ( offset + len ) > memfile->len ) {
		return SQLITE_IOERR_SHORT_READ;
	}

	memcpy( buffer, memfile->buffer + offset, len );

	return SQLITE_OK;
}

int spmemfileWrite( sqlite3_file * file, const void * buffer, int len, sqlite3_int64 offset )
{
	spmemfile_t * memfile = (spmemfile_t*)file;

	spmemvfsDebug( "call %s( %p, ..., %d, %lld ), len %d",
		__func__, memfile, len, offset, memfile->len );

	if( ( offset + len ) > memfile->total ) {
		int newTotal = 2 * ( offset + len + memfile->total );
		char * newBuffer = (char*)realloc( memfile->buffer, newTotal );
		if( NULL == newBuffer ) {
			return SQLITE_NOMEM;
		}

		memfile->total = newTotal;
		memfile->buffer = newBuffer;
	}

	memcpy( memfile->buffer + offset, buffer, len );

	memfile->len = SPMEMVFS_MAX( memfile->len, offset + len );

	return SQLITE_OK;
}

int spmemfileTruncate( sqlite3_file * file, sqlite3_int64 size )
{
	spmemfile_t * memfile = (spmemfile_t*)file;

	spmemvfsDebug( "call %s( %p )", __func__, memfile );

	memfile->len = SPMEMVFS_MIN( memfile->len, size );

	return SQLITE_OK;
}

int spmemfileSync( sqlite3_file * file, int flags )
{
	spmemvfsDebug( "call %s( %p )", __func__, file );

	return SQLITE_OK;
}

int spmemfileFileSize( sqlite3_file * file, sqlite3_int64 * size )
{
	spmemfile_t * memfile = (spmemfile_t*)file;

	spmemvfsDebug( "call %s( %p )", __func__, memfile );

	* size = memfile->len;

	return SQLITE_OK;
}

int spmemfileLock( sqlite3_file * file, int type )
{
	spmemvfsDebug( "call %s( %p )", __func__, file );

	return SQLITE_OK;
}

int spmemfileUnlock( sqlite3_file * file, int type )
{
	spmemvfsDebug( "call %s( %p )", __func__, file );

	return SQLITE_OK;
}

int spmemfileCheckReservedLock( sqlite3_file * file, int * result )
{
	spmemvfsDebug( "call %s( %p )", __func__, file );

	*result = 0;

	return SQLITE_OK;
}

int spmemfileFileControl( sqlite3_file * file, int op, void * arg )
{
	spmemvfsDebug( "call %s( %p )", __func__, file );

	return SQLITE_OK;
}

int spmemfileSectorSize( sqlite3_file * file )
{
	spmemvfsDebug( "call %s( %p )", __func__, file );

	return 0;
}

int spmemfileDeviceCharacteristics( sqlite3_file * file )
{
	spmemvfsDebug( "call %s( %p )", __func__, file );

	return 0;
}

//===========================================================================

typedef struct spmemvfs_t spmemvfs_t;
struct spmemvfs_t {
	sqlite3_vfs base;
	spmemvfs_cb_t cb;
	sqlite3_vfs * parent;
};

static int spmemvfsOpen( sqlite3_vfs * vfs, const char * path, sqlite3_file * file, int flags, int * outflags );
static int spmemvfsDelete( sqlite3_vfs * vfs, const char * path, int syncDir );
static int spmemvfsAccess( sqlite3_vfs * vfs, const char * path, int flags, int * result );
static int spmemvfsFullPathname( sqlite3_vfs * vfs, const char * path, int len, char * fullpath );
static void * spmemvfsDlOpen( sqlite3_vfs * vfs, const char * path );
static void spmemvfsDlError( sqlite3_vfs * vfs, int len, char * errmsg );
static void ( * spmemvfsDlSym ( sqlite3_vfs * vfs, void * handle, const char * symbol ) ) ( void );
static void spmemvfsDlClose( sqlite3_vfs * vfs, void * handle );
static int spmemvfsRandomness( sqlite3_vfs * vfs, int len, char * buffer );
static int spmemvfsSleep( sqlite3_vfs * vfs, int microseconds );
static int spmemvfsCurrentTime( sqlite3_vfs * vfs, double * result );

static spmemvfs_t g_spmemvfs = {
	{
		1,                                           /* iVersion */
		0,                                           /* szOsFile */
		0,                                           /* mxPathname */
		0,                                           /* pNext */
		SPMEMVFS_NAME,                               /* zName */
		0,                                           /* pAppData */
		spmemvfsOpen,                                /* xOpen */
		spmemvfsDelete,                              /* xDelete */
		spmemvfsAccess,                              /* xAccess */
		spmemvfsFullPathname,                        /* xFullPathname */
		spmemvfsDlOpen,                              /* xDlOpen */
		spmemvfsDlError,                             /* xDlError */
		spmemvfsDlSym,                               /* xDlSym */
		spmemvfsDlClose,                             /* xDlClose */
		spmemvfsRandomness,                          /* xRandomness */
		spmemvfsSleep,                               /* xSleep */
		spmemvfsCurrentTime                          /* xCurrentTime */
	}, 
	{ 0 },
	0                                                /* pParent */
};

int spmemvfsOpen( sqlite3_vfs * vfs, const char * path, sqlite3_file * file, int flags, int * outflags )
{
	spmemvfs_t * memvfs = (spmemvfs_t*)vfs;
	spmemfile_t * memfile = (spmemfile_t*)file;

	spmemvfsDebug( "call %s( %p(%p), %s, %p, %x, %p )\n",
			__func__, vfs, &g_spmemvfs, path, file, flags, outflags );

	memset( memfile, 0, sizeof( spmemfile_t ) );
	memfile->base.pMethods = &g_spmemfile_io_memthods;
	memfile->flags = flags;

	memfile->cb = memvfs->cb;
	memfile->path = strdup( path );

	if( SQLITE_OPEN_MAIN_DB & memfile->flags ) {
		memfile->buffer = memvfs->cb.load( memvfs->cb.arg, path, &memfile->len );
		memfile->total = memfile->len;
	}

	return SQLITE_OK;
}

int spmemvfsDelete( sqlite3_vfs * vfs, const char * path, int syncDir )
{
	spmemvfsDebug( "call %s( %p(%p), %s, %d )\n",
			__func__, vfs, &g_spmemvfs, path, syncDir );

	return SQLITE_OK;
}

int spmemvfsAccess( sqlite3_vfs * vfs, const char * path, int flags, int * result )
{
	return SQLITE_OK;
}

int spmemvfsFullPathname( sqlite3_vfs * vfs, const char * path, int len, char * fullpath )
{
	strncpy( fullpath, path, len );
	fullpath[ len - 1 ] = '\0';

	return SQLITE_OK;
}

void * spmemvfsDlOpen( sqlite3_vfs * vfs, const char * path )
{
	return NULL;
}

void spmemvfsDlError( sqlite3_vfs * vfs, int len, char * errmsg )
{
	// noop
}

void ( * spmemvfsDlSym ( sqlite3_vfs * vfs, void * handle, const char * symbol ) ) ( void )
{
	return NULL;
}

void spmemvfsDlClose( sqlite3_vfs * vfs, void * handle )
{
	// noop
}

int spmemvfsRandomness( sqlite3_vfs * vfs, int len, char * buffer )
{
	return SQLITE_OK;
}

int spmemvfsSleep( sqlite3_vfs * vfs, int microseconds )
{
	return SQLITE_OK;
}

int spmemvfsCurrentTime( sqlite3_vfs * vfs, double * result )
{
	return SQLITE_OK;
}

//===========================================================================

int spmemvfs_init( spmemvfs_cb_t * cb )
{
	sqlite3_vfs * parent = NULL;

	if( g_spmemvfs.parent ) return SQLITE_OK;

	parent = sqlite3_vfs_find( 0 );

	g_spmemvfs.parent = parent;

	g_spmemvfs.base.mxPathname = parent->mxPathname;
	g_spmemvfs.base.szOsFile = sizeof( spmemfile_t );

	g_spmemvfs.cb = * cb;

	return sqlite3_vfs_register( (sqlite3_vfs*)&g_spmemvfs, 0 );
}

//===========================================================================

/* base on tsearch to implement a membuffer map */

struct spmembuffer_map_t {
	void * root;
	pthread_mutex_t mutex;
};

typedef struct spmembuffer_t spmembuffer_t;

struct spmembuffer_t {
	char * key;
	void * buffer;
	int len;
};

static int spmembuffer_cmp( const void * item1, const void * item2 )
{
	spmembuffer_t * b1 = (spmembuffer_t*)item1;
	spmembuffer_t * b2 = (spmembuffer_t*)item2;

	return strcmp( b1->key, b2->key );
}

static void spmembuffer_free( void * item )
{
	spmembuffer_t * b = (spmembuffer_t*)item;

	free( b->key );
	free( b->buffer );
	free( b );
}

static void spmembuffer_free_action( const void * nodep, const VISIT which, const int depth )
{
	spmembuffer_t * b = *(spmembuffer_t**)nodep;

	switch( which ) {
		case preorder:
			break;
		case endorder:
			break;
		case postorder:
		case leaf:
			spmembuffer_free( b );
			free( (void*)nodep );
			break;
	}
}

void spmembuffer_map_del( spmembuffer_map_t * themap )
{

#ifdef __USE_GNU
	tdestroy( themap->root, spmembuffer_free );
#else
	twalk( themap->root, spmembuffer_free_action );
#endif

	pthread_mutex_destroy( &themap->mutex );

	free( themap );
}

spmembuffer_map_t * spmembuffer_map_new()
{
	spmembuffer_map_t * themap = (spmembuffer_map_t*)calloc( sizeof( spmembuffer_map_t ), 1 );

	pthread_mutex_init( &themap->mutex, NULL );

	return themap;
}

int spmembuffer_map_put( spmembuffer_map_t * themap, const char * key, void * buffer, int len )
{
	int ret = 0;

	spmembuffer_t * olditem = NULL;
	spmembuffer_t * item = (spmembuffer_t*)calloc( sizeof( spmembuffer_t ), 1 );

	item->key = strdup( key );
	item->buffer = buffer;
	item->len = len;

	pthread_mutex_lock( &themap->mutex );

	olditem = *(spmembuffer_t**)tsearch( item, &themap->root, spmembuffer_cmp );

	if( olditem != item ) {
		/* update olditem */

		free( olditem->buffer );

		olditem->buffer = buffer;
		olditem->len = len;

		free( item->key );
		free( item );

		ret = 1;
	}

	pthread_mutex_unlock( &themap->mutex );

	return ret;
}

void * spmembuffer_map_take( spmembuffer_map_t * themap, const char * key, int * len )
{
	void * buffer = NULL, * tmp = NULL;
	spmembuffer_t keyitem, * olditem = NULL;

	keyitem.key = (char*)key;
	*len = 0;

	pthread_mutex_lock( &themap->mutex );

	tmp = tfind( &keyitem, &themap->root, spmembuffer_cmp );

	if( NULL != tmp ) {
		olditem = *(spmembuffer_t**)tmp;

		tdelete( &keyitem, &themap->root, spmembuffer_cmp );

		if( NULL != olditem ) {
			buffer = olditem->buffer;
			*len = olditem->len;

			free( olditem->key );
			free( olditem );
		}
	}

	pthread_mutex_unlock( &themap->mutex );

	return buffer;
}

int spmembuffer_map_has( spmembuffer_map_t * themap, const char * key )
{
	int ret = 0;
	spmembuffer_t keyitem;

	keyitem.key = (char*)key;

	pthread_mutex_lock( &themap->mutex );

	ret = ( NULL != tfind( &keyitem, &themap->root, spmembuffer_cmp ) );

	pthread_mutex_unlock( &themap->mutex );

	return ret;
}

