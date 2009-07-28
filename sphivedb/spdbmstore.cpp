/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>

#include "spdbmstore.hpp"
#include "sphivemsg.hpp"
#include "sphiveconfig.hpp"

#include "spmemvfs.h"

#include "spcabinet.h"

#include "spnetkit/spnklist.hpp"
#include "spnetkit/spnklock.hpp"
#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"

const char * SP_DbmStore :: getPath()
{
	return mPath;
}

void * SP_DbmStore :: getDbm()
{
	return mDbm;
}

SP_DbmStore :: SP_DbmStore( const char * path, void * dbm )
{
	mPath = strdup( path );
	mDbm = dbm;

	mRefCount = 0;
}

SP_DbmStore :: ~SP_DbmStore()
{
	if( NULL != mPath ) free( mPath );
	mPath = NULL;

	if( NULL != mDbm ) {
		sp_tcadbclose( mDbm );
		sp_tcadbdel( mDbm );
	}
	mDbm = NULL;
}

void SP_DbmStore :: addRef()
{
	mRefCount ++;
}

int SP_DbmStore :: getRef()
{
	return mRefCount;
}

void SP_DbmStore :: release()
{
	mRefCount --; 
}

//====================================================================

int SP_DbmStoreCache :: pathcmp( const void * item1, const void * item2 )
{
	SP_NKDoubleLinkNode_t * n1 = (SP_NKDoubleLinkNode_t*)item1;
	SP_NKDoubleLinkNode_t * n2 = (SP_NKDoubleLinkNode_t*)item2;

	SP_DbmStore * file1 = (SP_DbmStore*)n1->mData;
	SP_DbmStore * file2 = (SP_DbmStore*)n2->mData;

	return strcmp( file1->getPath(), file2->getPath() );
}

SP_DbmStoreCache :: SP_DbmStoreCache( int maxCount )
{
	mMaxCount = maxCount;
	mLockManager = new SP_NKTokenLockManager();
	mItemList = new SP_NKSortedArray( pathcmp );
	mTimeList = new SP_NKDoubleLinkList();

	pthread_mutex_init( &mMutex, NULL );
}

SP_DbmStoreCache :: ~SP_DbmStoreCache()
{
	delete mLockManager, mLockManager = NULL;

	for( ; mItemList->getCount() > 0; ) {
		pthread_mutex_lock( &mMutex );

		for( int i = mItemList->getCount() - 1; i >= 0; i-- ) {
			SP_NKDoubleLinkNode_t * item = (SP_NKDoubleLinkNode_t*)mItemList->getItem( i );

			SP_DbmStore * file = (SP_DbmStore*)item->mData;

			if( file->getRef() <= 0 ) {
				mItemList->takeItem( i );
				mTimeList->remove( item );

				delete file;

				free( item );
			}
		}

		pthread_mutex_unlock( &mMutex );
	}

	pthread_mutex_destroy( &mMutex );

	delete mItemList, mItemList = NULL;

	delete mTimeList, mTimeList = NULL;
}

SP_DbmStore * SP_DbmStoreCache :: get( const char * path )
{
	SP_DbmStore * ret = NULL;

	mLockManager->lock( path, -1 );
	{
		pthread_mutex_lock( &mMutex );
		{
			SP_NKDoubleLinkNode_t key;
			key.mData = new SP_DbmStore( path, NULL );

			int index = mItemList->find( &key );

			delete (SP_DbmStore*)key.mData;

			if( index >= 0 ) {
				SP_NKDoubleLinkNode_t * node = (SP_NKDoubleLinkNode_t*)mItemList->getItem( index );
				ret = (SP_DbmStore*)node->mData;
				ret->addRef();

				mTimeList->remove( node );
				mTimeList->append( node );
			}
		}
		pthread_mutex_unlock( &mMutex );

		if( NULL == ret ) {
			char realpath[ 256 ] = { 0 };
			snprintf( realpath, sizeof( realpath ), "%s#xmsiz=0", path );

			void * adb = sp_tcadbnew();
			if( sp_tcadbopen( adb, realpath ) ) {
				ret = new SP_DbmStore( path, adb );
			} else {
				sp_tcadbdel( adb );
				SP_NKLog::log( LOG_ERR, "ERROR: sp_tcadbopen fail" );
			}

			if( NULL != ret ) {
				SP_NKDoubleLinkNode_t * node = SP_NKDoubleLinkList::newNode();
				node->mData = ret;
				ret->addRef();

				pthread_mutex_lock( &mMutex );
				{
					void * old = NULL;
					mItemList->insert( node, &old );
					mTimeList->append( node );

					assert( NULL == old );
				}
				pthread_mutex_unlock( &mMutex );
			}
		}
	}
	mLockManager->unlock( path );

	return ret;
}

int SP_DbmStoreCache :: save( SP_DbmStore * file )
{
	int ret = 0;

	pthread_mutex_lock( &mMutex );
	{
		file->release();

		SP_NKDoubleLinkNode_t key;
		key.mData = file;

		int index = mItemList->find( &key );

		assert( index >= 0 );

		if( mItemList->getCount() > mMaxCount ) {
			SP_NKVector toDeleteList;

			SP_NKDoubleLinkNode_t * node = (SP_NKDoubleLinkNode_t*)mTimeList->getHead();

			for( ; NULL != node; node = node->mNext ) {
				SP_DbmStore * file = (SP_DbmStore*)node->mData;

				if( file->getRef() <= 0 ) {
					toDeleteList.append( node );
				}

				if( mItemList->getCount() - toDeleteList.getCount() <= mMaxCount ) {
					break;
				}
			}

			for( int i = 0; i < toDeleteList.getCount(); i++ ) {
				node = (SP_NKDoubleLinkNode_t*)toDeleteList.getItem( i );

				index = mItemList->find( node );

				assert( index >= 0 );

				mItemList->takeItem( index );
				mTimeList->remove( node );

				delete (SP_DbmStore*)node->mData;
				free( node );
			}
		}
	}
	pthread_mutex_unlock( &mMutex );

	return ret;
}

//====================================================================

SP_DbmStoreSource :: SP_DbmStoreSource()
{
	mCache = NULL;
	mConfig = NULL;
}

SP_DbmStoreSource :: ~SP_DbmStoreSource()
{
	if( mCache ) delete mCache, mCache = NULL;
}

int SP_DbmStoreSource :: init( SP_HiveConfig * config )
{
	mConfig = config;

	mCache = new SP_DbmStoreCache( config->getMaxOpenFiles() );

	return 0;
}

const char * SP_DbmStoreSource :: getPath( int dbfile,
		const char * dbname, char * path, int size )
{
	snprintf( path, size, "%s/%d/%s.%d.tch", mConfig->getDataDir(),
			dbfile / 100, dbname, dbfile );

	return path;
}

int SP_DbmStoreSource :: loadFromDbm( void * dbm, SP_HiveReqObject * req,
		spmemvfs_db_t * db )
{
	spmembuffer_t * mem = (spmembuffer_t*)calloc( sizeof( spmembuffer_t ), 1 );

	int vlen = 0;
	void * vbuf = sp_tcadbget( dbm, req->getUser(), strlen( req->getUser() ), &vlen );

	if( NULL != vbuf ) {
		mem->data = (char*)vbuf;
		mem->used = mem->total = vlen;
	}

	if( 0 != spmemvfs_open_db( db, req->getUniqKey(), mem ) ) {
		SP_NKLog::log( LOG_ERR, "ERROR: cannot open db, %s", req->getUniqKey() );
		return -1;
	}

	return 0;
}

SP_HiveStore * SP_DbmStoreSource :: load( SP_HiveReqObject * req )
{
	SP_HiveStore * store = NULL;

	char path[ 256 ] = { 0 };
	getPath( req->getDBFile(), req->getDBName(), path, sizeof( path ) );

	SP_DbmStore * dbmStore = mCache->get( path );

	if( NULL != dbmStore ) {
		spmemvfs_db_t * db = (spmemvfs_db_t*)calloc( sizeof( spmemvfs_db_t ), 1 );

		int ret = loadFromDbm( dbmStore->getDbm(), req, db );

		if( 0 == ret ) {
			store = new SP_HiveStore();
			store->setArgs( db );
			store->setHandle( db->handle );
		} else {
			spmemvfs_close_db( db );
		}

		mCache->save( dbmStore );
	}

	return store;
}

int SP_DbmStoreSource :: save( SP_HiveReqObject * req, SP_HiveStore * store )
{
	int ret = -1;

	char path[ 256 ] = { 0 };
	getPath( req->getDBFile(), req->getDBName(), path, sizeof( path ) );

	SP_DbmStore * dbmStore = mCache->get( path );

	if( NULL != dbmStore ) {

		spmemvfs_db_t * db = (spmemvfs_db_t*)store->getArgs();

		if( NULL != db->mem->data ) {
			if( sp_tcadbput( dbmStore->getDbm(), req->getUser(),
					strlen( req->getUser() ), db->mem->data, db->mem->used ) ) {
				ret = 0;
			} else {
				SP_NKLog::log( LOG_ERR, "ERROR: tcadbput %s fail", req->getUser() );
			}
		}

		mCache->save( dbmStore );
	}

	return ret;
}

int SP_DbmStoreSource :: close( SP_HiveStore * store )
{
	spmemvfs_db_t * db = (spmemvfs_db_t*)store->getArgs();

	spmemvfs_close_db( db );
	free( db );

	store->setArgs( NULL );
	store->setHandle( NULL );

	return 0;
}

int SP_DbmStoreSource :: remove( SP_HiveReqObject * req )
{
	int ret = -1;

	char path[ 256 ] = { 0 };
	getPath( req->getDBFile(), req->getDBName(), path, sizeof( path ) );

	SP_DbmStore * dbmStore = mCache->get( path );

	if( NULL != dbmStore ) {
		if( sp_tcadbout( dbmStore->getDbm(), req->getUser(), strlen( req->getUser() ) ) ) {
			ret = 0;
		} else {
			ret = 1;
			SP_NKLog::log( LOG_ERR, "ERROR: tcadbout %s fail, no record", req->getUser() );
		}
	}

	return ret;
}

