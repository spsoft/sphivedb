/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include "sphivefile.hpp"

#include "spcabinet.h"

#include "spnetkit/spnklist.hpp"
#include "spnetkit/spnklock.hpp"
#include "spnetkit/spnklog.hpp"
#include "spnetkit/spnkporting.hpp"

const char * SP_HiveFile :: getPath()
{
	return mPath;
}

void * SP_HiveFile :: getDbm()
{
	return mDbm;
}

SP_HiveFile :: SP_HiveFile( const char * path, void * dbm )
{
	mPath = strdup( path );
	mDbm = dbm;

	mRefCount = 0;
}

SP_HiveFile :: ~SP_HiveFile()
{
	if( NULL != mPath ) free( mPath );
	mPath = NULL;

	if( NULL != mDbm ) {
		sp_tcadbclose( mDbm );
		sp_tcadbdel( mDbm );
	}
	mDbm = NULL;
}

void SP_HiveFile :: addRef()
{
	mRefCount ++;
}

int SP_HiveFile :: getRef()
{
	return mRefCount;
}

void SP_HiveFile :: release()
{
	mRefCount --; 
}

//====================================================================

int SP_HiveFileCache :: pathcmp( const void * item1, const void * item2 )
{
	SP_NKDoubleLinkNode_t * n1 = (SP_NKDoubleLinkNode_t*)item1;
	SP_NKDoubleLinkNode_t * n2 = (SP_NKDoubleLinkNode_t*)item2;

	SP_HiveFile * file1 = (SP_HiveFile*)n1->mData;
	SP_HiveFile * file2 = (SP_HiveFile*)n2->mData;

	return strcmp( file1->getPath(), file2->getPath() );
}

SP_HiveFileCache :: SP_HiveFileCache( int maxCount )
{
	mMaxCount = maxCount;
	mLockManager = new SP_NKTokenLockManager();
	mItemList = new SP_NKSortedArray( pathcmp );
	mTimeList = new SP_NKDoubleLinkList();

	pthread_mutex_init( &mMutex, NULL );
}

SP_HiveFileCache :: ~SP_HiveFileCache()
{
	delete mLockManager, mLockManager = NULL;

	for( ; mItemList->getCount() > 0; ) {
		pthread_mutex_lock( &mMutex );

		for( int i = mItemList->getCount() - 1; i >= 0; i-- ) {
			SP_NKDoubleLinkNode_t * item = (SP_NKDoubleLinkNode_t*)mItemList->getItem( i );

			SP_HiveFile * file = (SP_HiveFile*)item->mData;

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

	mTimeList = NULL;
}

SP_HiveFile * SP_HiveFileCache :: get( const char * path )
{
	SP_HiveFile * ret = NULL;

	mLockManager->lock( path, -1 );
	{
		pthread_mutex_lock( &mMutex );
		{
			SP_NKDoubleLinkNode_t key;
			key.mData = new SP_HiveFile( path, NULL );

			int index = mItemList->find( &key );

			delete (SP_HiveFile*)key.mData;

			if( index >= 0 ) {
				SP_NKDoubleLinkNode_t * node = (SP_NKDoubleLinkNode_t*)mItemList->getItem( index );
				ret = (SP_HiveFile*)node->mData;
				ret->addRef();

				mTimeList->remove( node );
				mTimeList->append( node );
			}
		}
		pthread_mutex_unlock( &mMutex );

		if( NULL == ret ) {
			void * adb = sp_tcadbnew();
			if( sp_tcadbopen( adb, path ) ) {
				ret = new SP_HiveFile( path, adb );
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

int SP_HiveFileCache :: save( SP_HiveFile * file )
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
				SP_HiveFile * file = (SP_HiveFile*)node->mData;

				if( file->getRef() <= 0 ) {
					toDeleteList.append( node );
				}

				if( mItemList->getCount() - toDeleteList.getCount() <= mMaxCount ) {
					break;
				}
			}

			for( int i = 0; i < toDeleteList.getCount(); i++ ) {
				node = (SP_NKDoubleLinkNode_t*)toDeleteList.getItem( i );

				SP_HiveFile * file = (SP_HiveFile*)node->mData;

				index = mItemList->find( file );

				assert( index >= 0 );

				mItemList->takeItem( index );
				mTimeList->remove( node );

				delete file;
				free( node );
			}
		}
	}
	pthread_mutex_unlock( &mMutex );

	return ret;
}

//====================================================================

SP_HiveFileCacheGuard :: SP_HiveFileCacheGuard( SP_HiveFileCache * cache )
{
	mFile = NULL;
	mCache = cache;
}

SP_HiveFileCacheGuard :: ~SP_HiveFileCacheGuard()
{
	if( NULL != mFile ) mCache->save( mFile );
	mFile = NULL;
}

SP_HiveFile * SP_HiveFileCacheGuard :: get( const char * path )
{
	if( NULL == mFile ) mFile = mCache->get( path );

	return mFile;
}

