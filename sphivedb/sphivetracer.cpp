/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#include <string.h>
#include <syslog.h>

#include "sphivetracer.hpp"

#include "spnetkit/spnklog.hpp"

int SP_HiveExecuteTracer :: mCount = 0;
SP_HiveExecuteTracer::Stat_t SP_HiveExecuteTracer :: mGlobalTotal = { 0 };
SP_HiveExecuteTracer::Stat_t SP_HiveExecuteTracer :: mGlobalMax = { 0 };

SP_HiveExecuteTracer :: SP_HiveExecuteTracer()
{
	memset( &mStat, 0, sizeof( mStat ) );
}

SP_HiveExecuteTracer :: ~SP_HiveExecuteTracer()
{
	mGlobalTotal.mLock += mStat.mLock;
	mGlobalTotal.mLoad += mStat.mLoad;
	mGlobalTotal.mSchema += mStat.mSchema;
	mGlobalTotal.mTransBegin += mStat.mTransBegin;
	mGlobalTotal.mTrans += mStat.mTrans;
	mGlobalTotal.mTransEnd += mStat.mTransEnd;
	mGlobalTotal.mSave += mStat.mSave;
	mGlobalTotal.mClose += mStat.mClose;

	if( mStat.mLock > mGlobalMax.mLock ) mGlobalMax.mLock = mStat.mLock;
	if( mStat.mLoad > mGlobalMax.mLoad ) mGlobalMax.mLoad = mStat.mLoad;
	if( mStat.mSchema > mGlobalMax.mSchema ) mGlobalMax.mSchema = mStat.mSchema;
	if( mStat.mTransBegin > mGlobalMax.mTransBegin ) mGlobalMax.mTransBegin = mStat.mTransBegin;
	if( mStat.mTrans > mGlobalMax.mTrans ) mGlobalMax.mTrans = mStat.mTrans;
	if( mStat.mTransEnd > mGlobalMax.mTransEnd ) mGlobalMax.mTransEnd = mStat.mTransEnd;
	if( mStat.mSave > mGlobalMax.mSave ) mGlobalMax.mSave = mStat.mSave;
	if( mStat.mClose > mGlobalMax.mClose ) mGlobalMax.mClose = mStat.mClose;

	++mCount;

	if( mCount > 0 && 0 == ( mCount % 1000 ) ) {
		Stat_t total = mGlobalTotal, max = mGlobalMax;

		SP_NKLog::log( LOG_ERR, "STAT: lk %.2f(%d), ld %.2f(%d), s %.2f(%d), tb %.2f(%d), "
				"t %.2f(%d), te %.2f(%d), sv %.2f(%d), cl %.2f(%d)",
				total.mLock / 1000.0, max.mLock, total.mLoad / 1000.0, max.mLoad,
				total.mSchema / 1000.0, max.mSchema, total.mTransBegin / 1000.0, max.mTransBegin,
				total.mTrans / 1000.0, max.mTrans, total.mTransEnd / 1000.0, max.mTransEnd,
				total.mSave / 1000.0, max.mSave, total.mClose / 1000.0, max.mClose );
	}
}

void SP_HiveExecuteTracer :: lock()
{
	mStat.mLock = mClock.getIntervalUsec();
}

void SP_HiveExecuteTracer :: load()
{
	mStat.mLoad = mClock.getIntervalUsec();
}

void SP_HiveExecuteTracer :: schema()
{
	mStat.mSchema = mClock.getIntervalUsec();
}

void SP_HiveExecuteTracer :: transBegin()
{
	mStat.mTransBegin = mClock.getIntervalUsec();
}

void SP_HiveExecuteTracer :: trans()
{
	mStat.mTrans = mClock.getIntervalUsec();
}

void SP_HiveExecuteTracer :: transEnd()
{
	mStat.mTransEnd = mClock.getIntervalUsec();
}

void SP_HiveExecuteTracer :: save()
{
	mStat.mSave = mClock.getIntervalUsec();
}

void SP_HiveExecuteTracer :: close()
{
	mStat.mClose = mClock.getIntervalUsec();
}

