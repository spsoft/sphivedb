/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivetracer_hpp__
#define __sphivetracer_hpp__

#include "spnetkit/spnktime.hpp"

class SP_HiveExecuteTracer {
public:
	SP_HiveExecuteTracer();
	~SP_HiveExecuteTracer();

	void lock();

	void load();

	void schema();

	void transBegin();

	void trans();

	void transEnd();

	void save();

	void close();

private:

typedef struct tagStat {
	int mLock, mLoad, mSchema, mTransBegin,
		mTrans, mTransEnd, mSave, mClose;
} Stat_t;

	SP_NKClock mClock;

	Stat_t mStat;

	static int mCount;
	static Stat_t mGlobalTotal, mGlobalMax;
};

#endif

