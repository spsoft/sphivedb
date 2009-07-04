/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.util.ArrayList;

public class EndPointTable {
	private int mTableKeyMax = 0;
	private ArrayList mList = null;

	public static class EndPoint {
		private String mIP = null;
		private int mPort = -1;
		private int mKeyMin = -1;
		private int mKeyMax = -1;

		public EndPoint( String ip, int port, int keyMin, int keyMax ) {
			mIP = ip;
			mPort = port;
			mKeyMin = keyMin;
			mKeyMax = keyMax;
		}

		public String toString() {
			return "ip " + mIP + ", port " + mPort + ", keyMin " + mKeyMin + ", keyMax " + mKeyMax;
		}

		public String getIP() { return mIP; }
		public int getPort() { return mPort; }
		public int getKeyMin() { return mKeyMin; }
		public int getKeyMax() { return mKeyMax; }
	};

	public EndPointTable( int tableKeyMax ) {
		mTableKeyMax = tableKeyMax;
		mList = new ArrayList();
	}

	public int getTableKeyMax() {
		return mTableKeyMax;
	}

	public int getCount() {
		return mList.size();
	}

	public EndPoint getByKey( int key ) {
		EndPoint ret = null;

		for( int i = 0; i < mList.size(); i++ ) {
			EndPoint iter = (EndPoint)mList.get( i );

			if( iter.getKeyMin() <= key && key <= iter.getKeyMax() ) {
				ret = iter;
				break;
			}
		}

		return ret;
	}

	public void add( EndPoint endpoint ) {
		mList.add( endpoint );
	}

	public EndPoint get( int index ) {
		return (EndPoint)mList.get( index );
	}
};

