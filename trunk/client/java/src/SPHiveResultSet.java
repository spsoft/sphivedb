/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import org.json.JSONObject;
import org.json.JSONArray;

public class SPHiveResultSet {
	private JSONObject mInner;
	private JSONArray  mTypeList;
	private JSONArray  mNameList;
	private JSONArray  mRowList;

	private int mRowIndex;

	public SPHiveResultSet( JSONObject inner ) {
		mInner = inner;

		mTypeList = mInner.optJSONArray( "type" );
		mNameList = mInner.optJSONArray( "name" );
		mRowList  = mInner.optJSONArray( "row" );

		mRowIndex = 0;
	}

	public int getColumnCount() {
		return null != mTypeList ? mTypeList.length() : 0 ;
	}

	public String getType( int index ) {
		return mTypeList.getString( index );
	}

	public String getName( int index ) {
		return mNameList.getString( index );
	}

	public int getRowCount() {
		return null != mRowList ? mRowList.length() : 0;
	}

	public int moveTo( int index ) {
		mRowIndex = index;

		return 0;
	}

	public String getString( int index ) {
		JSONArray row = mRowList.optJSONArray( mRowIndex );

		return null != row ? row.optString( index ) : null;
	}

	public Integer getInt( int index ) {
		Integer ret = null;

		JSONArray row = mRowList.optJSONArray( mRowIndex );
		if( null != row ) {
			if( index >= 0 && index < row.length() ) {
				ret = new Integer( row.getInt( index ) );
			}
		}

		return ret;
	}

	public Double getDouble( int index ) {
		Double ret = null;

		JSONArray row = mRowList.optJSONArray( mRowIndex );
		if( null != row ) {
			if( index >= 0 && index < row.length() ) {
				ret = new Double( row.getDouble( index ) );
			}
		}

		return ret;
	}
};

