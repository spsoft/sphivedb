/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import org.json.JSONObject;
import org.json.JSONArray;

public class SPHiveRespObject {
	public SPHiveRespObject( JSONObject inner ) {
		mInner = inner;
	}

	public int getErrorCode() {
		JSONObject error = mInner.optJSONObject( "error" );

		return null != error ? error.getInt( "code" ) : 0;
	}

	public String getErrorMsg() {
		JSONObject error = mInner.optJSONObject( "error" );

		return null != error ? error.getString( "message" ) : null;
	}

	public int getErrdataCode() {
		JSONObject errdata = null;

		JSONObject error = mInner.optJSONObject( "error" );
		errdata = ( null != error ? error.optJSONObject( "data" ) : null );

		return null != errdata ? errdata.getInt( "code" ) : 0;
	}

	public String getErrdataMsg() {
		JSONObject errdata = null;

		JSONObject error = mInner.optJSONObject( "error" );
		errdata = ( null != error ? error.optJSONObject( "data" ) : null );

		return null != errdata ? errdata.getString( "message" ) : null;
	}

	public int getResultCount() {
		JSONArray resultlist = mInner.optJSONArray( "result" );

		return null != resultlist ? resultlist.length() : 0;
	}

	public SPHiveResultSet getResultSet( int index ) {
		JSONObject result = null;

		JSONArray resultlist = mInner.optJSONArray( "result" );

		result = ( null != resultlist ? resultlist.optJSONObject( index ) : null );

		return null != result ? new SPHiveResultSet( result ) : null;
	}

	private JSONObject mInner;
};

