/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.net.HttpURLConnection;
import java.net.URL;

import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.BufferedReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONTokener;

public class SPHiveDBProtocol {

	private static Logger _logger = Logger.getLogger( SPHiveDBProtocol.class.getName() );

	private String mUrl = null;

	public SPHiveDBProtocol( String url ) {
		mUrl = url;
	}

	protected String makeReq( String method, int dbfile, String user,
			String dbname, String [] sql ) {
		JSONObject params = new JSONObject();
		{
			params.put( "dbfile", dbfile );
			params.put( "user", user );
			params.put( "dbname", dbname );

			if( null != sql ) {
				JSONArray sqlArray = new JSONArray();
				for( int i = 0; i < sql.length; i++ ) {
					sqlArray.put( sql[i] );
				}

				params.put( "sql", sqlArray );
			}
		}

		JSONArray paramsArray = new JSONArray();
		paramsArray.put( params );

		JSONObject req = new JSONObject();
		req.put( "id", "" + System.currentTimeMillis() );
		req.put( "method", method );
		req.put( "params", paramsArray );

		return req.toString();
	}

	public SPHiveRespObject execute( int dbfile, String user,
			String dbname, String [] sql ) {

		HttpURLConnection conn = null;

		try {
			String req = makeReq( "execute", dbfile, user, dbname, sql );

			URL urlObj = new URL( mUrl );

			conn = (HttpURLConnection) urlObj.openConnection();

			conn.setDoOutput(true);
			conn.setRequestMethod( "POST" );

			OutputStream output = conn.getOutputStream();
			output.write( req.getBytes( "US-ASCII" ) );
			output.close();

			conn.connect();

			InputStream input = conn.getInputStream();
			BufferedReader reader =
					new BufferedReader(new InputStreamReader(input));
			StringBuffer buffer = new StringBuffer();
			String line = null;

			while (null != (line = reader.readLine())) {
				buffer.append(line);
			}

			conn.disconnect();
			conn = null;

			JSONTokener tokener = new JSONTokener( buffer.toString() );

			return new SPHiveRespObject( (JSONObject)tokener.nextValue() );
		} catch (Exception e) {
			_logger.log(Level.WARNING, "ERROR", e);

			if( null != conn ) {
				try {
					conn.disconnect();
				} catch( Exception e1 ) {
					_logger.log(Level.WARNING, "ERROR", e1);
				}
			}
		}

		return null;
	}

	public int remove( int dbfile, String user, String dbname ) {

		HttpURLConnection conn = null;

		try {
			String req = makeReq( "remove", dbfile, user, dbname, null );

			URL urlObj = new URL( mUrl );

			conn = (HttpURLConnection) urlObj.openConnection();

			conn.setDoOutput(true);
			conn.setRequestMethod( "POST" );

			OutputStream output = conn.getOutputStream();
			output.write( req.getBytes( "US-ASCII" ) );
			output.close();

			conn.connect();

			InputStream input = conn.getInputStream();
			BufferedReader reader =
					new BufferedReader(new InputStreamReader(input));
			StringBuffer buffer = new StringBuffer();
			String line = null;

			while (null != (line = reader.readLine())) {
				buffer.append(line);
			}

			conn.disconnect();
			conn = null;

			JSONTokener tokener = new JSONTokener( buffer.toString() );

			JSONObject respObj = (JSONObject)tokener.nextValue();

			String result = respObj.optString( "result" );

			return null != respObj ? Integer.parseInt( result ) : -1;
		} catch (Exception e) {
			_logger.log(Level.WARNING, "ERROR", e);

			if( null != conn ) {
				try {
					conn.disconnect();
				} catch( Exception e1 ) {
					_logger.log(Level.WARNING, "ERROR", e1);
				}
			}
		}

		return -1;
	}
};

