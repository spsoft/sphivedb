/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

public class SPHiveDBClient {
	private EndPointTable mEndPointTable = null;

	public SPHiveDBClient() {
	}

	public EndPointTable getEndPointTable() {
		return mEndPointTable;
	}

	public boolean init( String configFile ) {
		IniFile iniFile = new IniFile( configFile );

		int tableKeyMax = iniFile.getInt( "EndPointTable", "TableKeyMax", -1 );

		if( tableKeyMax < 0 ) return false;

		int count = iniFile.getInt( "EndPointTable", "EndPointCount", -1 );

		if( count <= 0 ) return false;

		mEndPointTable = new EndPointTable( tableKeyMax );

		for( int i = 0; i < count; i++ ) {
			String section = "EndPoint" + i;

			String ip = iniFile.getString( section, "ServerIP", "" );
			int port = iniFile.getInt( section, "ServerPort", -1 );
			int keyMin = iniFile.getInt( section, "KeyMin", -1 );
			int keyMax = iniFile.getInt( section, "KeyMax", -1 );

			if( ip.length() <= 0 || port < 0 || keyMin < 0 || keyMax < 0 ) return false;

			EndPointTable.EndPoint endpoint = new EndPointTable.EndPoint( ip, port, keyMin, keyMax );
			mEndPointTable.add( endpoint );
		}

		return true;
	}

	public SPHiveRespObject execute( int dbfile, String user,
			String dbname, String [] sql ) {

		EndPointTable.EndPoint endpoint = mEndPointTable.getByKey( dbfile );

		if( null == endpoint ) return null;

		String url = "http://" + endpoint.getIP() + ":" + endpoint.getPort() + "/sphivedb";

		SPHiveDBProtocol protocol = new SPHiveDBProtocol( url );

		return protocol.execute( dbfile, user, dbname, sql );
	}

	public int remove( int dbfile, String user, String dbname ) {

		EndPointTable.EndPoint endpoint = mEndPointTable.getByKey( dbfile );

		if( null == endpoint ) return -1;

		String url = "http://" + endpoint.getIP() + ":" + endpoint.getPort() + "/sphivedb";

		SPHiveDBProtocol protocol = new SPHiveDBProtocol( url );

		return protocol.remove( dbfile, user, dbname );
	}
};

