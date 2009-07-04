/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.util.Random;
import java.util.ArrayList;

public class TestStress {

	public class Task implements Runnable {
		private SPHiveDBClient mClient;
		private int mReadTimes;
		private int mWriteTimes;

		private Random mRandom;

		public Task( SPHiveDBClient client, Random random, int readTimes, int writeTimes ) {
			mClient = client;
			mReadTimes = readTimes;
			mWriteTimes = writeTimes;

			mRandom = random;
		}

		public void run() {
			int tableKeyMax = mClient.getEndPointTable().getTableKeyMax();

			int failTimes = 0;

			int loops = mReadTimes + mWriteTimes;
			int maxUid = tableKeyMax * 100;

			String readSql[] = { "select * from addrbook limit 100;" };

			long beginTime = System.currentTimeMillis();

			for( int i = 0; i < loops; i++ ) {
				boolean isWrite = mRandom.nextInt( loops ) >= mReadTimes;

				int uid = mRandom.nextInt( maxUid );

				String actionSql [] = readSql;
				if( isWrite ) {
					String writeSql = "insert into addrbook ( gid, addr, freq ) values ( 0, '"
							+ i + "." + System.currentTimeMillis() + "." + "', 0 )";
					actionSql = new String[] { writeSql };
				}

				SPHiveRespObject respObj = mClient.execute( uid / 100, "" + uid, "addrbook", actionSql );
				if( null != respObj ) {
					if( 0 != respObj.getErrorCode() ) {
						failTimes++;
						System.out.println( "errcode " + respObj.getErrdataCode()
								+ ", errmsg " + respObj.getErrdataMsg() );
					}
				} else {
					failTimes++;
				}
			}

			long endTime = System.currentTimeMillis();

			long usedTime = endTime - beginTime;

			double writePerSeconds = ( mWriteTimes * 1000.0 ) / usedTime;
			double readPerSeconds = ( mReadTimes * 1000.0 ) / usedTime;

			System.out.println( "Used Time: " + usedTime + "(ms), Write "
					+ mWriteTimes + " (" + writePerSeconds + "), "
					+ "Read " + mReadTimes + " (" + readPerSeconds + "), Fail " + failTimes );
		}
	};

	public void test( String [] args ) {
		String configFile = args[0];
		int clientCount = Integer.parseInt( args[1] );
		int readTimes = Integer.parseInt( args[2] );
		int writeTimes = Integer.parseInt( args[3] );

		SPHiveDBClient client = new SPHiveDBClient();
		client.init( configFile );

		Random random = new Random( System.currentTimeMillis() );

		ArrayList runlist = new ArrayList();

		for( int i = 0; i < clientCount; i++ ) {
			runlist.add( new Task( client, random, readTimes, writeTimes ) );
		}

		for( int i = 0; i < clientCount; i++ ) {
			( (Runnable)runlist.get(i) ).run();
		}
	}

	public static void main( String [] args ) {
		if( 4 != args.length ) {
			System.out.println( "Usage: TestStress <config file> <client count> <read times> <write times>" );
			return;
		}

		TestStress testCase = new TestStress();
		testCase.test( args );
	}

};

