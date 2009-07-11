/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

package sphivedbcli;

import java.util.Random;
import java.util.ArrayList;

public class TestStress {

	public class Task extends Thread {
		private SPHiveDBClient mClient;
		private int mReadTimes;
		private int mWriteTimes;
		private int mFailTimes;

		private Random mRandom;

		public Task( SPHiveDBClient client, Random random, int readTimes, int writeTimes ) {
			mClient = client;
			mReadTimes = readTimes;
			mWriteTimes = writeTimes;
			mFailTimes = 0;

			mRandom = random;
		}

		int getReadTimes() { return mReadTimes; }

		int getWriteTimes() { return mWriteTimes; }

		int getFailTimes() { return mFailTimes; }

		public void run() {
			int tableKeyMax = mClient.getEndPointTable().getTableKeyMax();

			int loops = mReadTimes + mWriteTimes;
			int maxUid = tableKeyMax * 100;

			String readSql[] = { "select * from addrbook limit 100;" };

			long beginTime = System.currentTimeMillis();

			int readTimes = 0, writeTimes = 0;

			for( int i = 0; i < loops; i++ ) {
				boolean isWrite = mRandom.nextInt( loops ) >= mReadTimes;

				int uid = mRandom.nextInt( maxUid );

				String actionSql [] = readSql;
				if( isWrite ) {
					writeTimes++;
					String writeSql = "insert into addrbook ( gid, addr, freq ) values ( 0, '"
							+ i + "." + System.currentTimeMillis() + "." + "', 0 )";
					actionSql = new String[] { writeSql };
				} else {
					readTimes++;
				}

				SPHiveRespObject respObj = mClient.execute( uid / 100, "" + uid, "addrbook", actionSql );
				if( null != respObj ) {
					if( 0 != respObj.getErrorCode() ) {
						mFailTimes++;
						System.out.println( "errcode " + respObj.getErrdataCode()
								+ ", errmsg " + respObj.getErrdataMsg() );
					}
				} else {
					mFailTimes++;
				}

				try {
					java.lang.Thread.sleep( 0, 10 );
				} catch( Exception e ) {
				}
			}

			long endTime = System.currentTimeMillis();

			long usedTime = endTime - beginTime;

			double writePerSeconds = ( writeTimes * 1000.0 ) / usedTime;
			double readPerSeconds = ( readTimes * 1000.0 ) / usedTime;

			System.out.println( "Used Time: " + usedTime + "(ms), Write "
					+ writeTimes + " (" + writePerSeconds + "), "
					+ "Read " + readTimes + " (" + readPerSeconds + "), Fail " + mFailTimes );

			mReadTimes = readTimes;
			mWriteTimes = writeTimes;
		}
	};

	public void test( String [] args ) throws Exception {
		String configFile = args[0];
		int clientCount = Integer.parseInt( args[1] );
		int readTimes = Integer.parseInt( args[2] );
		int writeTimes = Integer.parseInt( args[3] );

		SPHiveDBClient client = new SPHiveDBClient();
		client.init( configFile );

		Random random = new Random( System.currentTimeMillis() );

		ArrayList runlist = new ArrayList();

		System.out.println( "create threads ..." );
		for( int i = 0; i < clientCount; i++ ) {
			runlist.add( new Task( client, random, readTimes, writeTimes ) );
		}

		long beginTime = System.currentTimeMillis();

		System.out.println( "run threads ..." );
		for( int i = 0; i < clientCount; i++ ) {
			( (Thread)runlist.get(i) ).start();
		}

		System.out.println( "all threads are running ..." );

		int totalReadTimes = 0, totalWriteTimes = 0, totalFailTimes = 0;
		for( int i = 0; i < clientCount; i++ ) {
			( (Thread)runlist.get(i) ).join();
			totalReadTimes += ((Task)runlist.get(i)).getReadTimes();
			totalWriteTimes += ((Task)runlist.get(i)).getWriteTimes();
			totalFailTimes += ((Task)runlist.get(i)).getFailTimes();
		}

		long endTime = System.currentTimeMillis();

		long usedTime = endTime - beginTime;

		double writePerSeconds = ( totalWriteTimes * 1000.0 ) / usedTime;
		double readPerSeconds = ( totalReadTimes * 1000.0 ) / usedTime;

		System.out.println( "\nTotal used Time: " + usedTime + "(ms), Write "
				+ totalWriteTimes + " (" + writePerSeconds + "), "
				+ "Read " + totalReadTimes + " (" + readPerSeconds + "), Fail " + totalFailTimes );
	}

	public static void main( String [] args ) {
		if( 4 != args.length ) {
			System.out.println( "Usage: TestStress <config file> <client count> <read times> <write times>" );
			System.out.println( "\tjava -cp sphivedbcli.jar:3rdlib/json.jar sphivedbcli.TestStress "
					+ "../../sphivedb/sphivedbcli.ini 8 100 100" );
			return;
		}

		try {
			TestStress testCase = new TestStress();
			testCase.test( args );
		} catch ( Exception e ) {
		}
	}

};

