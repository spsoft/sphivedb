
import time
from threading import Thread
import thread

import sphivedbcli
import sys
import random

class Stat:
	def __init__( self ):
		self.readTimes = 0
		self.writeTimes = 0
		self.failTimes = 0

def task( client, lock, readTimes, writeTimes, stat ):
	tableKeyMax = client.getEndPointTable().getTableKeyMax()

	loops = readTimes + writeTimes
	maxUid = tableKeyMax * 100

	lock.acquire()
	lock.release()

	beginTime = int( time.time() * 1000 )

	for i in range( loops ):
		isWrite = ( random.randint( 0, loops - 1 ) >= readTimes )

		uid = random.randint( 0, maxUid - 1 )

		sql = []

		if isWrite:
			sql.append( "insert into addrbook ( gid, addr, freq ) values ( 0, '%d.%d', 0 )" \
					% ( i, time.time() ) )
			stat.readTimes = stat.readTimes + 1
		else:
			sql.append( "select * from addrbook" )
			stat.writeTimes = stat.writeTimes + 1

		try:
			respObj = client.execute( uid / 100, str( uid ), "addrbook", sql )

			if 0 != respObj.getErrorCode(): stat.failTimes = stat.failTimes + 1
		except Exception, e:
			stat.failTimes = stat.failTimes + 1
			print e

	endTime = int( time.time() * 1000 )

	usedTime = endTime - beginTime

	readPerSecond = ( 1000.0 * stat.readTimes ) / usedTime
	writePerSecond = ( 1000.0 * stat.writeTimes ) / usedTime

	print "Used Time: %d (ms), Write %d (%.2f), Read %d (%.2f), Fail %d" \
			% ( usedTime, stat.writeTimes, writePerSecond, stat.readTimes, \
			readPerSecond, stat.failTimes )

if __name__ == "__main__":
	if len( sys.argv ) != 5:
		print "Usage: %s <config file> <client count> <read times> <write times>" % ( sys.argv[0] )
		print "\tpython %s ../../sphivedb/sphivedbcli.ini 10 10 10" % ( sys.argv[0] )
		sys.exit( -1 )

	random.seed( time.time() )

	configFile = sys.argv[1]
	count = int( sys.argv[2] )
	readTimes = int( sys.argv[3] )
	writeTimes = int( sys.argv[4] )

	client = sphivedbcli.SPHiveDBClient()
	client.init( configFile )

	tlist = []
	stats = []

	lock = thread.allocate_lock()

	print "create thread ..."

	for i in range( count ):
		stat = Stat()
		stats.append( stat )

		t = Thread( target = task, args = ( client, lock, readTimes, writeTimes, stat ) )
		tlist.append( t )

	lock.acquire();

	print "start thread ..."

	beginTime = int( time.time() * 1000 )

	for t in tlist: t.start()

	lock.release()

	print "all thread are running ..."

	for t in tlist: t.join()

	endTime = int( time.time() * 1000 )

	totalReadTimes = 0
	totalWriteTimes = 0
	totalFailTimes = 0

	for s in stats:
		totalReadTimes += s.readTimes
		totalWriteTimes += s.writeTimes
		totalFailTimes += s.failTimes

	usedTime = endTime - beginTime

	readPerSecond = ( 1000.0 * totalReadTimes ) / usedTime
	writePerSecond = ( 1000.0 * totalWriteTimes ) / usedTime

	print "\nTotal Used Time: %d (ms), Write %d (%.2f), Read %d (%.2f), Fail %d" \
			% ( usedTime, totalWriteTimes, writePerSecond, totalReadTimes, \
			readPerSecond, totalFailTimes )

