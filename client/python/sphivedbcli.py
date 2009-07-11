"""
  Copyright 2009 Stephen Liu
  For license terms, see the file COPYING along with this library.
"""

#####################################################################

"""
  Copyright (c) 2007 Jan-Klaas Kollhof

  This file is part of jsonrpc.

  jsonrpc is free software; you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation; either version 2.1 of the License, or
  (at your option) any later version.

  This software is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this software; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""

from types import *
import re

CharReplacements ={
        '\t': '\\t',
        '\b': '\\b',
        '\f': '\\f',
        '\n': '\\n',
        '\r': '\\r',
        '\\': '\\\\',
        '/': '\\/',
        '"': '\\"'}

EscapeCharToChar = {
        't': '\t',
        'b': '\b',
        'f': '\f',
        'n': '\n',
        'r': '\r',
        '\\': '\\',
        '/': '/',
        '"' : '"'}

StringEscapeRE= re.compile(r'[\x00-\x19\\"/\b\f\n\r\t]')
Digits = ['0', '1', '2','3','4','5','6','7','8','9']


class JSONEncodeException(Exception):
    def __init__(self, obj):
        Exception.__init__(self)
        self.obj = obj

    def __str__(self):
       return "Object not encodeable: %s" % self.obj

       
class JSONDecodeException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

    def __str__(self):
       return self.message

    
def escapeChar(match):
    c=match.group(0)
    try:
        replacement = CharReplacements[c]
        return replacement
    except KeyError:
        d = ord(c)
        if d < 32:
            return '\\u%04x' % d
        else:
            return c

def dumps(obj):
    return unicode("".join([part for part in dumpParts (obj)]))

def dumpParts (obj):
    objType = type(obj)
    if obj == None:
       yield u'null'
    elif objType is BooleanType:
        if obj:
            yield u'true'
        else:
            yield u'false'
    elif objType is DictionaryType:
        yield u'{'
        isFirst=True
        for (key, value) in obj.items():
            if isFirst:
                isFirst=False
            else:
                yield u","
            yield u'"' + StringEscapeRE.sub(escapeChar, key) +u'":'
            for part in dumpParts (value):
                yield part
        yield u'}'
    elif objType in StringTypes:
        yield u'"' + StringEscapeRE.sub(escapeChar, obj) +u'"'

    elif objType in [TupleType, ListType, GeneratorType]:
        yield u'['
        isFirst=True
        for item in obj:
            if isFirst:
                isFirst=False
            else:
                yield u","
            for part in dumpParts (item):
                yield part
        yield u']'
    elif objType in [IntType, LongType, FloatType]:
        yield unicode(obj)
    else:
        raise JSONEncodeException(obj)
    

def loads(s):
    stack = []
    chars = iter(s)
    value = None
    currCharIsNext=False

    try:
        while(1):
            skip = False
            if not currCharIsNext:
                c = chars.next()
            while(c in [' ', '\t', '\r','\n']):
                c = chars.next()
            currCharIsNext=False
            if c=='"':
                value = ''
                try:
                    c=chars.next()
                    while c != '"':
                        if c == '\\':
                            c=chars.next()
                            try:
                                value+=EscapeCharToChar[c]
                            except KeyError:
                                if c == 'u':
                                    hexCode = chars.next() + chars.next() + chars.next() + chars.next()
                                    value += unichr(int(hexCode,16))
                                else:
                                    raise JSONDecodeException("Bad Escape Sequence Found")
                        else:
                            value+=c
                        c=chars.next()
                except StopIteration:
                    raise JSONDecodeException("Expected end of String")
            elif c == '{':
                stack.append({})
                skip=True
            elif c =='}':
                value = stack.pop()
            elif c == '[':
                stack.append([])
                skip=True
            elif c == ']':
                value = stack.pop()
            elif c in [',',':']:
                skip=True
            elif c in Digits or c == '-':
                digits=[c]
                c = chars.next()
                numConv = int
                try:
                    while c in Digits:
                        digits.append(c)
                        c = chars.next()
                    if c == ".":
                        numConv=float
                        digits.append(c)
                        c = chars.next()
                        while c in Digits:
                            digits.append(c)
                            c = chars.next()
                        if c.upper() == 'E':
                            digits.append(c)
                            c = chars.next()
                            if c in ['+','-']:
                                digits.append(c)
                                c = chars.next()
                                while c in Digits:
                                    digits.append(c)
                                    c = chars.next()
                            else:
                                raise JSONDecodeException("Expected + or -")
                except StopIteration:
                    pass
                value = numConv("".join(digits))
                currCharIsNext=True

            elif c in ['t','f','n']:
                kw = c+ chars.next() + chars.next() + chars.next()
                if kw == 'null':
                    value = None
                elif kw == 'true':
                    value = True
                elif kw == 'fals' and chars.next() == 'e':
                    value = False
                else:
                    raise JSONDecodeException('Expected Null, False or True')
            else:
                raise JSONDecodeException('Expected []{}," or Number, Null, False or True')

            if not skip:
                if len(stack):
                    top = stack[-1]
                    if type(top) is ListType:
                        top.append(value)
                    elif type(top) is DictionaryType:
                        stack.append(value)
                    elif type(top)  in StringTypes:
                        key = stack.pop()
                        stack[-1][key] = value
                    else:
                        raise JSONDecodeException("Expected dictionary key, or start of a value")
                else:
                    return value
    except StopIteration:
         raise JSONDecodeException("Unexpected end of JSON source")

#####################################################################

"""
  Copyright (c) 2007 Jan-Klaas Kollhof

  This file is part of jsonrpc.

  jsonrpc is free software; you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation; either version 2.1 of the License, or
  (at your option) any later version.

  This software is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this software; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
"""

import urllib

class JSONRPCException(Exception):
    def __init__(self, rpcError):
        Exception.__init__(self)
        self.error = rpcError
        
class ServiceProxy(object):
    def __init__(self, serviceURL, serviceName=None):
        self.__serviceURL = serviceURL
        self.__serviceName = serviceName

    def __getattr__(self, name):
        if self.__serviceName != None:
            name = "%s.%s" % (self.__serviceName, name)
        return ServiceProxy(self.__serviceURL, name)

    def __call__(self, *args):
         postdata = dumps({"method": self.__serviceName, 'params': args, 'id':'jsonrpc'})
         respdata = urllib.urlopen(self.__serviceURL, postdata).read()
         return loads(respdata)


#####################################################################

class EndPoint:
	def __init__( self ):
		self.ip = ""
		self.port = -1
		self.keyMin = -1
		self.keyMax = -1

	def __str__( self ):
		return "ip %s, port %d, keyMin %d, keyMax %d" \
				% ( self.ip, self.port, self.keyMin, self.keyMax )

#####################################################################

class EndPointTable:
	def __init__( self, tableKeyMax ):
		self.tableKeyMax = tableKeyMax
		self.list = []

	def getTableKeyMax( self ):
		return self.tableKeyMax

	def getCount( self ):
		return len( self.list )

	def getByKey( self, key ):
		for ep in self.list:
			if ep.keyMin <= key and key <= ep.keyMax:
				return ep

		return None

	def add( self, ep ):
		self.list.append( ep )

	def get( self, index ):
		return self.list[ index ]

#####################################################################

class SPHiveResultSet:
	def __init__( self, inner ):
		self.inner = inner
		self.rowIndex = 0

	def __str__( self ):
		return self.inner.__str__()

	def getColumnCount( self ):
		if self.inner.has_key( "name" ):
			return len( self.inner[ "name" ] )
		return 0

	def getType( self, index ):
		if self.inner.has_key( "type" ):
			return self.inner[ "type" ][ index ]

		return None

	def getName( self, index ):
		if self.inner.has_key( "name" ):
			return self.inner[ "name" ][ index ]

		return None

	def getRowCount( self ):
		if self.inner.has_key( "row" ):
			return len( self.inner[ "row" ] )

		return 0

	def moveTo( self, index ):
		self.rowIndex = index

	def getString( self, index ):
		if self.inner.has_key( "row" ):
			row = self.inner[ "row" ][ self.rowIndex ]
			return row[ index ]

		return None

	def getInt( self, index ):
		if self.inner.has_key( "row" ):
			row = self.inner[ "row" ][ self.rowIndex ]
			return int(row[ index ])

		return None

	def getDouble( self, index ):
		if self.inner.has_key( "row" ):
			row = self.inner[ "row" ][ self.rowIndex ]
			return float(row[ index ])

		return None

#####################################################################

class SPHiveRespObject:
	def __init__( self, inner ):
		self.inner = inner

	def __str__( self ):
		return self.inner.__str__()

	def getErrorCode( self ):
		if self.inner.has_key( "error" ):
			error = self.inner[ "error" ]
			if error.has_key( "code" ):
				return error[ "code" ]
		return 0

	def getErrorMsg( self ):
		if self.inner.has_key( "error" ):
			error = self.inner[ "error" ]
			if error.has_key( "message" ):
				return error[ "message" ]
		return None

	def getErrdataCode( self ):
		if self.inner.has_key( "error" ):
			error = self.inner[ "error" ]
			if error.has_key( "data" ):
				data = error[ "data" ]
				if data.has_key( "code" ):
					return data[ "code" ]
		return 0

	def getErrdataMsg( self ):
		if self.inner.has_key( "error" ):
			error = self.inner[ "error" ]
			if error.has_key( "data" ):
				data = error[ "data" ]
				if data.has_key( "message" ):
					return data[ "message" ]

		return None 

	def getResultCount( self ):
		if self.inner.has_key( "result" ):
			return len( self.inner[ "result" ] )
		return 0

	def getResultSet( self, index ):
		if self.inner.has_key( "result" ):
			return SPHiveResultSet( self.inner[ "result" ][ index ] )
		return None

#####################################################################

import ConfigParser

class SPHiveDBClient:
	def __init__( self ):
		self.endPointTable = None

	def getEndPointTable( self ):
		return self.endPointTable

	def init( self, configFile ):
		config = ConfigParser.RawConfigParser()
		config.read( configFile )

		tableKeyMax = config.getint( "EndPointTable", "TableKeyMax" )

		count = config.getint( "EndPointTable", "EndPointCount" )

		self.endPointTable = EndPointTable( tableKeyMax )

		for i in range( 0, count ):
			section = ( "EndPoint%d" % ( i ) )

			ep = EndPoint()

			ep.ip = config.get( section, "ServerIP" );
			ep.port = config.getint( section, "ServerPort" );
			ep.keyMin = config.getint( section, "KeyMin" );
			ep.keyMax = config.getint( section, "KeyMax" );

			self.endPointTable.add( ep )

		return 0

	def execute( self, dbfile, user, dbname, sql ):
		ep = self.endPointTable.getByKey( dbfile )

		if None == ep: return None

		url = "http://%s:%d/sphivedb" % ( ep.ip, ep.port )

		proxy = ServiceProxy( url )

		args  = {}
		args[ "dbfile" ] = dbfile
		args[ "dbname" ] = dbname
		args[ "user" ] = user
		args[ "sql" ] = sql

		return SPHiveRespObject( proxy.execute( args ) )

	def remove( self, dbfile, user, dbname ):
		ep = self.endPointTable.getByKey( dbfile )

		if None == ep: return -1

		url = "http://%s:%d/sphivedb" % ( ep.ip, ep.port )

		proxy = ServiceProxy( url )

		args  = {}
		args[ "dbfile" ] = dbfile
		args[ "dbname" ] = dbname
		args[ "user" ] = user

		resp = proxy.remove( args )

		if resp.has_key( "result" ):
			return resp[ "result" ]
		else:
			return -1

