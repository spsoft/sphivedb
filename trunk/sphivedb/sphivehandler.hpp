/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivehandler_hpp__
#define __sphivehandler_hpp__

#include "spserver/sphttp.hpp"

class SP_HiveManager;

class SP_JsonRpcReqObject;
class SP_JsonArrayNode;
class SP_JsonObjectNode;

class SP_HiveHandler : public SP_HttpHandler {
public:
	SP_HiveHandler( SP_HiveManager * manager );
	virtual ~SP_HiveHandler();

	virtual void handle( SP_HttpRequest * request, SP_HttpResponse * response );

private:
	int doExecute( SP_JsonRpcReqObject * rpcReq, SP_JsonArrayNode * result,
			SP_JsonObjectNode * error );

private:
	SP_HiveManager * mManager;
};

class SP_HiveHandlerFactory : public SP_HttpHandlerFactory {
public:
	SP_HiveHandlerFactory( SP_HiveManager * manager );
	virtual ~SP_HiveHandlerFactory();

	virtual SP_HttpHandler * create() const;

private:
	SP_HiveManager * mManager;
};

#endif

