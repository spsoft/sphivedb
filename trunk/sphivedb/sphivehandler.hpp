/*
 * Copyright 2009 Stephen Liu
 * For license terms, see the file COPYING along with this library.
 */

#ifndef __sphivehandler_hpp__
#define __sphivehandler_hpp__

#include "spserver/sphttp.hpp"

class SP_HiveManager;

class SP_HiveReqObject;
class SP_HiveRespObjectGather;

class SP_HiveHandler : public SP_HttpHandler {
public:
	SP_HiveHandler( SP_HiveManager * manager );
	virtual ~SP_HiveHandler();

	virtual void handle( SP_HttpRequest * request, SP_HttpResponse * response );

private:

	void handleJson( SP_HttpRequest * request, SP_HttpResponse * response );

	void handleProtoBuf( SP_HttpRequest * request, SP_HttpResponse * response );

	int doExecute( SP_HiveReqObject * reqObject, SP_HiveRespObjectGather * respObject );

	int doRemove( SP_HiveReqObject * reqObject, SP_HiveRespObjectGather * respObject );

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

