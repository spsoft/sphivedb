
include ../port/port.mk

#--------------------------------------------------------------------

LIBEVENT_LIB  = -L$(HOME)/libevent -levent -lrt

TC_LIB = -L$(HOME)/tokyocabinet -ltokyocabinet -lm -lbz2 -lz
TC_INCL = -I$(HOME)/tokyocabinet

#-------------------Cprof related Macros----------------
ifeq ($(cprof), 1)
	CFLAGS += -finstrument-functions
	LDFLAGS += -lcprof
endif

#--------------------------------------------------------------------

CFLAGS += -I../ $(TC_INCL)

LDFLAGS += -ldl -lm

COMMON_LIB = -L../spserver -lspserver -L../spnetkit -lspnetkit \
		-L../spjson -lspjson $(LIBEVENT_LIB) $(LDFLAGS)

HIVEDBSVR_LIB = $(TC_LIB) $(COMMON_LIB)

SQLITESVR_LIB = $(COMMON_LIB)

#--------------------------------------------------------------------

LIBOBJS = sphivemsg.o sphivejson.o sphivepb.o sphivedbcli.o

HIVEDBSVR_OBJS = spmemvfs.o spcabinet.o sphivehandler.o sphivemanager.o \
		sphiveconfig.o sphivedbsvr.o sphiveschema.o \
		sphivestore.o spdbmstore.o sphivegather.o sphivetracer.o

SQLITESVR_OBJS = sphivehandler.o sphivemanager.o \
		sphiveconfig.o sphiveschema.o sphivetracer.o \
		sphivestore.o spfilestore.o spsqlitesvr.o sphivegather.o

TARGET = libsphivedbcli.so libsphivedbcli.a \
		sphivedbsvr spsqlitesvr sqlite

TEST_TARGET = testmemvfs testmsg testmanager testcli testschema \
		testconfig testsqlite teststress testremove

#--------------------------------------------------------------------

all: $(TARGET) $(TEST_TARGET)

libsphivedbcli.so: $(LIBOBJS)
	$(LINKER) $(SOFLAGS) $^ -o $@

libsphivedbcli.a: $(LIBOBJS)
	$(AR) $@ $^

sphivedbsvr: $(HIVEDBSVR_OBJS) spsqlite.o
	$(LINKER) -o $@ $^ $(HIVEDBSVR_LIB) -L. -lsphivedbcli

spsqlitesvr: $(SQLITESVR_OBJS) spsqlite.o
	$(LINKER) -o $@ $^ -static $(SQLITESVR_LIB) -L. -lsphivedbcli

sqlite: sqlite3shell.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmemvfs: spmemvfs.o testmemvfs.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmsg: sphivemsg.o sphivejson.o testmsg.o spsqlite.o
	$(LINKER) -o $@ $^ $(COMMON_LIB)

testmanager: testmanager.o sphivemanager.o sphivemsg.o \
		sphiveconfig.o sphivejson.o spcabinet.o spmemvfs.o spsqlite.o \
		sphiveschema.o sphivestore.o spdbmstore.o sphivegather.o sphivetracer.o
	$(LINKER) -o $@ $^ $(HIVEDBSVR_LIB)

testcli: testcli.o
	$(LINKER) -o $@ $^ $(COMMON_LIB) -L. -lsphivedbcli

testremove: testremove.o
	$(LINKER) -o $@ $^ $(COMMON_LIB) -L. -lsphivedbcli

teststress: teststress.o
	$(LINKER) -o $@ $^ $(COMMON_LIB) -L. -lsphivedbcli

testschema: testschema.o sphiveschema.o sphiveconfig.o spsqlite.o
	$(LINKER) -o $@ $^ $(COMMON_LIB)

testconfig: testconfig.o sphiveconfig.o
	$(LINKER) -o $@ $^ $(COMMON_LIB)

testsqlite: testsqlite.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -ldl -lstdc++

testmysql: testmysql.o
	$(LINKER) -o $@ $^ -L/usr/lib -lmysqlclient -L../spnetkit -lspnetkit \
		-lpthread -lstdc++

testmysql.o: CFLAGS += -I/usr/local/include/mysql

spcabinet.o: CFLAGS += -std=c99

spsqlite.o: CFLAGS += -O2

clean:
	@( $(RM) *.o vgcore.* core core.* $(TARGET) $(TEST_TARGET) )

