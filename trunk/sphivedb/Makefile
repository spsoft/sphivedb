
include ../port/port.mk

#--------------------------------------------------------------------

LIBEVENT_LIB  = -L$(HOME)/libevent -levent -lrt

TC_LIB = -L$(HOME)/tokyocabinet -ltokyocabinet -lm -lbz2 -lz
TC_INCL = -I$(HOME)/tokyocabinet

#--------------------------------------------------------------------

CFLAGS += -I../ $(TC_INCL)

LDFLAGS += -ldl -lm

COMMON_LIB = -L../spserver -lspserver -L../spnetkit -lspnetkit \
		-L../spjson -lspjson $(LIBEVENT_LIB) $(LDFLAGS)

HIVEDBSVR_LIB = $(TC_LIB) $(COMMON_LIB)

SQLITESVR_LIB = $(COMMON_LIB)

#--------------------------------------------------------------------

LIBOBJS = sphivecomm.o sphivemsg.o sphivedbcli.o

HIVEDBSVR_OBJS = spmemvfs.o spcabinet.o sphivehandler.o sphivemanager.o \
		sphiveconfig.o sphivedbsvr.o sphiveschema.o \
		sphivestore.o spdbmstore.o

SQLITESVR_OBJS = sphivehandler.o sphivemanager.o \
		sphiveconfig.o sphiveschema.o \
		sphivestore.o spfilestore.o spsqlitesvr.o

TARGET = libsphivedbcli.so libsphivedbcli.a \
		sphivedbsvr spsqlitesvr sqlite

TEST_TARGET = testmemvfs testmsg testmanager testcli testschema \
		testconfig testsqlite

#--------------------------------------------------------------------

all: $(TARGET) $(TEST_TARGET)

libsphivedbcli.so: $(LIBOBJS)
	$(LINKER) $(SOFLAGS) $^ -o $@

libsphivedbcli.a: $(LIBOBJS)
	$(AR) $@ $^

sphivedbsvr: $(HIVEDBSVR_OBJS) spsqlite.o
	$(LINKER) -o $@ $^ -static $(HIVEDBSVR_LIB) -L. -lsphivedbcli

spsqlitesvr: $(SQLITESVR_OBJS) spsqlite.o
	$(LINKER) -o $@ $^ -static $(SQLITESVR_LIB) -L. -lsphivedbcli

sqlite: sqlite3shell.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmemvfs: spmemvfs.o testmemvfs.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmsg: sphivecomm.o sphivemsg.o testmsg.o spsqlite.o
	$(LINKER) -o $@ $^ $(COMMON_LIB)

testmanager: testmanager.o sphivemanager.o sphivemsg.o \
		sphiveconfig.o sphivecomm.o spcabinet.o spmemvfs.o spsqlite.o \
		sphiveschema.o sphivestore.o spdbmstore.o
	$(LINKER) -o $@ $^ $(HIVEDBSVR_LIB)

testcli: testcli.o
	$(LINKER) -o $@ $^ $(COMMON_LIB) -L. -lsphivedbcli

testschema: testschema.o sphiveschema.o sphiveconfig.o spsqlite.o
	$(LINKER) -o $@ $^ $(COMMON_LIB)

testconfig: testconfig.o sphiveconfig.o
	$(LINKER) -o $@ $^ $(COMMON_LIB)

testsqlite: testsqlite.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -ldl -lstdc++

spcabinet.o: CFLAGS += -std=c99

clean:
	@( $(RM) *.o vgcore.* core core.* $(TARGET) $(TEST_TARGET) )

