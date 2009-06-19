
include ../port/port.mk

#--------------------------------------------------------------------

LIBEVENT_LIB  = -L$(HOME)/libevent -levent -lrt

TC_LIB = -L$(HOME)/tokyocabinet -ltokyocabinet -lm -lbz2 -lz
TC_INCL = -I$(HOME)/tokyocabinet

CFLAGS += -I../ $(TC_INCL)

LDFLAGS += -L../spserver -lspserver -L../spnetkit -lspnetkit \
		-L../spjson -lspjson \
		$(LIBEVENT_LIB) $(TC_LIB) -lpthread -lstdc++ -ldl

LIBOBJS = sphivecomm.o sphivemsg.o sphivedbcli.o

SVROBJS = spmemvfs.o spcabinet.o sphivehandler.o sphivemanager.o \
		sphiveconfig.o sphivedbsvr.o sphiveschema.o \
		sphivestore.o spdbmstore.o

TARGET = libsphivedbcli.so libsphivedbcli.a sphivedbsvr sqlite

TEST_TARGET = testmemvfs testmsg testmanager testcli testschema \
		testconfig testsqlite

#--------------------------------------------------------------------

all: $(TARGET) $(TEST_TARGET)

libsphivedbcli.so: $(LIBOBJS)
	$(LINKER) $(SOFLAGS) $^ -o $@

libsphivedbcli.a: $(LIBOBJS)
	$(AR) $@ $^

sphivedbsvr: $(SVROBJS) spsqlite.o
	$(LINKER) -o $@ $^ -static $(LDFLAGS) -L. -lsphivedbcli

sqlite: sqlite3shell.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmemvfs: spmemvfs.o testmemvfs.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmsg: sphivecomm.o sphivemsg.o testmsg.o spsqlite.o
	$(LINKER) -o $@ $^ $(LDFLAGS)

testmanager: testmanager.o sphivemanager.o sphivemsg.o \
		sphiveconfig.o sphivecomm.o spcabinet.o spmemvfs.o spsqlite.o \
		sphiveschema.o sphivestore.o spdbmstore.o
	$(LINKER) -o $@ $^ $(LDFLAGS)

testcli: testcli.o
	$(LINKER) -o $@ $^ $(LDFLAGS) -L. -lsphivedbcli

testschema: testschema.o sphiveschema.o sphiveconfig.o spsqlite.o
	$(LINKER) -o $@ $^ $(LDFLAGS)

testconfig: testconfig.o sphiveconfig.o
	$(LINKER) -o $@ $^ $(LDFLAGS)

testsqlite: testsqlite.o spsqlite.o
	$(LINKER) -o $@ $^ -lpthread -ldl -lstdc++

spcabinet.o: CFLAGS += -std=c99

clean:
	@( $(RM) *.o vgcore.* core core.* $(TARGET) $(TEST_TARGET) )

