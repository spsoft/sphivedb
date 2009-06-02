
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
		sphiveconfig.o sphivedbsvr.o sphivefile.o

TARGET = libsphivedbcli.so libsphivedbcli.a sphivedbsvr sqlite

TEST_TARGET = testmemvfs testmsg testmanager testcli

#--------------------------------------------------------------------

all: $(TARGET) $(TEST_TARGET)

libsphivedbcli.so: $(LIBOBJS)
	$(LINKER) $(SOFLAGS) $^ -o $@

libsphivedbcli.a: $(LIBOBJS)
	$(AR) $@ $^

sphivedbsvr: $(SVROBJS) sqlite3.o
	$(LINKER) -o $@ $^ -static $(LDFLAGS) -L. -lsphivedbcli

sqlite: sqlite3shell.o sqlite3.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmemvfs: spmemvfs.o testmemvfs.o sqlite3.o
	$(LINKER) -o $@ $^ -lpthread -lrt -ldl

testmsg: sphivecomm.o sphivemsg.o testmsg.o sqlite3.o
	$(LINKER) -o $@ $^ $(LDFLAGS)

testmanager: testmanager.o sphivemanager.o sphivemsg.o \
		sphiveconfig.o sphivecomm.o spcabinet.o spmemvfs.o sqlite3.o \
		sphivefile.o
	$(LINKER) -o $@ $^ $(LDFLAGS)

testcli: testcli.o
	$(LINKER) -o $@ $^ $(LDFLAGS) -L. -lsphivedbcli

spcabinet.o: CFLAGS += -std=c99

clean:
	@( $(RM) *.o vgcore.* core core.* $(TARGET) $(TEST_TARGET) )

