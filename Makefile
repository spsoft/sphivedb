
#--------------------------------------------------------------------

ifeq ($(origin version), undefined)
	version = 0.1
endif

#--------------------------------------------------------------------

all: 3rd
	@( cd sphivedb; make )

3rd:
	@( cd spjson; make )
	@( cd spnetkit; make )
	@( cd spserver; make )

dist: clean sphivedb-$(version).src.tar.gz

sphivedb-$(version).src.tar.gz:
	@find . -type f | grep -v CVS | grep -v .svn | sed s:^./:sphivedb-$(version)/: > MANIFEST
	@(cd ..; ln -s sphivedb sphivedb-$(version))
	(cd ..; tar cvf - `cat sphivedb/MANIFEST` | gzip > sphivedb/sphivedb-$(version).src.tar.gz)
	@(cd ..; rm sphivedb-$(version))

clean:
	@( cd spjson; make clean )
	@( cd spnetkit; make clean )
	@( cd spserver; make clean )
	@( cd sphivedb; make clean )

