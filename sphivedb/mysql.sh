

host=127.0.0.1
port=3306
user=root
pass=root

echo "create database if not exists addrbook" | mysql -h$host -P$port -u$user -p$pass

i=0

while [[ $i -lt 100 ]];
do

	echo "create table if not exists addrbook_$i \
		( id INTEGER PRIMARY KEY AUTO_INCREMENT, user varchar(32), gid int, \
		addr varchar(64), freq int, unique( addr ) ) " | mysql -h$host -P$port -u$user -p$pass addrbook

	echo "CREATE INDEX addrbook_idx_$i on addrbook_$i ( user ) " | mysql -h$host -P$port -u$user -p$pass addrbook

	i=$((i+1))

done

