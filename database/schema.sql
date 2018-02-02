drop table if exists connections;
drop table if exists probes;

create table connections (
    ID integer primary key autoincrement,
    client text,
    src_addr text,
    src_port text,
    dst_addr text,
    dst_port text
);

create table probes (
    ID integer primary key autoincrement,
    connection integer,
    ts text,
    pkts text,
    bytes text,
	foreign key(connection) references connections(ID)
);

create table port_mapping (
    ID integer primary key autoincrement,
    addr text,
    port text,
    pid text
);