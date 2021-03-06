drop table if exists connections;
drop table if exists probes;
drop table if exists port_mapping;
drop table if exists supervisor;
drop table if exists topology;
drop table if exists worker;
drop table if exists component;
drop table if exists executor;

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
	foreign key(connection) references connections(ID),
    constraint unique_connection_time unique (connection, ts)
);

create table port_mapping (
    ID integer primary key autoincrement,
    addr text,
    port text,
    pid text
);

create table supervisor (
    host text primary key,
    uptime real
);

create table topology (
    ID text primary key,
    name text
);

create table worker (
    host text,
    port text,
    topoID text,
    last_seen text,
    primary key(host, port, topoID),
    foreign key(topoID) references topology(ID)
);

create table component(
    ID text primary key,
    topoID text,
    foreign key(topoID) references topology(ID)
);

create table executor (
    executor text,
    time text,
    host text,
    port text,
    component text,
    foreign key (host,port) references worker(host, port),
    foreign key (component) references component(ID),
    primary key (executor, time)
);