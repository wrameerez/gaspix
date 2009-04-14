drop table PointData;
drop table PointDataType;
drop table AtmospherePoint;
drop table Trajectory;

create table Trajectory (
	id          bigserial 	primary key,
	startDate	timestamp 	not null
);
	
create table AtmospherePoint (
	id          bigserial 	primary key,
	trajectory	bigint	 	references Trajectory not null,
	moment		timestamp	not null,
	longitude	real		not null,
	latitude	real		not null,
	height		real		not null
);

create table PointDataType (
	id          smallint 	primary key,
	name		varchar(25)	not null
);

create table PointData (
    id          bigserial   primary key,
	point		bigint	 	references AtmospherePoint,
	pointType	smallint	references PointDataType,
	value		real		not null
);

create unique index PointDataKey on PointData( point, pointType );

insert into PointDataType values ( 1, 'tri' );
insert into PointDataType values ( 2, 'hmixi' );
insert into PointDataType values ( 3, 'topo' );
insert into PointDataType values ( 4, 'pvi' );
insert into PointDataType values ( 5, 'tti' );
insert into PointDataType values ( 6, 'qvi' );
insert into PointDataType values ( 7, 'rhoi' );

