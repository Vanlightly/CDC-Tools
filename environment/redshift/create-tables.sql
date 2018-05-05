create table "person"
(
	personid int not null distkey sortkey,
	firstname nvarchar(200) not null,
	surname nvarchar(200) not null,
	dateofbirth date not null,
    primary key(personid)
);

create table personaddress
(
	addressid int not null,
	addressline1 nvarchar(1000) not null,
	city nvarchar(100) not null,
	postalcode nvarchar(20) not null,
	country nvarchar(100) not null,
	personid int not null distkey sortkey,
	primary key(addressid)
);