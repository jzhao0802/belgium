libname sasdata "C:\work\working materials\Belgium\data\from_Alex";
proc import out=Icome_data
datafile='C:\work\working materials\Belgium\data\toBeMergedData\Icomed data at physician level.csv'
dbms=csv replace;
GUESSINGROWS=63387;
run;

proc sql;
create table merge1 as
	select a.*, b.*
	from sasdata.statins a left join Icome_data b
	on a.shortcode=b.short_id;
quit;

proc sql;
select count(*) into :cnt_nonmiss_shortcode
from merge1
where short_id ^= "";
quit;

/*qc1*/
proc sql;
select count(*) from 
	(select short_id
	from merge1
	where short_id ^="")
where short_id = ""; 
quit;

proc sql;
create table merge_valid as
select *
from merge1
where cats(c001, c003, c004) ^= "";
quit;

proc sql;
select count(distinct shortcode)
from merge_valid;
quit;

proc sql;
select count(*) into :cnt_have_vale_c001_coo3_coo4
from merge1
where cats(c001, c003, c004) ^= "";
quit;
/*check2*/
proc sql;
select count(*) from
	(select c001, c003, c004 from merge1
	where cats(c001, c003, c004) ^= "")
where c001="" and c003="" and c004="";
quit;

proc contents data = merge1
out = vars(keep = varnum name)
noprint;
run; 

proc sql noprint ;
select name into :orderedvars3 separated by ' '
from vars
order by varnum;
quit; 
/*proc sql;*/
/*select count(*) into :cnt_notAllMissing_rcd*/
/*from merge1*/
/*where cat(&orderedvars3) ^= "";*/
/*quit;*/

data check_miss;
set merge1;
miss_n = cmiss(of &orderedvars3.);
run;

data check_miss2;
set merge1;
miss_n = cmiss(of c001--c079);
run;

proc sql;
select count(*) into :cnt_nonMiss_rcd
from check_miss
where miss_n = 0;
quit;

proc export data=merge_valid
outfile="C:\work\working materials\Belgium\data\preModelData\Oct09\merge_valid.csv"
dbms=CSV replace;
run;

%put &cnt_nonmiss_shortcode.;
%put &cnt_have_vale_c001_coo3_coo4;
%put &cnt_nonMiss_rcd;

%let match_rate=(&cnt_nonmiss_shortcode.)/9117942;
%put &match_rate.;
%put &orderedvars3;

