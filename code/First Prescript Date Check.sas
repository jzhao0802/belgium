/*check the statin data patients time window*/

%let inpath = "C:\work\working materials\Belgium";
%let outpath = "C:\work\working materials\Belgium\temp";

libname sasdata "C:\work\working materials\Belgium";
proc import out=statins
datafile="C:\work\working materials\Belgium\statins.csv"
dbms=csv replace;
run;

proc import out=sasdata.statins_pat
datafile="C:\work\working materials\Belgium\statins_pat.csv"
dbms=csv replace;
run;

proc sql;
create table temp2_2 as
	select distinct c.p_id, min(transactiondate) as min_date from
	(select distinct  a.p_id, b.transactiondate
	from sasdata.statins_pat a left join statins b
	on a.p_id=b.p_id where b.market=1) as c
	group by c.p_id;
quit;

proc sql;
create table unNew_pt as
	select a.*
	from temp2 as a
	where a.min_date < 20120701;
quit; 

proc sql;
create table unNew_statins_pt as
	select b.*, a.min_date as first_trans_date
	from unNew_pt a left join sasdata.statins_pat b
	on a.p_id = b.p_id;
quit;

proc export data=unNew_statins_pt
outfile="C:\work\working materials\Belgium\unNew_statins_pt.csv"
dbms=csv replace;

run;

proc sql;
select max(a.min_date) as check from unNew_pt a;
quit;

/*check the market for statins_pat*/
proc sort data= statins out=statins_byPatMkt; by p_id market;run;
proc sql;
create table pat_Mkt0 as
	select p_id
	from
	(
	select p_id, max(market) as maxMkt
	from statins
	group by p_id
	)
	where maxMkt=0;
quit;

proc sql;
create table pat_outData as
	select p_id, min(transactiondate)as min_date
	from statins
	group by p_id
	having min(transactiondate)<20120701;
quit;

proc sql;
create table rec_outData as
	select p_id, transactiondate, market
	from statins
	where (transactiondate > 20130631 or transactiondate < 20120701) and market=1;
quit;
