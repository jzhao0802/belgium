data index_date;
set sasout.Unique_patient_index;
trans_date= input(put(index_date,yymmddn8.),8.);
run;

data &dataset.;
set sasin.&dataset.;
run;

data &dataset._t1;
set &dataset.;
	trans_date= input(put(transactiondate, 8.), yymmdd8.);
	format trans_date date9.;
/*keep p_id trans_date transactiondate market atc;*/
run;

proc sql;
select count(distinct p_id) from &dataset._t1;
quit;

proc sql;
create table &dataset._pat_indexdate_v2 as
select p_id, min(trans_date) as index_date
/*,input(put(trans_date,yymmddn8.),8.) as index_date2 */
from &dataset._t1
where market=1 and '01Jul2012'd <=trans_date <='30Jun2013'd
group by p_id
;
quit;
/*67911  patients*/
/*check dates*/

proc sql;
create table sasout.Unique_patient_index_packsize_v2 as
select a.p_id, b.index_date, a.packsize_start
from sasout.Unique_patient_index_packsize a left join &dataset._pat_indexdate_v2 b
on a.p_id=b.p_id;
quit;

proc sql;
select input(put(min(index_date), yymmddn8.), 8.) as min,
	input(put(max(index_date), yymmddn8.), 8.) as max
from sasout.Unique_patient_index_packsize_v2;
quit;
/*20120701  20130629*/


proc sql;
create table remove_pat as
select p_id, trans_date, market
from &dataset._t1
where p_id not in (select distinct p_id from &dataset._pat_indexdate_v2);
quit;

data sasout.&dataset._pat_indexdate_v2;
set &dataset._pat_indexdate_v2;
index_date2=input(put(index_date,yymmddn8.),8.);
run;

/*QC2*/
proc sql;
create table check_index_date_range as
select min(index_date) as min_indexdate, max(index_date) as max_indexdate from &dataset._pat_indexdate;quit;
data check_index_date_range;
set check_index_date_range;
format min_indexdate date9.
max_indexdate date9.;
run;
proc print;run;
/*
min-- 01JUL2012
max--29JUN2013
*/
/*End of QC2*/

proc sql;
create table &dataset._t2 as
select a.*, b.index_date
from &dataset._t1 a left join &dataset._pat_indexdate b
on a.p_id=b.p_id;quit;

/*QC3*/
proc sql;
select count(*) from &dataset._t2
where index_date=.;quit;
/*it must be zero, otherwise something must be wrong*/
/*End of QC3*/


data sasin.&dataset._market sasin.&dataset._pre_index QC_invalid; /*if QC_invalid contains any record, please stop and double check*/
	set &dataset._t2;

	format index_date date9.;

	output sasin.&dataset._market ; 

	if trans_date < index_date;

	if  market=0 then output sasin.&dataset._pre_index;

	else output QC_invalid;
run;

data &dataset._market;
set sasdata.&dataset._market;
data &dataset._pre_index;
set sasdata.&dataset._pre_index;
run;

proc sql;
create table index_date as
select p_id, min(trans_date) as min_trans_date
from sasin.&dataset._market
group by p_id;
quit;

proc sql;
create table index_date_1 as
select p_id, min_trans_date, input(put(min_trans_date,yymmddn8.),8.) as index_date
from index_date;
quit;

/*QC*/
proc sql;
select count( distinct p_id) as num_pat from &dataset._pre_index
quit;

proc sql;
create table &dataset._pat_indexdate_v2 as
select p_id, min(trans_date) as index_date
from sasin.&dataset._pre_index
/*where market=1*/
group by p_id
;
quit;



proc sql;
create table d1 as
select p_id, min(transactiondate) as min_date 
from &dataset.
where transactiondate >= 20120731
group by p_id;
quit;
/*67857*/

/*check the index date*/

proc sql;
create table temp1 as
select p_id, trans_date, atc
from &dataset._t1
where p_id in (select distinct p_id from sasout.temp_check01_unequal);
quit;

proc sql;
create table temp2 as
select a.p_id, a.regen_index_date, b.trans_date, b.atc
from sasout.temp_check01_unequal a left join temp1 b
on a.p_id=b.p_id and a.regen_index_date = b.trans_date;
quit;

proc sql;
create table temp3 as
select mean(p_id) as p_id, mean(regen_index_date) as regen_index_date
from temp2
group by p_id, regen_index_date;
quit;
proc sql;
create table sasout.temp4 as
select a.*, b.*
from temp3 a left join &dataset._t1 b
on a.p_id=b.p_id and a.regen_index_date=b.trans_date;
quit;

proc sql;
create table temp5 as
select distinct p_id, regen_index_date, atc
from sasout.temp4
where atc="C10A9";
quit;
/*7*/

proc sql;
create table temp6 as
select p_id, regen_index_date, atc
from temp4
where p_id not in (select distinct p_id from temp5);
quit;

proc sql;
select count(distinct p_id) from temp4;
quit;
/*38*/
