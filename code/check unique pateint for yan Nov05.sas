libname sasout "C:\work\working materials\Belgium\data\sasdata";
libname sasin "C:\work\working materials\Belgium\data\from_Alex";
%let dataset=statins;
data &dataset.;
set raw_data.&dataset.;
trans_date= input(put(transactiondate, 8.), yymmdd8.);
run;


proc sql;
create table &dataset._pat_indexdate as
select p_id, min(transactiondate) as index_date, market
from sasin.&dataset.
where market=1
group by p_id
;
quit;
/*67911*/

proc sql;
create table merged as
select a.p_id, a.trans_date, b.index_date, a.market
from &dataset. a left join &dataset._pat_indexdate b
on a.p_id=b.p_id;
quit;

proc sql;
delete from merged
where market=1 and trans_date<index_date;quit;

proc sql;
select count(distinct p_id) from merged
where trans_date<index_date;
quit;


