

/*
SAS Programmer: Yan Xue
First Edit Date: 01SEP2015
Second Edit Date: 02SEP2015
Third Edit Date: 03SEP2015
*/

/*statin*/
/*index time period is -- */
libname indata "D:\Project Files\Belgium Predictive Modeling\newdata Aug28";  

%let trx=trx;


/*clean data by delete duplicated records*/
proc sql;
create table statins_dedup as
select distinct * from indata.Statins;quit; /*8656005*/
proc sql;
select count(*) from indata.Statins;quit; /*9117942*/
proc sql;
select count(distinct p_id) from indata.Statins;quit;  /*67911  patients*/


data statins;
set statins_dedup;

trans_date= input(put(transactiondate, 8.), yymmdd8.);
format trans_date date9.;

Mol_Ticagrelor=0;
Mol_Prasugrel=0;
Mol_Clopidogrel=0;
Mol_Ticlodipin=0;
oral_anti_platelet=0;

if index(lowcase(mol1),"ticagrelor") then Mol_Ticagrelor=1;
if index(lowcase(mol1),"prasugrel") then Mol_Prasugrel=1; 
if index(lowcase(mol1),"clopidogrel") then Mol_Clopidogrel=1; 
/*if index(lowcase(mol1),"ticlodipin") then Mol_Ticlodipin=1; */
if index(lowcase(mol1),"ticlopidin") then Mol_Ticlodipin=1;  /*changed from  to "ticlodipin" to "ticlopidin"*/

if Mol_Ticagrelor=1 or Mol_Prasugrel=1 or Mol_Clopidogrel=1 or Mol_Ticlodipin=1 then oral_anti_platelet=1;



ace_inhibitors=0;
beta_blockers=0;
diabetes_insulin=0;
diabetes_other=0;


if atc^="" then do;
	/*ACE inhibitors	*/
	if substr(trim(left(atc)),1,4) in ("C09A", "C09B") then ace_inhibitors=1;

	/*Beta blockers*/
	if substr(trim(left(atc)),1,4) in ("C07A", "C07B") then beta_blockers=1;

	/*Diabetes products (Insulin)*/
	if substr(trim(left(atc)),1,4)="A10C" then diabetes_insulin=1;

	/*Diabetes products (Other)	*/
	if substr(trim(left(atc)),1,4) in ("A10H", "A10J", "A10K", "A10L", "A10M" , "A10N", "A10P", "A10S") then diabetes_other=1;

end;

if  oral_anti_platelet=0 and ace_inhibitors=0 and beta_blockers=0 and diabetes_insulin=0 and diabetes_other=0 and market=0 then other_drugs=1;
else other_drugs=0;



/*cost and trx*/
/*cost=rsp*units;*/
trx=1;   *!!!!may need to change!!!!;

OAP_cost=0;
ace_inhibitors_cost=0;
beta_blockers_cost=0;
diabetes_insulin_cost=0;
diabetes_other_cost=0;
other_drugs_cost=0;

OAP_Trx=0;
ace_inhibitors_Trx=0;
beta_blockers_Trx=0;
diabetes_insulin_Trx=0;
diabetes_other_Trx=0;
other_drugs_Trx=0;

if oral_anti_platelet=1 then do; 
	OAP_cost=rsp*units;
	OAP_Trx=&trx.;
end;
if ace_inhibitors=1 then do; 
	ace_inhibitors_cost=rsp*units;
	ace_inhibitors_Trx=&trx.;
end;
if beta_blockers=1 then do;
	beta_blockers_cost=rsp*units;
	beta_blockers_Trx=&trx.;
end;
if diabetes_insulin=1 then do;
	diabetes_insulin_cost=rsp*units;
	diabetes_insulin_Trx=&trx.;
end;
if diabetes_other=1 then do; 
	diabetes_other_cost=rsp*units;
	diabetes_other_Trx=&trx.;
end;
if other_drugs=1 then do; 
	other_drugs_cost=rsp*units;
	other_drugs_Trx=&trx.;
end;


run;

/*further QC*/
data QC_statins;
set statins;
QC_sum=oral_anti_platelet + ace_inhibitors + beta_blockers + diabetes_insulin + diabetes_other + other_drugs+market;
run;
proc sql;
select distinct QC_sum from QC_statins;quit;


/*QC1*/
proc sql;
create table check_dates as
select distinct trans_date from statins;quit;

proc sql;
select count(distinct p_id) from indata.Statins;quit;  /*total number is 67911 patientes in the raw data*/
/*End of QC1*/


/* only delete those records with statins prescription before index period*/
proc sql;
delete from statins
where market=1 and trans_date<"01Jul2012"d;
quit;
/*22364 rows were deleted from WORK.STATINS*/



/*find index date for each patient*/
proc sql;
create table statin_pat_indexdate as
select p_id, min(trans_date) as index_date
from statins
where market=1
group by p_id
;
quit;
/*67911  patients*/
/*check dates*/



/*QC2*/
proc sql;
create table check_index_date_range as
select min(index_date) as min_indexdate, max(index_date) as max_indexdate from statin_pat_indexdate;quit;
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


/*merge in index date for each patient*/
proc sql;
create table cleaned_cohort_pat_index as
select a.*, b.index_date
from statins a left join statin_pat_indexdate b
on a.p_id=b.p_id;quit;

/*physician specialty on index date*/
/*proc sql;*/
/*create table index_phys_spec as*/
/*select a.*, b.index_date*/
/*from cleaned_cohort_pat_index a inner join statin_pat_indexdate b*/
/*on a.p_id=b.p_id and a.trans_date=b.index_date*/
/*where market=1;*/
/*quit;*/

proc sql;
create table index_phys_spec as
select p_id,  transactiondate,  doc_speciality,  mol1
from cleaned_cohort_pat_index
where market=1 and trans_date=index_date;quit;




/*--dedup by patient id and index date (index date=transaction date in this table)-*/
proc sort data=index_phys_spec out=index_phys_spec_dedup(keep=p_id  transactiondate  doc_speciality  mol1) nodupkey;
by p_id  transactiondate;run;
proc sql;
select distinct mol1 from index_phys_spec_dedup;
select distinct doc_speciality from index_phys_spec_dedup;quit;


/*QC3*/
proc sql;
select count(*) from cleaned_cohort_pat_index
where index_date=.;quit;
/*it must be zero, otherwise something must be wrong*/
/*End of QC3*/


data pat_records_pre_index  QC_invalid; /*if QC_invalid contains any record, please stop and double check*/
set cleaned_cohort_pat_index;

format index_date date9.;
 
if trans_date < index_date;

if  market=0 then output pat_records_pre_index;
else output QC_invalid;

run;


/*features pre-index*/
proc sql;
create table profiling_features as
select p_id,  

max(oral_anti_platelet) as pre_OAP,
max(ace_inhibitors) as pre_ace_inhibitors,
max(beta_blockers) as pre_beta_blockers,
max(diabetes_insulin) as pre_diabetes_insulin,
max(diabetes_other) as pre_diabetes_other,
max(other_drugs) as pre_other_drugs,

sum(OAP_cost) as pre_OAP_cost,
sum(ace_inhibitors_cost) as pre_ace_inhibitors_cost,
sum(beta_blockers_cost) as pre_beta_blockers_cost,
sum(diabetes_insulin_cost) as pre_diabetes_insulin_cost,
sum(diabetes_other_cost) as pre_diabetes_other_cost,
sum(other_drugs_cost) as pre_other_drugs_cost,

sum(OAP_Trx) as pre_OAP_Trx,
sum(ace_inhibitors_Trx) as pre_ace_inhibitors_Trx,
sum(beta_blockers_Trx) as pre_beta_blockers_Trx,
sum(diabetes_insulin_Trx) as pre_diabetes_insulin_Trx,
sum(diabetes_other_Trx) as pre_diabetes_other_Trx,
sum(other_drugs_Trx) as pre_other_drugs_Trx

from pat_records_pre_index
group by p_id
order by p_id
;quit;

/*merge info together */

proc sql;
create table pat_demo as 
select distinct p_id,  P_gender from statins;quit;


/*data pat_demo_duplicated;*/
/*set pat_demo;*/
/*by p_id ;*/
/*if first.p_id^=last.p_id;*/
/*run;*/
/*proc sql;*/
/*select count(distinct p_id) from pat_demo_duplicated;quit;*/
/*6 patients with 2 gender*/

/*randomly select  gender for the 6 patients*/
proc sort data=pat_demo nodupkey;
by p_id;
run;

/*QC*/
data pat_missing_gender;
set pat_demo;
if P_gender=.;
run;

proc sql;
create table check_missing_gender as
select p_id, p_gender 
from pat_demo
where p_id in (select p_id from pat_missing_gender);quit;
proc sql;
select distinct p_gender from check_missing_gender;quit; 
/*they are indeed missing*/


proc sql;
create table statins_pre_model0 as
select a.p_id, a.p_gender, 
b.doc_speciality as index_doc_spec,
b.mol1 as index_statin_molecule,

c.pre_OAP,
c.pre_ace_inhibitors,
c.pre_beta_blockers,
c.pre_diabetes_insulin,
c.pre_diabetes_other,
c.pre_other_drugs,

c.pre_OAP_cost,
c.pre_ace_inhibitors_cost,
c.pre_beta_blockers_cost,
c.pre_diabetes_insulin_cost,
c.pre_diabetes_other_cost,
c.pre_other_drugs_cost,

c.pre_OAP_Trx,
c.pre_ace_inhibitors_Trx,
c.pre_beta_blockers_Trx,
c.pre_diabetes_insulin_Trx,
c.pre_diabetes_other_Trx,
c.pre_other_drugs_Trx

from pat_demo a left join index_phys_spec_dedup b
on a.p_id=b.p_id
left join profiling_features c
on a.p_id=c.p_id;
quit;


data statins_pre_model0b;
set statins_pre_model0;

if p_gender=. then p_gender=999;
array profile pre_OAP--pre_other_drugs_Trx;

do over profile;
	if profile=. then  profile=0;
end;

run;
/*proc sql;*/
/*select distinct p_gender from indata.statins_pre_model;*/
/*select distinct index_doc_spec from indata.statins_pre_model;*/
/*select distinct index_statin_molecule from  indata.statins_pre_model;*/
/*select distinct pre_OAP from indata.statins_pre_model;*/
/**/
/**/


/*QC*/
/*proc sql;*/
/*select sum(pre_OAP) as pre_OAP,*/
/*sum(pre_ace_inhibitors) as pre_ace_inhibitors,*/
/*sum(pre_beta_blockers) as pre_beta_blockers,*/
/*sum(pre_diabetes_insulin) as pre_diabetes_insulin,*/
/*sum(pre_diabetes_other) as pre_diabetes_other,*/
/*sum(pre_other_drugs) as pre_other_drugs*/
/*from profiling_features;*/
/*quit;*/

/*proc sql;*/
/*select count(distinct p_id) into:total_valid_statin_pats from statins;quit;*/
/*%put &total_valid_statin_pats.;*/
/*proc sql;*/
/*select sum(pre_OAP)/&total_valid_statin_pats. as pre_OAP,*/
/*sum(pre_ace_inhibitors)/&total_valid_statin_pats. as pre_ace_inhibitors,*/
/*sum(pre_beta_blockers)/&total_valid_statin_pats. as pre_beta_blockers,*/
/*sum(pre_diabetes_insulin)/&total_valid_statin_pats. as pre_diabetes_insulin,*/
/*sum(pre_diabetes_other)/&total_valid_statin_pats. as pre_diabetes_other,*/
/*sum(pre_other_drugs)/&total_valid_statin_pats. as pre_other_drugs*/
/*from profiling_features;*/
/*quit;*/

/*additional patient features to be added...*/

/*check response rate by different number of months as threshold*/
/*%let nmonths=10;*/
proc sql;
create table pat_capdays as
select distinct p_id, Capped_Days_treatment
from statins;quit;

/*proc sql;*/
/*select sum(persistence)/count(*) as persistence_rate from*/
/*(select p_id, */
/*case when Capped_Days_treatment/30>=&nmonths. then 1 else 0 end as persistence*/
/*from pat_capdays);quit;*/

/*
3--0.792817
6--0.388052 
8--0.289909
10-0.237576 

*/

proc sql;
create table p_persistence as
select p_id,
case when  Capped_Days_treatment/30 >= 3 then 1 else 0 end as persistence_3m,
case when Capped_Days_treatment/30 >= 6 then 1 else 0 end as persistence_6m,
case when Capped_Days_treatment/30 >= 9 then 1 else 0 end as persistence_9m
from pat_capdays;quit;

proc sql;
select sum(persistence_3m)/count(*), sum(persistence_6m)/count(*), sum(persistence_9m)/count(*) from p_persistence;quit;

/*final table*/
proc sql;
create table indata.statins_pre_model as
select a.*, b.persistence_3m, b.persistence_6m, b.persistence_9m
from statins_pre_model0b a left join p_persistence b
on a.p_id = b.p_id;quit;


proc export data=indata.statins_pre_model
outfile="D:\Project Files\Belgium Predictive Modeling\newdata Aug28\statins_pre_model.csv"
dbms=csv replace;
run;





proc corr data=indata.statins_pre_model;
var  
pre_OAP
 pre_ace_inhibitors
 pre_beta_blockers
 pre_diabetes_insulin
 pre_diabetes_other
 pre_other_drugs

 pre_OAP_cost
 pre_ace_inhibitors_cost
 pre_beta_blockers_cost
 pre_diabetes_insulin_cost
 pre_diabetes_other_cost
 pre_other_drugs_cost

 pre_OAP_Trx
 pre_ace_inhibitors_Trx
 pre_beta_blockers_Trx
 pre_diabetes_insulin_Trx
 pre_diabetes_other_Trx
 pre_other_drugs_Trx;
run;
 
proc corr data=indata.statins_pre_model;
var  
pre_OAP
 pre_ace_inhibitors
 pre_beta_blockers
 pre_diabetes_insulin
 pre_diabetes_other
 pre_other_drugs

 pre_OAP_cost
 pre_ace_inhibitors_cost
 pre_beta_blockers_cost
 pre_diabetes_insulin_cost
 pre_diabetes_other_cost
 pre_other_drugs_cost

 pre_OAP_Trx
 pre_ace_inhibitors_Trx
 pre_beta_blockers_Trx
 pre_diabetes_insulin_Trx
 pre_diabetes_other_Trx
 pre_other_drugs_Trx;
 with persistence_6m
persistence_3m
persistence_9m
;
run;

