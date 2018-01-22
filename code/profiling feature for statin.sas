

/*
SAS Programmer: Yan Xue
First Edit Date: 01SEP2015
*/

/*statin*/
/*index time period is -- */
libname indata  "C:\work\working materials\Belgium\data\pre_data"; 

data indata.Statins;
set statins;
run;
data statins;
set indata.Statins;

trans_date= input(put(transactiondate, 8.), yymmdd8.);
format trans_date date9.;

Mol_Ticagrelor=0;
Mol_Prasugrel=0;
Mol_Clopidogrel=0;
Mol_Ticlodipin=0;

if index(lowcase(mol1),"ticagrelor") then Mol_Ticagrelor=1;
if index(lowcase(mol1),"prasugrel") then Mol_Prasugrel=1; 
if index(lowcase(mol1),"clopidogrel") then Mol_Clopidogrel=1; 
/*if index(lowcase(mol1),"ticlodipin") then Mol_Ticlodipin=1; */
if index(lowcase(mol1),"ticlopidin") then Mol_Ticlodipin=1;  /*changed from  to "ticlodipin" to "ticlopidin"*/

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


run;

/*QC1*/
proc sql;
create table check_dates as
select distinct trans_date from statins;quit;
/*End of QC1*/

/*filter out patients with statin script before the index period*/
proc sql;
create table false_new_statin_pats as
select distinct p_id from
(
select p_id, trans_date from statins
where trans_date<"01Jul2012"d and market=1
);
quit;

/*records for the patients prior to index period-- */

proc sql;
create table cleaned_cohort_pat_data as
select * from Statins
where p_id not in (select p_id from false_new_statin_pats);quit;
proc sql;select count(distinct p_id) from cleaned_cohort_pat_data;quit; /*55289 patients*/

/*find index date for each patient*/
proc sql;
create table statin_pat_indexdate as
select p_id, min(trans_date) as index_date
from cleaned_cohort_pat_data
where market=1
group by p_id
;
quit;
/*55289 patients*/
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
/*
min-- 01JUL2012
max--29JUN2013
*/
/*End of QC2*/


/*merge in index date for each patient*/
proc sql;
create table cleaned_cohort_pat_index as
select a.*, b.index_date
from cleaned_cohort_pat_data a left join statin_pat_indexdate b
on a.p_id=b.p_id;quit;

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
max(Mol_Ticagrelor) as Mol_Ticagrelor,
max(Mol_Prasugrel) as Mol_Prasugrel,
max(Mol_Clopidogrel) as Mol_Clopidogrel,
max(Mol_Ticlodipin) as Mol_Ticlodipin,
max(ace_inhibitors) as ace_inhibitors,
max(beta_blockers) as beta_blockers,
max(diabetes_insulin) as diabetes_insulin,
max(diabetes_other) as diabetes_other
from pat_records_pre_index
group by p_id
order by p_id
;quit;

proc sql;
select sum(Mol_Ticagrelor) as Mol_Ticagrelor,
sum(Mol_Prasugrel) as Mol_Prasugrel,
sum(Mol_Clopidogrel) as Mol_Clopidogrel,
sum(Mol_Ticlodipin) as Mol_Ticlodipin,
sum(ace_inhibitors) as ace_inhibitors,
sum(beta_blockers) as beta_blockers,
sum(diabetes_insulin) as diabetes_insulin,
sum(diabetes_other) as diabetes_other
from profiling_features;
quit;

proc sql;
select count(distinct p_id) into:total_valid_statin_pats from cleaned_cohort_pat_index;quit;
proc sql;
select sum(Mol_Ticagrelor)/&total_valid_statin_pats. as Mol_Ticagrelor,
sum(Mol_Prasugrel)/&total_valid_statin_pats. as Mol_Prasugrel,
sum(Mol_Clopidogrel)/&total_valid_statin_pats. as Mol_Clopidogrel,
sum(Mol_Ticlodipin)/&total_valid_statin_pats. as Mol_Ticlodipin,
sum(ace_inhibitors)/&total_valid_statin_pats. as ace_inhibitors,
sum(beta_blockers)/&total_valid_statin_pats. as beta_blockers,
sum(diabetes_insulin)/&total_valid_statin_pats. as diabetes_insulin,
sum(diabetes_other)/&total_valid_statin_pats. as diabetes_other
from profiling_features;
quit;

/*additional patient features to be added...*/




