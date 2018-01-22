/*import data*/

libname raw_data "C:\work\working materials\Belgium\data\from_Alex"; /*path for record data from Alex*/
libname sasdata "C:\work\working materials\Belgium\data\sasdata";


/* IMPORT SOCIODEMOGRAPHICS */

PROC IMPORT OUT= sociodemo
            DATAFILE= "C:\work\working materials\Belgium\data\toBeMergedData\demo.csv" 
            DBMS=CSV REPLACE;
			GUESSINGROWS=590;
RUN;


/*IMPORT SALES (PER ATC) PER PHYSICIAN FROM XPONENT */

PROC IMPORT OUT= Xpo_Sales
            DATAFILE= "C:\work\working materials\Belgium\data\toBeMergedData\PRED_PER_extract_v2 - AStaus.csv" 
            DBMS=CSV REPLACE;
			GUESSINGROWS=500;
RUN;


/* IMPORT ONEKEY DATA PER PHYSICIAN */

PROC IMPORT OUT= Onekey 
            DATAFILE= "C:\work\working materials\Belgium\data\toBeMergedData\Icomed data at physician level.csv" 
            DBMS=CSV REPLACE;
			GUESSINGROWS=63365;
RUN;


/* IMPORT GRADUATION YEAR PER GP */

PROC IMPORT OUT= GP_graduated
            DATAFILE= "C:\work\working materials\Belgium\data\toBeMergedData\Graduation year GPs.csv" 
            DBMS=CSV REPLACE;
RUN;
/*IMPORT DATA DICTIONARY */

PROC IMPORT OUT= dictionary_statins
            DATAFILE= "C:\work\working materials\Belgium\data\toBeMergedData\Statins mkt def.csv" 
            DBMS=CSV REPLACE;
			GUESSINGROWS=213;
RUN;



/* SETTING INDEX DATE AND FLAG OTHER MARKET PRODUCTS FROM FLORE (DIABETES, ACE, ETC.) */


/*************************************************************/
/*** 3.1 CREATE NEW DUMMY VARIABLES BASED ON CODE FROM YAN ***/
/*************************************************************/
%let trx=1;
%let dataset=statins;
%macro yan(dataset, trx) ; 
data &dataset.;
set raw_data.&dataset.;
run;
/* CODE FROM YAN */

data &dataset._t1;
	set &dataset.;

	trans_date= input(put(transactiondate, 8.), yymmdd8.);
	format trans_date date9.;

	Oral_anti_platelet = 0 ;

	if index(lowcase(mol1),"ticagrelor") then Oral_anti_platelet=1;
	if index(lowcase(mol1),"prasugrel") then Oral_anti_platelet=1; 
	if index(lowcase(mol1),"clopidogrel") then Oral_anti_platelet=1; 
	if index(lowcase(mol1),"ticlopidin") then Oral_anti_platelet=1;  /*changed from  to "ticlodipin" to "ticlopidin"*/

	ace_inhibitors=0;
	beta_blockers=0;
	diabetes_insulin=0;
	diabetes_other=0;


	if atc NE "" then do;
		/*ACE inhibitors	*/
		if substr(trim(left(atc)),1,4) in ("C09A", "C09B") then ace_inhibitors=1;

		/*Beta blockers*/
		if substr(trim(left(atc)),1,4) in ("C07A", "C07B") then beta_blockers=1;

		/*Diabetes products (Insulin)*/
		if substr(trim(left(atc)),1,4)="A10C" then diabetes_insulin=1;

		/*Diabetes products (Other)	*/
		if substr(trim(left(atc)),1,4) in ("A10H", "A10J", "A10K", "A10L", "A10M" , "A10N", "A10P", "A10S") then diabetes_other=1;
	if  oral_anti_platelet=0 and ace_inhibitors=0 and beta_blockers=0 and diabetes_insulin=0 and diabetes_other=0 and market=0 then other_drugs=1;
	else other_drugs=0;

	end;


		
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


/*	%if &dataset. = icslaba %then %do;*/
/*		if trans_date <'01Jul2013'd and market = 1 then delete ; */
/*	%end;*/
/**/
/*	%if &dataset. = statins %then %do;*/
/*		if trans_date <'01Jul2012'd and market = 1 then delete ; */
/*	%end;*/
		if trans_date <'01Jul2012'd and market = 1 then delete ; 

run;

/*further QC*/
data QC_&dataset.;
set &dataset._t1;
QC_sum=oral_anti_platelet + ace_inhibitors + beta_blockers + diabetes_insulin + diabetes_other + other_drugs+market;
run;
proc sql;
select distinct QC_sum from QC_&dataset.;quit;



/*QC1*/
proc sql;
create table check_dates as
select distinct trans_date from &dataset._t1;quit;

proc sql;
select count(distinct p_id) from &dataset._t1;quit;  /*total number is 67911 patientes in the raw data*/
/*End of QC1*/


/*find index date for each patient*/
proc sql;
create table &dataset._pat_indexdate as
select p_id, min(trans_date) as index_date
from &dataset._t1
where market=1
group by p_id
;
quit;
/*67911  patients*/
/*check dates*/



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



/*merge in index date for each patient*/
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


data sasdata.&dataset._market sasdata.&dataset._pre_index QC_invalid; /*if QC_invalid contains any record, please stop and double check*/
	set &dataset._t2;

	format index_date date9.;

	output sasdata.&dataset._market ; 

	if trans_date < index_date;

	if  market=0 then output sasdata.&dataset._pre_index;

	else output QC_invalid;
run;

data &dataset._market;
set sasdata.&dataset._market;
data &dataset._pre_index;
set sasdata.&dataset._pre_index;
run;

/*QC*/
proc sql;
select count( distinct p_id) as num_pat from &dataset._pre_index
quit;
%mend;

%yan(statins)

/*create the speciatly at the very index data*/
proc sql;
create table &dataset._at_index as
select p_id, P_gender, doc_speciality, market
from statins_mark et
where index_date=trans_date and market=1;
quit;

%nominal_to_binary(sm_dataset=&dataset._at_index, sm_var=doc_speciality, sm_prefix=doc_speciality_);

proc summary data = &dataset._at_index nway;
class p_id ;
var  doc_speciality_11 -- doc_speciality_90 ;
output out = &dataset._at_index_uniquePat (drop=_type_ _freq_ ) max= /autoname ;
run;
/*67911*/

/*create the P_gender*/
proc sql;
create table &dataset._P_gender as
select distinct p_id,
case when P_gender = . then 999 else P_gender end as pat_gender
from statins_market
quit;

proc sql;
create table p_gender_dup as
select p_id, pat_gender, count(*) as dup_num from &dataset._P_gender
group by p_id
having count(*) > 1;
quit;

%nominal_to_binary(sm_dataset=&dataset._P_gender, sm_var=pat_gender, sm_prefix=pat_gender_);


proc summary data = &dataset._P_gender nway;
class p_id ;
var  pat_gender_0 -- pat_gender_999 ;
output out =&dataset._P_gender_max (drop=_type_ _freq_ ) max= /autoname ;
run;
/*67911*/

proc sql;
create table &dataset._P_gender_max_f as
select p_id,
case when pat_gender_999_Max=1 then 999 else pat_gender_0_Max end as pat_gender_0_Max,
case when pat_gender_999_Max=1 then 999 else pat_gender_1_Max end as pat_gender_1_Max
from &dataset._P_gender_max;
quit;

proc sql;
select * from &dataset._P_gender_max 
where pat_gender_999_Max=1;
quit;

proc sql;
select * from &dataset._P_gender_max_f
where p_id in (select p_id from &dataset._P_gender_max where pat_gender_999_Max=1);
quit;

proc sql;
select * from &dataset._P_gender_max_f
where pat_gender_1_Max=999;
quit;
/*merge the pat_gender and doc_speciality into pat_data*/
proc import out=sasdata.&dataset._pat_v2
datafile="C:\work\working materials\Belgium\data\preModelData\&dataset._pat_v2.csv"
dbms=CSV replace;
run;
proc sql;
create table sasdata.&dataset._pat_v3 as
select a.*, b.*, c.*
from sasdata.&dataset._pat_v2 a left join &dataset._P_gender_max_f b 
on a.p_id=b.p_id
left join &dataset._at_index_uniquePat c
on a.p_id=c.p_id;
quit;

proc sql;
create table missing_pat_gender as
select p_id, pat_gender_0_Max, pat_gender_1_Max from sasdata.&dataset._pat_v3
where p_id in (select p_id from &dataset._P_gender_max where pat_gender_999_Max=1);
quit;

proc sql;
select count(p_id) from &dataset._P_gender_max where pat_gender_999_Max=1;
quit;

proc sql;
select p_id, pat_gender_0_Max, pat_gender_1_Max from &dataset._P_gender_max_f
where p_id in (select p_id from sasdata.&dataset._pat_v3 where pat_gender_1_Max=999);
quit;

proc export data=sasdata.&dataset._pat_v3
outfile="C:\work\working materials\Belgium\data\preModelData\&dataset._pat_v3.csv"
dbms=CSV replace;
run;

/**************************************/
/*** 3.2 CREATE NEW DUMMY VARIABLES ***/
/**************************************/

/* SOME COUNT AND AVERAGE CALCULATIONS */
/* A macro for dumming coding */
%macro nominal_to_binary(
        sm_dataset= /* data set */, 
        sm_var= /* categorical variable */, 
        sm_prefix= /* prefix for dummy variables */);
 
/* Find the unique levels of the categorical variable */
proc sort data=&sm_dataset(keep=&sm_var) out=&sm_dataset._unique nodupkey;
    by &sm_var;
run;
 
data _null_;
    set &sm_dataset._unique end=end;
    /* Use CALL EXECUTE to dynamically create a macro that executes */
    /* after this DATA step finishes. The metaprogrammed macro */
    /* modifies the original data set. */
    if _N_ eq 1 then do;
        call execute("data &sm_dataset;");
        call execute("set &sm_dataset;");
        end;
    call execute(cat("length &sm_prefix", &sm_var," 3;")); /* use minimum storage */
    call execute(cats("&sm_prefix", &sm_var," = &sm_var = '", &sm_var,"';"));
    if end then call execute('run;');
run;
 
proc sql;
    /* Clean up */
    drop table &sm_dataset._unique;
quit;
%mend;



%macro pat_create(dataset) ;

/* CREATE DUMMIES OF ADDITIONAL MARKETS (ACE INHIBITORS, BETA BLOCKERS, DIABETES...) */

proc summary data = &dataset._pre_index nway;
class p_id ;
var  Oral_anti_platelet -- diabetes_other ;
output out = Pat_create_t0_&dataset. (drop=_type_ _freq_ ) max= /autoname ;
run;



/* WITHIN ~12 MONTHS BEFORE FIRST MARKET RX (INDEX DATE) */

/*%if &dataset. = icslaba %then %do;*/
/*proc sort data=&dataset. (keep=p_id transactiondate fcc market where=(market=1 and 20130701<=transactiondate<=20140630)) */
/*			out=temp01_cnt_time ; by p_id transactiondate ; run ; */
/*%end;*/
/**/
/*%if &dataset. = statins %then %do;*/
/*proc sort data=&dataset. (keep=p_id transactiondate fcc market where=(market=1 and 20120701<=transactiondate<=20130630)) */
/*			out=temp01_cnt_time ; by p_id transactiondate ; run ; */
/*%end;*/
/**/
/**/
/*proc sort data = temp01_cnt_time nodupkey out = temp02_cnt_time (rename=(transactiondate=FirstRx)) ; by p_id ; run ; */
/**/
/*data &dataset._pre_index ;*/
/*	merge temp02_cnt_time (keep=firstrx p_id) &dataset. /*(keep=p_id transactiondate fcc market)*/ ; */
/*	by p_id ; */
/*	if FirstRx - 10000 <= transactiondate <= FirstRx ; */
/*run; */
;


proc sql;
	create table pat_create_t1a_&dataset. as
	select
		 p_id
		,count(*) as cnt_tx_12m
		,count(distinct transactiondate) as cnt_tx_days_12m
		,count(distinct shortcode) as cnt_docs_12m
		,count(distinct doc_speciality) as cnt_specialties_12m
/*		,mean(input(doc_gender,8.)) - 1 as mean_doc_gender_12m*/
		,count(distinct ddms_pha) as cnt_pharmacies_12m
		,count(distinct fcc) as cnt_fcc_12m
		,count(distinct atc) as cnt_atc_12m
		,count(distinct otc) as cnt_otc_12m
		,mean(rsp) as mean_price_12m
		,sum(rsp*units) as sum_spending_12m

		/* ADDED */
/*		,mean(rx_id) as mean_rx_id_12m*/
		,mean(transactiontype) - 1 as mean_tx_type_12m
		,mean(dci_vos) as mean_dcivos_12m
		,mean(units) as mean_units_12m

		,mean(capped_days_treatment) as persistence
		
	from &dataset._pre_index
	group by p_id
	;
quit;


proc sql;
create table doc_gender_tb as
select doc_gender from &dataset._pre_index;
quit;
proc sql;
	create table pat_create_t1b_&dataset. as
	select
		 p_id
/*		,mean(input(doc_gender,8.)) - 1 as mean_doc_gender_12m*/
		,mean(doc_gender)-1 as mean_doc_gender_12m
	from
			(select p_id, transactiondate, shortcode, doc_gender
			,count(*)
			from &dataset._pre_index
			where doc_gender in (1,2)
			group by p_id, transactiondate, shortcode, doc_gender
		)
	group by p_id
	;	
quit;
/*66231 patient who have nonmissing doc_gender in all available records*/

data pat_create_t1_&dataset. ;
	merge pat_create_t1a_&dataset. pat_create_t1b_&dataset. ;
	by p_id ; 
run;



/* SAME AS ABOVE, BUT CALCULATED PER YEAR TO CHECK CORR FROM YEAR TO YEAR */
/*
%macro create(dataset,date1,date2) ;
proc sql;
	create table pat_create_&date2. as
	select
		 p_id
		,count(*) as cnt_tx_&date2.
		,count(distinct transactiondate) as cnt_tx_days_&date2.
		,count(distinct shortcode) as cnt_docs_&date2.
		,count(distinct doc_speciality) as cnt_specialties_&date2.
		,mean(input(doc_gender,8.)) as mean_doc_gender_&date2.
		,count(distinct ddms_pha) as cnt_pharmacies_&date2.
		,count(distinct fcc) as cnt_fcc_&date2.
		,count(distinct atc) as cnt_atc_&date2.
		,count(distinct otc) as cnt_otc_&date2.
		,mean(rsp*units) as mean_spending_&date2.
		,sum(rsp*units) as sum_spending_&date2.
		
	from &dataset. 
	where transactiondate >= &date1. and transactiondate <= &date2.
	group by p_id
	;
quit;
%mend;
%create(icslaba,20110501,20120431)
%create(icslaba,20120501,20130431)
%create(icslaba,20130501,20140431)
%create(icslaba,20140501,20150431)



data check_t1 ; 
	merge pat_create_20: ;
	by p_id;
run;

%macro corr(var);
proc corr data=check_t1;
var &var._20120431 &var._20130431 &var._20140431 &var._20150431 ;
run;
%mend;
%corr(cnt_tx) ;
%corr(cnt_tx_days) ;
%corr(cnt_docs) ;
%corr(cnt_specialties) ;
%corr(mean_doc_gender) ;
%corr(cnt_pharmacies) ;
%corr(cnt_fcc) ;
%corr(cnt_atc) ;
%corr(cnt_otc) ;
%corr(mean_spending) ;
%corr(sum_spending) ;

*/


/* --> RESULT: 
	- TX DAYS PER PATIENT HAVE ~80% CORRELATION FROM YEAR TO YEAR 
	- MOST OTHER VARIABLES HAVE AT LEAST ~50% CORRELATION AND ABOVE FROM YEAR TO YEAR 
*/


/* DOC SPEC (ALSO PER MARKET VISITS ONLY) */

data temp01_doc_spec ;
	set &dataset._pre_index (keep=p_id doc_speciality);

		spec11=(doc_speciality=11) ;
		spec21=(doc_speciality=21) ;
		spec22=(doc_speciality=22) ;
		spec23=(doc_speciality=23) ;
		spec24=(doc_speciality=24) ;
		spec25=(doc_speciality=25) ;
		spec26=(doc_speciality=26) ;
		spec27=(doc_speciality=27) ;
		spec28=(doc_speciality=28) ;
		spec29=(doc_speciality=29) ;

		spec31=(doc_speciality=31) ;
		spec32=(doc_speciality=32) ;
		spec33=(doc_speciality=33) ;
		spec34=(doc_speciality=34) ;

		spec41=(doc_speciality=41) ;
		spec42=(doc_speciality=42) ;
		spec43=(doc_speciality=43) ;
		spec44=(doc_speciality=44) ;
		spec45=(doc_speciality=45) ;
		spec46=(doc_speciality=46) ;
		spec47=(doc_speciality=47) ;
		spec48=(doc_speciality=48) ;
		spec49=(doc_speciality=49) ;

		spec50=(doc_speciality=50) ;
		spec60=(doc_speciality=60) ;
		spec90=(doc_speciality=90) ;
run;
proc sql;
select count(distinct p_id) from &dataset._pre_index;
quit;
/*67147 unique patient*/


proc summary data = temp01_doc_spec nway;
class p_id ;
var spec11 -- spec90 ;
output out = temp01_doc_spec (drop=_type_ _freq_) max= /autoname ;
run;


data temp02_doc_spec ;
	set &dataset._market (where=(market=1) keep=p_id doc_speciality market) ;

		market_spec11=(doc_speciality=11) ;
		market_spec21=(doc_speciality=21) ;
		market_spec22=(doc_speciality=22) ;
		market_spec23=(doc_speciality=23) ;
		market_spec24=(doc_speciality=24) ;
		market_spec25=(doc_speciality=25) ;
		market_spec26=(doc_speciality=26) ;
		market_spec27=(doc_speciality=27) ;
		market_spec28=(doc_speciality=28) ;
		market_spec29=(doc_speciality=29) ;

		market_spec31=(doc_speciality=31) ;
		market_spec32=(doc_speciality=32) ;
		market_spec33=(doc_speciality=33) ;
		market_spec34=(doc_speciality=34) ;

		market_spec41=(doc_speciality=41) ;
		market_spec42=(doc_speciality=42) ;
		market_spec43=(doc_speciality=43) ;
		market_spec44=(doc_speciality=44) ;
		market_spec45=(doc_speciality=45) ;
		market_spec46=(doc_speciality=46) ;
		market_spec47=(doc_speciality=47) ;
		market_spec48=(doc_speciality=48) ;
		market_spec49=(doc_speciality=49) ;

		market_spec50=(doc_speciality=50) ;
		market_spec60=(doc_speciality=60) ;
		market_spec90=(doc_speciality=90) ;
run;

proc summary data = temp02_doc_spec nway;
class p_id ;
var market_spec11 -- market_spec90 ;
output out = temp02_doc_spec (drop=_type_ _freq_) max= /autoname ;
run;

data pat_create_t2_&dataset. ;
	merge temp01_doc_spec temp02_doc_spec ;
	by p_id ;
run;

/*QC*/
proc sql;
select count(distinct p_id) from pat_create_t2_&dataset;quit; /*67911*/
/* DOC GENDER PER MARKET VISIT*/

data temp01_doc_gender ;
	set &dataset._market (where=(market=1) keep=p_id doc_gender market) ;
		market_doc_gender1 = (doc_gender=1) ;
		market_doc_gender2 = (doc_gender=2) ;
run;

proc summary data = temp01_doc_gender nway;
class p_id ;
var market_doc_gender1 market_doc_gender2 ;
output out = temp01_doc_gender (drop=_type_ _freq_) max= /autoname ;
run;


data pat_create_t2_&dataset. ;
	merge pat_create_t2_&dataset. temp01_doc_gender ;
	by p_id ;
run;


/* ATC DUMMIES */

data temp01_atc ; 
	set &dataset._pre_index (keep = p_id atc) ;
	atc3 = substr(atc,1,4) ;
run;


/* MOLECULE DUMMIES */

%if &dataset. = icslaba %then %do ;

data temp01_molecule ;
	set &dataset._market (where=(market=1) keep = p_id mol1 market) ;
	if mol1 = "FLUTICASONE FUROATE" then mol1 = "FLUTICASONE" ; 
	drop market ; 
run;
%end ;



%if &dataset. = statins %then %do ;

data temp01_molecule ;
	set &dataset._market (where=(market=1) keep = p_id mol1 market) ;
	if mol1 = "ATORVASTATIN" then mol1 = " " ; 
	drop market ; 
run;
%end;



/* RUN DUMMY GENERATING MACRO (RUN ALREADY BEFORE THIS CODE) */
%nominal_to_binary(sm_dataset=temp01_atc, sm_var=atc3, sm_prefix=atc3_);
%nominal_to_binary(sm_dataset=temp01_molecule, sm_var=mol1, sm_prefix=mol_);




/* ATC DUMMIES */
proc summary data = temp01_atc nway;
class p_id ;
var  atc3_a01a -- atc3_v07a ;
output out = Pat_create_t3_&dataset. (drop=_type_ _freq_ ) max= /autoname ;
run;


/* MOLECULE DUMMIES */

%if &dataset. = icslaba %then %do;

proc summary data = temp01_molecule nway;
class p_id ;
var  mol_BECLOMETASONE -- mol_VILANTEROL ;
output out = Pat_create_t4_&dataset. (drop=_type_ _freq_ ) max= /autoname ;
run;

%end;


%if &dataset. = statins %then %do ; 

proc summary data = temp01_molecule nway;
class p_id ;
var  mol_EZETIMIBE -- mol_SIMVASTATIN ;
output out = Pat_create_t4_&dataset. (drop=_type_ _freq_ ) max= /autoname ;
run;

%end; 



/* CREATE REGION OF PATIENT BASED ON HIGHEST SHARE OF VISITED REGION_DOC */

proc sql;
	create table a1 as
	select
		 p_id
		,region_doc as region_doc
		,count(*) as cnt_tx

	from 
		(select p_id, transactiondate, region_doc, count(*)
			from &dataset._market
			group by p_id, transactiondate, region_doc
		)
/*	where market = 1 */
	group by p_Id, region_doc
	;
quit;


proc summary data = a1 nway ; 
class p_id ; 
var cnt_tx ;
output out = a2 (drop = _type_ _freq_) sum=sum_tx /autoname ; 
run;

data a3 ;
	merge a1 a2 ;
	by p_id ; 
	if region_doc NE " " ;
	main_region_patient = cnt_tx / sum_tx ; 
run;


proc sql;
	create table a4 as
	select
		 p_id
		,max(main_region_patient) as max

	from a3
	group by p_id
	;
quit;


data pat_create_t5_&dataset._a ;
	merge a3 a4 ;
	by p_id ; 
	if main_region_patient = max ; 
run;

proc sort data = pat_create_t5_&dataset._a nodupkey out=pat_create_t5_&dataset._a; by p_id ; run;

/*changed by Jie*/
data pat_create_t5_&dataset.; 
	set pat_create_t5_&dataset._a (keep=p_id region_doc main_region_patient) ; 
	rename main_region_patient = share_main_region_pat;
/*	main_region_pat = input(region_doc,8.)  ;	*/
/*	drop region_doc ;*/  
	rename region_doc = main_region_pat;
run;

/*67829 unique patient without missing in doc_region*/

/* CREATE PHYSICIAN OF PATIENT BASED ON HIGHEST SHARE OF VISITED PHYSICIAN */

proc sql;
	create table a1 as
	select
		 p_id
		,shortcode as shortcode
		,count(*) as cnt_tx

	from
		(select p_id, transactiondate, shortcode, count(*)
			from &dataset._market
			where market = 1 /*and doc_speciality = "11" */
			group by p_id, transactiondate, shortcode
		)
/*	where market = 1 */
	group by p_Id, shortcode 
	;
quit;


proc summary data = a1 nway ; 
class p_id ; 
var cnt_tx ;
output out = a2 (drop = _type_ _freq_) sum=sum_tx /autoname ; 
run;

data a3 ;
	merge a1 a2 ;
	by p_id ; 
	*if region_doc NE " " ;
	main_physician = cnt_tx / sum_tx ; 
run;


proc sql;
	create table a4 as
	select
		 p_id
		,max(main_physician) as max

	from a3
	group by p_id
	;
quit;

data pat_create_t6_&dataset. ;
	merge a3 a4 ;
	by p_id ; 
	if main_physician = max ; 
run;

proc sort data = pat_create_t6_&dataset. nodupkey out=pat_create_t6_&dataset.; by p_id ; run;

data pat_create_t6_&dataset. ; 
	set pat_create_t6_&dataset. (keep=p_id shortcode) ; 
	rename shortcode = main_physician ;
run;
/*67911 unique patient*/

/* MERGE WITH PHYSICIAN GENDER AND PHYSICIAN GRADUATION YEAR */

proc sort data=gp_graduated; by short_id; run;
proc sort data=pat_create_t6_&dataset.; by main_physician; run;
data pat_create_t6_&dataset. ; 
	merge pat_create_t6_&dataset. (in=in1) gp_graduated (rename=(short_id = main_physician)) ;  
	by main_physician ; 
	if in1 ; 
	y=1 ;
run;


		/* CALCULATE MEAN FOR PHYSICIANS WITH NO GRADUATION YEAR */

		proc sql;
			create table temp01_graduation as
			select
				 1 as y
				,round(mean(Graduation_year)) as mean_Graduation_year

			from pat_create_t6_&dataset.
			;
		quit;


data pat_create_t6_&dataset. (keep=p_id main_physician graduation) ;
	merge pat_create_t6_&dataset. temp01_graduation ;
	if Graduation_year = . then Graduation_year = mean_Graduation_year ;
	by y ; 
	graduation = 2015 - Graduation_year;
run;


proc sql;
	create table temp01_physician_gender as
	select
		 shortcode as main_physician
		,doc_gender - 1 as main_physician_gender
		,count(*)

	from &dataset._market
	where doc_gender in (1,2)
	group by main_physician, main_physician_gender
	;
quit;


data pat_create_t6_&dataset. ;
	merge pat_create_t6_&dataset. (in=in1) temp01_physician_gender (drop=_TEMG001)  ; 
	by main_physician ; 
	if in1;
run;

proc sort data=pat_create_t6_&dataset.; by p_id; run;
/*67911 unique patient*/


/*
Result:
	- duration os for all patients = 30 --> delete var
*/



/* GET PACK AND STRENGTH INFO AT INDEX DATE OF MARKET PRODUCT */

/* MERGE NDF WITH ICS DICTIONARY (ALREADY AVAILABLE FOR STATINS) */

%if &dataset. = icslaba %then %do;
proc sort data=ndf.ndf_082012_v2 nodupkey out=temp01_ndf ; by fcc; run;
data dictionary_&dataset._temp ;
	merge dictionary_&dataset. (in=in1) ndf.ndf_082012_v2 (keep= fcc streng packsize) ;
	by fcc;
	if in1; 
run;
%end;
/*

RESULT:
NOT ALL FCC HAVE PACKSIZE AND STRENGTH INFO IN THE NDF FILE 

*/

%if &dataset. = statins %then %do ; 
proc sort data=&dataset._market; by fcc; run;
proc sort data=dictionary_&dataset.;by fcc;run;
data temp01_packsize (keep=p_id mol1 pack_size str_unit); 
	merge &dataset._market (keep=p_id fcc mol1 market trans_date index_date in=in1) dictionary_&dataset. (keep=fcc pack_size str_unit) ; 
	by fcc;
	if in1; 
	if index_date = trans_date ; 
	if market = 1 ; 
run;
proc sort data=temp01_packsize nodupkey out=temp01_packsize; by p_id ; run;
/*67911 unique patient*/
%nominal_to_binary(sm_dataset=temp01_packsize, sm_var=mol1, sm_prefix=mol_start_);
%nominal_to_binary(sm_dataset=temp01_packsize, sm_var=pack_size, sm_prefix=packsize_start_);
%nominal_to_binary(sm_dataset=temp01_packsize, sm_var=str_unit, sm_prefix=strength_start_);

data pat_create_t8_&dataset. ;
	set temp01_packsize (drop=mol1 pack_size str_unit)  ;
run;
/*67911 unique patient*/
%end; 




/* MERGE ALL PATIENT DATA */

%if &dataset. = statins %then %do ; 
data &dataset._pat  ;
	retain p_id persistence ; 
	merge pat_create_t0_&dataset. pat_create_t1_&dataset. pat_create_t2_&dataset. pat_create_t3_&dataset. 
			pat_create_t4_&dataset. pat_create_t5_&dataset. pat_create_t6_&dataset. pat_create_t8_&dataset.;
	by p_id ; 
run;

%end ; 



%if &dataset. = icslaba %then %do ; 
data &dataset._pat  ;
	retain p_id persistence ; 
	merge pat_create_t0_&dataset. pat_create_t1_&dataset. pat_create_t2_&dataset. pat_create_t3_&dataset. 
			pat_create_t4_&dataset. pat_create_t5_&dataset. pat_create_t6_&dataset.    /* pat_create_t8_&market. */;
	by p_id ; 
run;

%end; 


%mend;

%pat_create(statins) ; 







/********************************************************/
/* 4.0 MERGE ALL DATA TO ONE FINAL DATASET FOR ANALYSIS */
/********************************************************/


%let dataset = icslaba ; 
%let dataset = statins ; 

%macro merge1(dataset) ;

/* MERGE WITH SOCIODEMO FILE */

proc sort data=&dataset._pat ; by main_region_pat ; run;

/*added by Jie to convert the meyon to numeric*/
data sociodemo;
set sociodemo;
meyon_num = input(meyon, comma8.);
drop meyon;
rename meyon_num=meyon;
run;

data &dataset._pat_all ;
	retain p_id persistence main_region_pat ; 
	merge &dataset._pat (in=in1) sociodemo (rename=(brick=main_region_pat)) ;
	by main_region_pat ;
	if in1 ; 
run;

/*QC*/
proc sql;
select count(*) from &dataset._pat
where main_region_pat = 99910;
quit;
proc sql;
select count(main_region_pat) from
&dataset._pat 
where p_id in
(
select p_id from &dataset._pat_all where surface=.
);
quit;

proc sql;
select count(main_region_pat) from &dataset._pat
where main_region_pat not in (select brick from sociodemo);

quit;
/*156 patient have missing sociodemo (main_region_pat=99910)*/


/* MERGE WITH XPO DATA */

proc sort data=&dataset._pat_all ; by main_physician; run;
data &dataset._pat_all ;
	merge &dataset._pat_all (in=in1) xpo_sales (rename=(shortcode=main_physician )) ;
	by main_physician ; 
	if in1 ; 
run;


/* MERGE WITH ONEKEY DATA */

/*proc sort data = &dataset._pat_t2 ; by main_physician ; run;*/
/**/
/*data &dataset._pat_t3 ; */
/*	merge &dataset._pat_t2 (in=in1) onekey (rename=(SHORT_ID=main_physician)) ; */
/*	by ;*/
/*	if in1;*/
/*run;*/

/*

RESULT: 
TOO MANY MISSINGS IN ONEKEY DATA. WILL NOT BE FURTHER CONSIDERED;

*/




/* GENERATE URBAN DUMMY */

data temp01_urb;
	set &dataset._pat_all (keep=p_id urb ) ;
	if urb NE . ; 
run;
/*67670 unique patient*/

%nominal_to_binary(sm_dataset=temp01_urb, sm_var=urb, sm_prefix=urban_);


/* URBAN DUMMIES */
proc summary data = temp01_urb nway;
class p_id ;
var  urban_10 -- urban_64 ;
output out = Pat_create_t9_&dataset. (drop=_type_ _freq_ ) max= /autoname ;
run;
/*67670 unique patient*/

/* MERGE WITH ORIGINAL DATA */

PROC SORT DATA = &dataset._pat_all; BY P_ID; RUN;

data &dataset._pat_all ; 
	merge &dataset._pat_all Pat_create_t9_&dataset. ; 
	by p_id ; 
run;



/* REPLACE MISSINGS WHERE NECESSARY AND DELETE UNNECESSARY VARIABLES AND OBSERVATIONS */

data &dataset._pat_all ; 
	set &dataset._pat_all ; 
 	
	if active = "Y" then physician_active = 1;
		else if active ="N" then physician_active = 0;
		else physician_active = . ;
	
	if mean_doc_gender_12m = . then delete ; 
	if active = " " then delete ; 

	drop main_physician main_region_pat share_main_region_pat active ; 
run;
/*65087*/


/*ADD RESPONSE VARIBALE*/
/*check response rate by different number of months as threshold*/
/*%let nmonths=10;*/
proc sql;
create table pat_capdays as
select distinct p_id, Capped_Days_treatment
from &dataset.;quit;

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
create table &dataset._pat_all as
select a.*, b.persistence_3m, b.persistence_6m, b.persistence_9m
from &dataset._pat_all a left join p_persistence b
on a.p_id = b.p_id;quit;
/*65087*/

%mend ; 

%macro Yan_additionalPreProdFeature(dataset);
/*features pre-index*/
proc sql;
create table profiling_features_add as
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

from &dataset._pre_index
group by p_id
order by p_id
;quit;

%mend;

%macro final_pat_data_add(dataset);
/*add the additional product pre_feature covariates created by Yan to the patient level data */
proc sql;
create table &dataset._pat_all_v2 as
select a.*, b.*
from 
&dataset._pat_all a left join profiling_features_add b
on a.p_id=b.p_id;
quit;
%mend;

%merge1(statins);
%Yan_additionalPreProdFeature(statins);
%final_pat_data_add(statins);

/* EXPORT DATA */

PROC EXPORT DATA = icslaba_pat_all
            OUTFILE = 'E:\data\BE Persistence estimation\icslaba_pat.csv'
/*'Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02a Data StO\icslaba.csv' */
            DBMS = csv replace;
RUN;

PROC EXPORT DATA = statins_pat_all_v2
            OUTFILE = 'C:\work\working materials\Belgium\data\preModelData\statins_pat_v2.csv'
            DBMS = csv replace;
RUN;


%include "C:\work\working materials\Belgium\code\summarizer_post.sas";
option nomprint;
%let in_data=statins_pat_all_v2; *change;
*change if you need;
%let missing_value=%nrstr("not recorded", "not specified","unspecified","n/a","ni","n\a","n i", "unknown", "unsure of usage","none","u", "unclassified");
%let out_path=C:\work\working materials\Belgium\data\preModelData; *change;
%let out_table_name=statins_v2_data_summary; *no file extension, change;
%summarizer_post(&in_data,&missing_value, &out_path, &out_table_name);

proc import out=statins_jie
datafile="C:\work\working materials\Belgium\data\preModelData\statins_pat_v2.csv"
dbms=CSV replace;
run;

proc import out=statins_alex
datafile="C:\work\working materials\Belgium\data\from_Alex\statins_pat.csv"
dbms=CSV replace;
run;

/*check which patients in Jie's can not be found in Alex's */
proc sql;
select count(p_id) from statins_jie
where p_id not in (select p_id from statins_alex);
quit;
/*437*/
/*check which patients in Alex's can not be found in Jie's */
proc sql;
select count(p_id) from statins_alex
where p_id not in (select p_id from statins_jie);
quit;
/*0*/
/*output the 437 patients that is only in jie's data*/
proc sql;
create table jie_extra as
select * from statins_jie
where p_id not in (select p_id from statins_alex);
quit;

/*output the patients in Jie which are exist in Alex's data*/
proc sql;
create table jie_comp as
select * from statins_jie
where p_id in (select p_id from statins_alex);
quit;

proc summary data=jie_comp;
var _all_;
output out = summary_jie (drop=_type_ _freq_ ) mean= /autoname ;
run;
proc summary data=statins_alex;
var _all_;
output out = summary_alex (drop=_type_ _freq_ ) mean= /autoname ;
run;

data summary_jie_alex;
set summary_jie summary_alex;
run;

proc transpose data=summary_jie_alex out=comp1(rename=(col1=Jie col2=Alex));
var _all_;
run;

data sasdata.comp_diff;
set comp1;
diff_rate=abs(jie-alex)/alex;
run;

proc export data=sasdata.comp_diff
outfile="C:\work\working materials\Belgium\data\preModelData\Comparison_AlexandJie.csv"
dbms=CSV replace;
run;
