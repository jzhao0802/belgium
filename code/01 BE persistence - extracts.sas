/*********************************************************************************** HEADER START *
  IMS HEALTH / Statistical Services EMEA -- Frankfurt (c) 2013
 --------------------------------------------------------------------------------------------------
  TITLE          = PERSISTENCE ESTIMATION - PREDICTIVE ANALYTICS / ML
  DATASOURCE     = 
  AUTHOR         = Alexander Staus
  VERSION        = 1.0

  DESCRIPTION    = 	
					1.0 IMPORT ALL DATA
					2.0 MERGE DATA AND CO 
					3.0 CHECK CORRELATION AND AND CREATE NEW DUMMY VARIABLES
					4.0 MERGE ALL DATA TO ONE FINAL DATASET FOR ANALYSIS
 --------------------------------------------------------------------------------------------------
  COUNTRY        = BE
  AUDIT          = LRx Persistence
  month          = 
  CLIENT         = 
  PROJECT        = LRX BE PERSISTENCE
  STATUS         = 
  CATEGORY       = Program
  LOCATION       = Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\ 
 --------------------------------------------------------------------------------------------------
  MODIFICATIONS (latest entry at the top)

  Ver   DATE            NAME (COMPANY, DEP)                                              MOD-NO
                        short description
 --------------------------------------------------------------------------------------------------
  1.0	19/08/2015		Alexander Staus (IMS, StO Frankfurt)
 --------------------------------------------------------------------------------------------------
/************************************************************************************ HEADER END */

 
/******************************* FOLDER DEFINITION ***********************************************/


libname in "E:\data\BE Persistence estimation";
libname instatin "Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.3. STATINS\Dataset\" ;

/*options obs=10001;*/
/*options obs=max;*/


/* FIRST CHECKS TO VERIFY FROM BE LO */

%macro cnt(dataset) ;
proc sql;
	create table stats_&dataset. as
	select
		 count(*) as cnt_tx
		,count(distinct ddms_pha) as cnt_pharm
		,count(distinct p_id) as cnt_pat
		,count(distinct shortcode) as cnt_docs

	from in.&dataset.
	;
quit;
%mend;

%cnt(Icslaba);
%cnt(Statins);



/*******************************************************/
/*************** 1.0 IMPORT ALL DATA *******************/
/*******************************************************/


/*IMPORT DATA DICTIONARY */

PROC IMPORT OUT= dictionary_icslaba
            DATAFILE= "Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.1. Data dictionnary\ICS_LABA_mkt_def.xls" 
            DBMS=EXCELCS REPLACE;
			sheet="Mkt_def";		
RUN;

PROC IMPORT OUT= dictionary_statins
            DATAFILE= "Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.1. Data dictionnary\Statins mkt def.xlsx" 
            DBMS=EXCELCS REPLACE;
			sheet="MSD_CV";		
RUN;


/* IMPORT ADDITIONAL PATIENTS TO DELETE */

PROC IMPORT OUT= pat_delete_icslaba 
            DATAFILE= "Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.2. ICS_LABA\Dataset\ICS_LABA_EXCLUDE.xlsx" 
            DBMS=EXCELCS REPLACE;
			sheet="LN00330";		
RUN;

PROC IMPORT OUT= pat_delete_statins
            DATAFILE= "Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.3. STATINS\Dataset\Statins to exclude.csv" 
            DBMS=csv REPLACE;
/*			sheet="LN00330";		*/
RUN;



/* IMPORT PATIENTS TO KEEP AND CALCULATED PERSISTENCE (IN DAYS) PER PATIENT */

PROC IMPORT OUT= cal_persistence_icslaba 
            DATAFILE= "s:\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.2. ICS_LABA\Dataset\ICS_LABA_TOM_200_buff.xlsx" 
            DBMS=EXCELCS REPLACE;
			sheet="Persistent";		
RUN;

PROC IMPORT OUT= cal_persistence_statins 
            DATAFILE= "s:\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.3. STATINS\Dataset\Statins_TOM_50_buff.xlsx" 
            DBMS=EXCELCS REPLACE;
			sheet="Persistent";		
RUN;



/* IMPORT SOCIODEMOGRAPHICS */

PROC IMPORT OUT= sociodemo
            DATAFILE= "s:\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.4. Input variables\données demo 2014.xlsx" 
            DBMS=EXCELCS REPLACE;
			sheet="demo2014";		
RUN;


/*IMPORT SALES (PER ATC) PER PHYSICIAN FROM XPONENT */

PROC IMPORT OUT= Xpo_Sales
            DATAFILE= "s:\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.4. Input variables\PRED_PER_extract_v2 - AStaus.xlsx" 
            DBMS=EXCELCS REPLACE;
			sheet="REC_12";		
RUN;


/* IMPORT ONEKEY DATA PER PHYSICIAN */

PROC IMPORT OUT= Onekey 
            DATAFILE= "s:\STOCountry\BE\LRx\Persistence Estimation\02 Data LO\Persistence prediction\0.4. Input variables\Icomed data at physician level.xlsx" 
            DBMS=EXCELCS REPLACE;
			sheet="data";		
RUN;



/**************************************************/
/************ 2.0 MERGE DATA AND CO ***************/
/**************************************************/

/*  KEEP ONLY VARIABLE WHICH ARE NECESSARY FOR LRX PERSISTENCE STUDY */

%macro getdata(dataset);

%if &dataset. = icslaba %then %do ;
data &dataset. ;
	length doc_gender $1. region_doc $6. doc_speciality $2. mol1 $70. ;
	format doc_gender $1. region_doc $6.  doc_speciality $2.  mol1 $70. ;
	informat doc_gender $1. region_doc $6. doc_speciality $2. mol1 $70. ;

	set in.&dataset.(keep = ddms_pha transactiondate Transactiontype rx_id fcc units shortcode p_id p_gender dci_vos
								doc_gender region_doc doc_speciality cu atc otc rsp mol1);
run;

proc sort data=&dataset.; by fcc; run;
proc sort data=dictionary_&dataset.; by fcc; run;

data &dataset. ;
	merge &dataset. (in=in1) dictionary_&dataset. (in=in2 keep=fcc duration) ; 
	by fcc;
	if in1 ;
	if in1 and in2 then market = 1 ;
		else market = 0 ; 
run;
%end;


/* STATINS DATA IS MISSING SOME VARIABLES AND ARE MERGED IN A LATER STEP */
%if &dataset. = statins %then %do ;
data &dataset. ;
	set in.&dataset.(keep = ddms_pha transactiondate Transactiontype rx_id fcc units shortcode p_id p_gender dci_vos
								/*doc_gender region_doc doc_speciality */ cu atc otc rsp mol1);
run;

proc sort data=&dataset.; by fcc; run;
proc sort data=dictionary_&dataset.; by fcc; run;
data &dataset. ;
	merge &dataset. (in=in1) dictionary_&dataset. (in=in2 keep=fcc pack_size ) ; 
	by fcc;
	if in1 ;
	if in1 and in2 then market = 1 ;
		else market = 0 ; 
	rename pack_size = duration ;
run;


/* MERGE WITH DOCTOR INFORMATION, WHICH WAS PROVIDED FOR STATINS IN A SEPARATE DATASET FROM THE LO */

proc sort data=&dataset. ; by shortcode ; run;
proc sort data=instatin.doc_details out=doc_details ; by shortcode ; run;

data &dataset. ;
	length doc_gender $1. region_doc $6. doc_speciality $2. mol1 $70. ;
	format doc_gender $1. region_doc $6.  doc_speciality $2. mol1 $70. ;
	informat doc_gender $1. region_doc $6. doc_speciality $2. mol1 $70. ;

	merge &dataset. doc_details (drop = activity ) ;
	by shortcode ;
run;
%end;


proc sort data=&dataset. ; by p_id ; run; 
proc sort data=Pat_delete_&dataset. ; by p_id ; run;
data &dataset. ;
	merge &dataset. (in=in1) Pat_delete_&dataset. (in=in2) ;
	by p_id ; 
	if in1 and not in2 ;
run;


proc sort data=&dataset. ; by p_id ; run; 
proc sort data=cal_persistence_&dataset. ; by p_id ; run;
data &dataset. ;
	merge &dataset. (in=in1) cal_persistence_&dataset. (drop=F_FLAG in=in2) ;
	by p_id ; 
	if in1 and in2 ;
run;

%mend ;


%getdata(icslaba);
%getdata(statins);



/* EXPORT DATA */

PROC EXPORT DATA = icslaba
            OUTFILE = 'E:\data\BE Persistence estimation\icslaba.csv'
/*'Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02a Data StO\icslaba.csv' */
            DBMS = csv replace;
RUN;

PROC EXPORT DATA = statins
            OUTFILE = 'E:\data\BE Persistence estimation\statins.csv' 
/* 'E:\data\BE Persistence estimation\icslaba.csv' */
            DBMS = csv replace;
RUN;





/************************************************************/
/*** 3.0 CHECK CORRELATION AND CREATE NEW DUMMY VARIABLES ***/
/************************************************************/


/* SOME MACROS */

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





%let dataset = icslaba ;





%macro pat_create(dataset) ; 
/* SOME COUNT AND AVERAGE CALCULATIONS */

proc sql;
	create table pat_create_t1_&dataset. as
	select
		 p_id
		,count(*) as cnt_tx
		,count(distinct transactiondate) as cnt_tx_days
		,count(distinct shortcode) as cnt_docs
		,count(distinct doc_speciality) as cnt_specialties
		,mean(input(doc_gender,8.)) as mean_doc_gender
		,count(distinct ddms_pha) as cnt_pharmacies
		,count(distinct fcc) as cnt_fcc
		,count(distinct atc) as cnt_atc
		,count(distinct otc) as cnt_otc
		,mean(rsp) as mean_price
		,sum(rsp*units) as sum_spending

		/* ADDED */
		,mean(rx_id) as mean_rx_id
		,mean(transactiontype) as mean_tx_type
		,mean(dci_vos) as mean_dcivos
		,mean(units) as mean_units

		,mean(capped_days_treatment) as persistence

	from &dataset. 
	group by p_id
	;
quit;

/* PER MARKET */
proc sql;
	create table pat_create_t1a_&dataset. as
	select
		 p_id
		,count(distinct ddms_pha) as market_cnt_pharmacies
		,count(distinct fcc) as market_cnt_fcc
		,count(distinct atc) as market_cnt_atc
		,mean(rsp) as market_mean_price
/*		,sum(rsp*units) as market_sum_spending*/
		,mean(dci_vos) as market_mean_dcivos

		%if &dataset. = statins %then %do;
		/* ADDED */
		,mean(duration) as market_mean_duration
		%end ; 

	from &dataset. 
	where market = 1 
	group by p_id
	;
quit;



/* WITHIN ~12 MONTHS BEFORE FIRST MARKET RX */

proc sort data=&dataset. (keep=p_id transactiondate fcc market where=(market=1)) 
			out=temp01_cnt_time ; by p_id transactiondate ; run ; 

proc sort data = temp01_cnt_time nodupkey out = temp02_cnt_time (rename=(transactiondate=FirstRx)) ; by p_id ; run ; 

data temp03_cnt_time ;
	merge temp02_cnt_time (keep=firstrx p_id) &dataset. (keep=p_id transactiondate fcc market) ; 
	by p_id ; 
	if FirstRx - 10000 <= transactiondate <= FirstRx ; 
run; 


proc sql;
	create table pat_create_t1b_&dataset. as
	select
		 p_id
		,count(*) as cnt_tx_12m
		,count(distinct transactiondate) as cnt_tx_days_12m
		,count(distinct fcc) as cnt_fcc_12m
		
	from temp03_cnt_time
	group by p_id
	;
quit;




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
	set &dataset. (keep=p_id doc_speciality);

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

proc summary data = temp01_doc_spec nway;
class p_id ;
var spec11 -- spec90 ;
output out = temp01_doc_spec (drop=_type_ _freq_) max= /autoname ;
run;


data temp02_doc_spec ;
	set &dataset. (where=(market=1) keep=p_id doc_speciality market) ;

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


/* DOC GENDER PER MARKET VISIT*/

data temp01_doc_gender ;
	set &dataset. (where=(market=1) keep=p_id doc_gender market) ;
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
	set &dataset. (keep = p_id atc) ;
	atc3 = substr(atc,1,4) ;
run;


/* MOLECULE DUMMIES */

%if &dataset. = icslaba %then %do ;

data temp01_molecule ;
	set &dataset. (where=(market=1) keep = p_id mol1 market) ;
	if mol1 = "FLUTICASONE FUROATE" then mol1 = "FLUTICASONE" ; 
	drop market ; 
run;
%end ;



%if &dataset. = statins %then %do ;

data temp01_molecule ;
	set &dataset. (where=(market=1) keep = p_id mol1 market) ;
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
output out = Pat_create_t3_&dataset. (drop=_type_ _freq_ atc atc3 atc3_) max= /autoname ;
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

	from &dataset.
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


data pat_create_t5_&dataset. ;
	merge a3 a4 ;
	by p_id ; 
	if main_region_patient = max ; 
run;

proc sort data = pat_create_t5_&dataset. nodupkey out=pat_create_t5_&dataset.; by p_id ; run;

data pat_create_t5_&dataset. ; 
	set pat_create_t5_&dataset. (keep=p_id region_doc main_region_patient) ; 
	rename main_region_patient = share_main_region_pat;
	main_region_pat = input(region_doc,8.)  ;	
	drop region_doc ;
run;


/* CREATE PHYSICIAN OF PATIENT BASED ON HIGHEST SHARE OF VISITED PHYSICIAN */

proc sql;
	create table a1 as
	select
		 p_id
		,shortcode as shortcode
		,count(*) as cnt_tx

	from &dataset.
	where market = 1 
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


/*
Result:
	- duration os for all patients = 30 --> delete var
*/


/* MERGE ALL PATIENT DATA */

data &dataset._pat  ;
	retain p_id persistence ; 
	merge pat_create_t1_&dataset. pat_create_t1a_&dataset. pat_create_t2_&dataset. pat_create_t3_&dataset. 
			pat_create_t4_&dataset. pat_create_t5_&dataset. pat_create_t6_&dataset. ;
	by p_id ; 
run;

%mend;

%pat_create(icslaba) ; 
%pat_create(statins) ; 




/********************************************************/
/* 4.0 MERGE ALL DATA TO ONE FINAL DATASET FOR ANALYSIS */
/********************************************************/


%let dataset = icslaba ; 
%let dataset = statins ; 

%macro merge1(dataset) ;

/* MERGE WITH SOCIODEMO FILE */

proc sort data=&dataset._pat ; by main_region_pat ; run;

data &dataset._pat_t1 ;
	retain p_id persistence main_region_pat ; 
	merge &dataset._pat (in=in1) sociodemo (rename=(brick=main_region_pat)) ;
	by main_region_pat ;
	if in1 ; 
run;



/* MERGE WITH XPO DATA */

proc sort data=&dataset._pat_t1 ; by main_physician; run;
data &dataset._pat_t2 ;
	merge &dataset._pat_t1 (in=in1) xpo_sales (rename=(shortcode=main_physician )) ;
	by main_physician ; 
	if in1 ; 
run;


/* MERGE WITH ONEKEY DATA */

proc sort data = &dataset._pat_t2 ; by main_physician ; run;

data &dataset._pat_t3 ; 
	merge &dataset._pat_t2 (in=in1) onekey (rename=(SHORT_ID=main_physician)) ; 
	by ;
	if in1;
run;

%mend ; 

%merge1(icslaba);
%merge1(statins);



/* EXPORT DATA */

PROC EXPORT DATA = icslaba_pat
            OUTFILE = 'E:\data\BE Persistence estimation\icslaba_pat.csv'
/*'Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02a Data StO\icslaba.csv' */
            DBMS = csv replace;
RUN;

PROC EXPORT DATA = icslaba_pat_t3
            OUTFILE = 'E:\data\BE Persistence estimation\icslaba_pat_t3.csv'
/*'Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02a Data StO\icslaba.csv' */
            DBMS = csv replace;
RUN;


PROC EXPORT DATA = statins_pat
            OUTFILE = 'E:\data\BE Persistence estimation\statins_pat.csv'
/*'Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02a Data StO\icslaba.csv' */
            DBMS = csv replace;
RUN;

PROC EXPORT DATA = statins_pat_t3
            OUTFILE = 'E:\data\BE Persistence estimation\statins_pat_t3.csv'
/*'Z:\Departments\STOCountry\BE\LRx\Persistence Estimation\02a Data StO\icslaba.csv' */
            DBMS = csv replace;
RUN;




/*

Result: 
- ~4100 PATIENTS OF MORE THAN 60K PATIENTS HAVE FILLED DATA PER COLUMN FOR BOTH MARKETS. 

*/



/* ADD ADDITIONAL VARIABLES BASED ON AMOUNT OF CONSUMPTION WITHIN TIME FRAMES */


/*******************************************************/
/*******************************************************/
/************************* END *************************/
/*******************************************************/
/*******************************************************/



/*********/
/* STUFF */
/*********/



/* do:

NEW:
- new dummies based on specific other products (statins from Flore, ICS based on consumption)
	- create with different time frames
- create dummies/sums/counts from point 3.0 based on last 12 months or different/similar
- delete onekey data due to few patients have filled data


OLD: 
- create dummy variables, overall and per market Rx for some var
	- per specialty (per market rx) --> OK
	- region_doc --> too many characteristics: 594
	- doc_gender (per market rx) --> OK
	- per physician (per market rx) --> NO, too many
	- atc3/atc4 --> OK
	- fcc (per market rx) --> NO, too many

- get sum and avg prices per market rx --> OK
- average dci_vos per market rx --> OK

- add dummies: molecule, (duration)

- add evtl. onekey data!

*/


proc corr data = &dataset._pat ;
run;

proc sql;
	create table b1 as
	select
 		 shortcode
		,count(*)

	from icslaba
	where market = 1 
	group by shortcode
	;
quit;

proc sql;
	create table a1 as
	select
		 distinct region_doc
		,count(distinct p_id)

	from &dataset.
	where market = 1 
	group by region_doc
	;
quit;









proc sql;
	select
		count(distinct mol1)

	from Ics_laba_history_sql_no_combomol
	;
quit;


proc sort data=tmp1.Imb_mol nodupkey out=a1 ; by fcc ; run;
proc sort data=&dataset. out=a2 ; by fcc ; run;
 
data test ;
	merge a2 (in=in1) a1 (keep=fcc molname) ;
	by fcc ;
	if in1 ;
run;


proc sql;
	create table a3 as
	select
	 market
	,molname

	from test 
	where market = 1 
	group by market
	;
quit;






