

%macro summarizer_post(in_data,missing_value, out_path,out_table_name);

proc contents data=&in_data. out=temp_contents noprint;run;
proc sort data=temp_contents; by VARNUM;run;
proc sql noprint;
select NAME into :var_name_list separated by " " from temp_contents;
select TYPE into :var_type_list separated by " " from temp_contents;
quit;

proc sql noprint;
select count(*) into :total_rec from  &in_data.;quit;

%write_missing_values(&missing_value.);


%let _j=1;
%do %while(not(%qscan(&var_name_list.,&_j,%str( ))=));

	%let cv=%scan(&var_name_list.,&_j,%str( )); 
	%let cv_type= %scan(&var_type_list.,&_j,%str( ));
/*if it is numeirc type	*/
	%if &cv_type.=1 %then %do;

 
		proc sql noprint; 
		select count(*) into :total_available from &in_data. where &cv. ^=.;
		select count(*) into :total_missing from &in_data. where &cv. =. ;
		select count(distinct &cv.) into :total_distinct from &in_data. where &cv. ^=.;
		quit;

		proc means data=&in_data. noprint;
		var &cv.;
		output out=stats(drop=_:) min=Minimum max=Maximum  mean = Mean median=Median  range=Range;

	data outstats_&cv.;
	format Variable_Name $35. 
		   Variable_Type $10.;

	Variable_Name="&cv.";
	Variable_Type="Numeric";
	Total_Records=&total_rec.;
	Total_Available= &total_available.;
	format  Total_Available_Perc percent8.2;
	Total_Available_Perc = %sysevalf(&total_available./&total_rec.); 
	Total_Blank= &total_missing.;
	format  Total_Blank_Perc percent8.2;
	Total_Blank_Perc=%sysevalf(&total_missing./&total_rec.);
	format All_Other_Missing_Values $500.;
	All_Other_Missing_Values="";
	All_Other_Missing = 0;
	format  All_Other_Missing_Perc percent8.2;
	All_Other_Missing_Perc=0;
	All_Distinct=&total_distinct.;
	run;

	data outstats_&cv.;
	merge outstats_&cv. stats;
/*	format Minimum best32.*/
/*		   Maximum best32.*/
/*		   Mean best32.*/
/*		   Median best32.*/
/*		   Range best32.;*/
	run;

		    

	%end;
/*if it is a character type*/
	%else %if &cv_type.=2 %then %do;
/*		%let var_type=character;*/
		proc sql noprint;

		%if &missing_value.^= %then %do;
			select count(*) into :total_available from &in_data. where lowcase(&cv.) not in (&missing_value.) and &cv.^="";
			select count(*) into :total_other_missing from &in_data. where lowcase(&cv.) in (&missing_value.);
			select count(distinct &cv.) into :total_distinct from  &in_data. where lowcase(&cv.) not in (&missing_value.) and &cv. ^="";
		%end;
		%else %do;
			select count(*) into :total_available from &in_data. where &cv.^="";
			%let total_other_missing=0;
			select count(distinct &cv.) into :total_distinct from  &in_data. where &cv. ^="";
		%end;

			select count(*) into :total_missing from &in_data. where &cv.="" ; quit;

	data outstats_&cv.;
	format Variable_Name $35. 
		   Variable_Type $10.;

	Variable_Name="&cv.";
	Variable_Type="Character";
	Total_Records=&total_rec.;
	Total_Available= &total_available.;
	format  Total_Available_Perc percent8.2;
	Total_Available_Perc = %sysevalf(&total_available./&total_rec.); 
	Total_Blank= &total_missing.;
	format  Total_Blank_Perc percent8.2;
	Total_Blank_Perc=%sysevalf(&total_missing./&total_rec.);
	format All_Other_Missing_Values $500.;
	All_Other_Missing_Values="";
	All_Other_Missing = &total_other_missing.;
	format  All_Other_Missing_Perc percent8.2;
	All_Other_Missing_Perc=%sysevalf(&total_other_missing./&total_rec.);
	All_Distinct=&total_distinct.;


	Minimum=.;
	Maximum=.;
	Mean=.;
	Median =.;
	Range =.;
	run;

/*fill in missing values*/
	data outstats_&cv.;
	merge outstats_&cv. Missing_written; 
	All_Other_Missing_Values=comb_str;
	drop comb_str;
	run;

	%end;

%if &_j=1 %then %do;
	data final_out_table;
	run;

	data final_out_table;
	set outstats_&cv.;
	run;
%end;
%else %do;
	data final_out_table;
	set final_out_table outstats_&cv.;
	run;

%end;



 	%let _j=%eval(&_j+1);

%end;

	data final_out_table;
	set final_out_table;
	format Minimum 10.2
		   Maximum 10.2
		   Mean 10.2
		   Median 10.2
		   Range 10.2;
	run;

proc export data=final_out_table
outfile="&out_path.\&out_table_name..csv"
dbms=csv replace;
run;


proc datasets lib=work kill;
run;quit;


%mend summarizer_post;
