

%macro write_missing_values(missing_value);

%let __j=1;

%do %while(not(%qscan(&missing_value.,&__j,%str(,))=));
	%let v&__j.=%scan(&missing_value.,&__j,%str(,));
/*	%put &&v&__j.;*/


	%if &__j=1 %then %do;
	data test1;
	run;
	data test1;
	var&__j.=&&v&__j.;
/*	var&__j.= '"' !! var&__j. !! '"';*/
	run;
	%end;
	%else %do;
	data test1;
	set test1;
	var&__j.=&&v&__j.;
/*	var&__j.= '"' !! var&__j. !! '"';*/
	run;

	%end;

	%let __j=%eval(&__j+1);
%end;

%let total=%eval(&__j-1);
/*%put &total.;*/


data missing_written;
set test1;
length comb_str $500.;

%do __i =1 %to &total.;
	%if &__i.=1 %then %do;
		comb_str=trim(left(var1));
	%end;
	%else %do;
		comb_str=trim(left(comb_str)) || " , " || trim(left(var&__i.));
	%end;
%end;

keep comb_str;
run;

/*proc datasets lib=work;*/
/*delete test1;run;quit;*/



%mend write_missing_values;
