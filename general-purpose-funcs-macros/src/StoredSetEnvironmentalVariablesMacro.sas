

options mstored sasmstore=MSTORE;
/*
This macro loads in work library the xml with the environmental
macro variables contained in the ETL_variables.xml file
*/
%MACRO get_xml_config(out_ds=/*name of the output dataset*/) 
/ source store des="import xml in work library";
    /*location of the xml file is the same of the stored macros*/
    %let path=%sysfunc(pathname(MSTORE))\ETL_variables.xml;
    %let rc=%sysfunc(libname(ENVAR,&PATH.,XML));
	%put rc=&rc.;
	%put server_path=&server_path.;
	%if &rc. eq 0 %then  %do;
		data &out_ds.;	   
		   set envar.etl_variables;
		run;
	%end;
	%else %do;
	    %put Error libname not assigned, check if the xml file exists.;
	%end;
%mend;

/*
This macro set global macro variable VARNAME if found in ENV_VARIABLE dataset
*/
%macro load_env_macro_variable(varname=,macrovarname=)/ 
source store des="This macro set global macro variable VARNAME if found in ENV_VARIABLE dataset";
/* check if another macrovariable with same name exist */

%let varname_correct=%sysfunc(prxmatch(%nrstr(/^([A-Z]|[a-z]|_)([A-Z]|[a-z]|\d|_){0,31}$/),&varname.));
%let macrovarname_correct=%sysfunc(prxmatch(%nrstr(/^([A-Z]|[a-z]|_)([A-Z]|[a-z]|\d|_){0,31}$/),&macrovarname.));

%if &varname_correct. ne 0 & &macrovarname_correct. ne 0 %then %do;
	%global &macrovarname. ;
	data _null_;
		set aut_dwh.env_variables;
		where is_valid=1;
		if upcase(variable_name)=upcase("&varname.") then do;
			call symput("&macrovarname.",trim(variable_value));
		end;
	run;
	%if "&&&macrovarname.." eq ""  %then %do;
		%put Macro variable &macrovarname. not created, please check if &varname is in
         the list of available variables in AUT_DWH.ENV_VARIABLES;
		 data _NULL_;
             set aut_dwh.env_variables;
			 put variable_name= variable_value=;
			 where is_valid=1;
		 run;
		%symdel &macrovarname. ;
	%end;
%end;
%else %do;
	%if &varname_correct. = 0 %then %do;
		%put &varname. ->invalid variable name;
	%end;
	%else %if  &macrovarname_correct. eq 0 %then %do;
    	%put &macrovarname. ->invalid variable name;
	%end;
%end;
%mend;


%*get_xml_config(out_ds=test);

%*load_env_macro_variable(varname=LOGDIR_DELIVERY);
/*################################################################################################################*/


Select Environment

/*################################################################################################################*/
%macro ENV(var_name= /*name of the variable*/, 
           ED=EDITORIAL/*value of VAR_NAME if the environment is EDITORIAL(default value:EDITORIAL)*/,
           DE=DELIVERY/*value of VAR_NAME if the environment is DELIVERY( default value:DELIVERY)*/
           ) 
            / store source des='Checks the port of the connection (8561 or 8562) to identify the environment (respectively delivery or editorial).
                         ';
%if %sysfunc(nvalid(&var_name.))=1 %then %do;
    %global &var_name.; 
	%let port=%sysfunc(getoption(metaport));
	/* port identifies the environment */
	%if &port.=8561 %then %do;
	    %let &var_name.=&de.;
	%end;
	%if &port.=8562 %then %do;
	    %let &var_name.=&ED.;
	%end;
    %put Macro variable created: &var_name=&&&var_name.;
%end;
%else %put Invalid name for a macro variable ;
%mend;



