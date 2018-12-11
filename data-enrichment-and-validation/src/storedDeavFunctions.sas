
options mstored sasmstore=MSTORE;

%macro DEAV_CREATE_EMPTY_ERR_TABLE(errorTable) / store source des="Create the error table without records";
	proc sql;
		create table &errorTable. (ROW_ID integer, ERR_CODE varchar(4000), ERR_TYPE varchar(4000), 
			ERR_MESSAGE varchar(4000), ERR_COLUMN varchar(4000), ERR_VALUE varchar(4000));
	run;
%mend;

%macro writeErrorTo / store source des="Used in the writeErrorTo FCMP function. Do not use directly!";

	%let column = %sysfunc(compress(&column.));
	%let value = %sysfunc(compress(&value.));
	%let message = %sysfunc(trim(%sysfunc(dequote(&message.))));
	%let errorTable = %sysfunc(dequote(&errorTable.));
	%put code=&code. message=&message. errortable=&errorTable. column=&column. value=&value.;

	%if not %sysfunc(exist(&errorTable.)) %then %do;
		%put &errorTable. does not exist, creating it;
		%DEAV_CREATE_EMPTY_ERR_TABLE(&errorTable.);
	%end;

	proc sql;
		insert into &errorTable. (ROW_ID, ERR_CODE, ERR_TYPE, ERR_MESSAGE, ERR_COLUMN, ERR_VALUE) values (&id., &code., &type., "&message.", &column., &value.);
	run;
%mend writeErrorTo;



options cmplib=MSTORE.DEAV;
proc fcmp outlib = MSTORE.DEAV.validation;

/* Write an error/warning in the error table */
function writeErrorTo(id /* identifier of the validated row, e.g. row number */, 
	code $ /* error code */, message $ /* error message */, 
	type $ /* error type */, errorTable $ /* name of the error table */, 
	columns[*] $ /* list of columns names involved in the error */, 
	values[*] $ /* values of the columns involved in the error (same order as 'columns' */);

	length column $500;
	length value $500;

	do i = 1 to dim(columns);

		column = columns[i];
		value = values[i];

		rc = run_macro('writeErrorTo', id, code, message, errorTable, type, column, value);

		if rc = 1 then put "ERROR: cannot run macro test for column" column "and value" value;
	end;
	return(1);
endsub;
run;

