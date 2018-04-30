
options mstored sasmstore=MSTORE;
%macro DEAV_INT_TO_BR_ERRORS(DEAV_errorTable, ruid_col /* column which univocally identifies a row in the error table */, 
	BR_errorTable) 
	/ store source des="Convert the DEAV error table into a format suitable for business rules";

	proc sort data=&DEAV_errorTable.;
		by &ruid_col. ERR_CODE ERR_MESSAGE ERR_TYPE;
	run;

	proc transpose data=&DEAV_errorTable.
	        out=&BR_errorTable.(drop=_NAME_);
	  id ERR_COLUMN;
	  by &ruid_col. ERR_CODE ERR_MESSAGE ERR_TYPE;
	  var ERR_VALUE;
	run;

	data &BR_errorTable.;
		set &BR_errorTable.;
		BR_CODE = ERR_CODE;
		INFO_TYPE = ERR_TYPE;
		INFO_MESSAGE = ERR_MESSAGE;

		drop ROW_ID ERR_CODE ERR_TYPE ERR_MESSAGE;
	run;
%mend;
/*
%macro DEAV_BR_ENRICH_AND_VALIDATE(datasetId=);

%mend;
*/
%macro DEAV_ENRICH_AND_VALIDATE(
	inputTable=, 
	outputTable=,
	action=/* Check https://github.com/openefsa/enrichment-validation-step/blob/master/README.md for action details */, 
	foodex2Column=, 
	foodex2Hierarchy=,
	matrixColumn=) / store source des="Enrich and validate a dataset";

	%if &inputTable. ^= %str() and &outputTable. ^= %str() and &action. ^= %str() %then %do;

		%put NOTE: Enrichment and validation started with input table = &inputTable. error table = &outputTable.;
		%put NOTE: Performing action = &action.;

		/* If the foodex2 validation is required */
		%if &action. = "FOODEX2_VALIDATION" %then %do;
			%if &foodex2Column. ^= %str() %then %do;

				%put NOTE: Validating FoodEx2 column = &foodex2Column.;

				%DEAV_FOODEX2_VALIDATION(&inputTable., &outputTable., &foodex2Column., &foodex2Hierarchy.);
			%end;
			%else %put ERROR: Cannot validate FoodEx2, missing parameter foodex2Column;
		%end;

		/* If foodex2 => matrix mapping is required */
		%else %if &action. = "FOODEX2_TO_MATRIX" %then %do;
			%if &foodex2Column. ^= %str() and &matrixColumn. ^= %str() %then %do;

				%put NOTE: Mapping FoodEx2 column = &foodex2Column. to matrix column = &matrixColumn.;

				
			%end;
			%else %put ERROR: Cannot map FoodEx2 to matrix, either foodex2Column or matrixColumn is missing;
		%end;

		/* If not found */
		%else %do;
			%put ERROR: Unknown action = &action.;
		%end;
	%end;
	%else %put ERROR: Cannot apply enrichment and validation. Missing at least one parameter among inputTable, errorTable and action;
%mend DEAV_ENRICH_AND_VALIDATE;