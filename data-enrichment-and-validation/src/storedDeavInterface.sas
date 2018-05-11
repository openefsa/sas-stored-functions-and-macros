
options mstored sasmstore=MSTORE;
%macro DEAV_INT_TO_BR_ERRORS(DEAV_errorTable, dcfId, datasetId, 
	BR_errorTable)
	/ store source des="Convert the DEAV error table into a format suitable for business rules";

	proc sort data=&DEAV_errorTable.;
		by &dcfId. &datasetId. ERR_CODE ERR_MESSAGE ERR_TYPE;
	run;

	proc transpose data=&DEAV_errorTable.
	        out=&BR_errorTable.(drop=_NAME_);
	  id ERR_COLUMN;
	  by &dcfId. &datasetId. ERR_CODE ERR_MESSAGE ERR_TYPE;
	  var ERR_VALUE;
	run;

	data &BR_errorTable.;
		set &BR_errorTable.;
		BR_CODE = ERR_CODE;
		INFO_TYPE = ERR_TYPE;
		INFO_MESSAGE = ERR_MESSAGE;

		drop ROW_ID ERR_CODE ERR_TYPE ERR_MESSAGE;
	run;

	/* Remove empty rows created by the transposition */
	proc sql;
		delete from &BR_errorTable.
	 	where BR_CODE is null and INFO_TYPE is null and INFO_MESSAGE is null;
	run;
%mend;

%macro DEAV_INT_MOVE_TO_ORACLE(inputTable=, oracleTable=, dcfId=, datasetId=, enrichedCols=)
	/ store source des="Move the enriched columns to a transposed oracle table which can be used by the ETL";

	/* Get only the enriched columns with dcf id and dataset id */
	data DEAV_ENRICHED_COLS;
		set &inputTable.;
		keep &dcfId. &datasetId. &enrichedCols.;
	run;

	/* sort for transposing */
	proc sort data=DEAV_ENRICHED_COLS;
		by &dcfId. &datasetId.;
	run;

	/* Transpose the enriched columns */
	proc transpose data=DEAV_ENRICHED_COLS out=T_DEAV_ENRICHED_COLS(rename=(col1=VALUE)) name=F_C_NAME;
		by &dcfId. &datasetId.;
		var &enrichedCols.;
	run;

	%local maxId;
	%let maxId = 0;

	/* If already present, remove it since it will be replaced */
	%if %sysfunc(exist(&oracleTable.)) %then %do;

		/* Get max to set new IDs for the new rows */
		proc sql;
			select max(ID) into :maxId from &oracleTable.;
		run;

		/* Delete old rows if related to the same dataset */
		proc sql;
			delete from &oracleTable.
			where &datasetId. in (select distinct &datasetId. from &inputTable)
				and F_C_NAME in (
					%let i = 1;
					%do %while (%scan(&enrichedCols., &i., %str( ))^=%str());
						%let col = %scan(&enrichedCols., &i., %str( ));
						"&col."
						
						%let i = %eval(&i. + 1);

						%if %scan(&enrichedCols., &i., %str( ))^=%str() %then ,;
					%end;
				);
		run;
	%end;

	/* Add additional columns. NOTE THAT THIS DOES NOT SUPPORT COMPOUND FIELDS!
	 * EDIT THIS PART OF CODE IN ORDER TO MAKE IT COMPATIBLE WITH COMPOUNDS.
	 * In particular it is necessary to split compound fields into several records
	 * , to assign different sorting and attr_name. */
	data T_DEAV_ENRICHED_COLS;

		retain ID F_ID F_C_NAME ATTR_NAME VALUE SORTING DATASET_ID;

		set T_DEAV_ENRICHED_COLS (rename=(&dcfId.=F_ID &datasetId.=DATASET_ID));

		ID = _N_ + &maxId.;
		
		length ATTR_NAME $4000;
		SORTING = 1;

		label ID = 'Row id' F_ID = 'Dcf id' F_C_NAME = 'Column name' ATTR_NAME = 'Attribute name for compounds'
			VALUE = 'Column/attribute value' SORTING = 'Order of application of compound attributes' DATASET_ID = 'Dataset id';
	run;

	/* Append transposed data to oracle table */
	%appendDataset(T_DEAV_ENRICHED_COLS, &oracleTable.);
	
	/* Delete temporary tables */
	proc sql;
		drop table DEAV_ENRICHED_COLS;
		drop table T_DEAV_ENRICHED_COLS;
	run;
%mend;

/* Get a parameter from a list (comma separated) using the name as identifier. For example:

	%let names=a,b,c;
	%let values=1,3,4;

	%DEAV_INT_GET_PARAMETER(names=%quote(&names.),values=%quote(&values.),name=b);

	this will return 3
*/
%macro DEAV_INT_GET_PARAMETER(names=, values=, name=) / store source des="Get a parameter value by name from a list";

	%local i;
	%local currentName;
	%local found;

	%let i = 0;
	%let currentName = start;
	%let found = 0;

	%do %while(&currentName. ^= %str());

		%let i = %eval(&i. + 1);
		%let currentName = %scan(%quote(&names.), &i., %str(,));

		/* if match stop */
		%if &currentName. = &name. %then %do;
			%let currentName = %str();
			%let found = 1;
		%end;
	%end;
	
	/* Return the i-th value if found */
	%if &found. %then %scan(%quote(&values.), &i., %str(,));
%mend;

%macro DEAV_ENRICH_AND_VALIDATE(
	inputTable=, 
	outputTable=,
	action=, 
	parNames=,
	parValues=,
	dcfId=ID,
	datasetId=DATASET_ID) / store source des="Enrich and validate a dataset";

	%local isValid;
	%let isValid = 1;

	%local enrichedCols; /* Set for each enrichment the list space separated of columns which will be created (this will be used to transpose the output table) */

	%if &inputTable. ^= %str() and &outputTable. ^= %str() and &action. ^= %str() %then %do;

		%put NOTE: Enrichment and validation started with input table = &inputTable. error table = &outputTable.;
		%put NOTE: Performing action = &action., parameters = &parNames. values = &parValues.;

		/* If the foodex2 validation is required */
		%if &action. = "FOODEX2_VALIDATION" %then %do;

			%let foodex2Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Column);
			%let foodex2Hierarchy = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Hierarchy);

			%if &foodex2Column. ^= %str() %then %do;

				%put NOTE: Validating FoodEx2 column = &foodex2Column.;
				%DEAV_FOODEX2_VALIDATION(inputTable=&inputTable., outputTable=&outputTable., idColumns=&dcfId. &datasetId.,
					foodex2Column=&foodex2Column., foodex2Hierarchy=&foodex2Hierarchy.);
			%end;
			%else %put ERROR: Cannot validate FoodEx2, missing parameter foodex2Column;
		%end;

		/* If foodex2 => matrix mapping is required */
		%else %if &action. = "FOODEX2_TO_MATRIX" %then %do;

			%let foodex2Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Column);
			%let matrixColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=matrixColumn);

			%if &foodex2Column. ^= %str() and &matrixColumn. ^= %str() %then %do;

				%put NOTE: Mapping FoodEx2 column = &foodex2Column. to matrix column = &matrixColumn.;
				%let enrichedCols = &matrixColumn.;
	
				%DEAV_FOODEX2_TO_MATRIX(inputTable=&inputTable., outputTable=&outputTable., 
					foodex2Column=&foodex2Column., matrixColumn=&matrixColumn., idColumns=&dcfId. &datasetId.);
			%end;
			%else %put ERROR: Cannot map FoodEx2 to matrix, either foodex2Column or matrixColumn is missing. Found FoodEx = &foodex2Column. Matrix = &matrixColumn.;
		%end;

		/* If not found */
		%else %do;
			%put ERROR: Unknown action = &action.;
			%let isValid = 0;
		%end;

		/* If valid macro call convert error table into BR format */
		%if &isValid. %then %DEAV_INT_TO_BR_ERRORS(&outputTable., &dcfId., &datasetId., &outputTable.);

		/* If valid macro call for enrichment, append/create table also in ORACLE */
		%if &isValid. and &enrichedCols. ^= %str() %then %do;
			%DEAV_INT_MOVE_TO_ORACLE(inputTable=&inputTable., oracleTable=DEAV_ENRICHMENT_TABLE,
					dcfId=&dcfId., datasetId=&datasetId., enrichedCols=&enrichedCols.);
		%end;
	%end;
	%else %put ERROR: Cannot apply enrichment and validation. Missing at least one parameter among inputTable, errorTable and action;
%mend DEAV_ENRICH_AND_VALIDATE;