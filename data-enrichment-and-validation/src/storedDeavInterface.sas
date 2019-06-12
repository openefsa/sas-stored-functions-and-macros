
options mstored sasmstore=MSTORE;

%macro DEAV_INT_TO_BR_ERRORS(DEAV_errorTable, dcfId, datasetId, uniqueIdentifier,
	BR_errorTable)
	/ store source des="Convert the DEAV error table into a format suitable for business rules";

	proc sort data=&DEAV_errorTable.;
		by &dcfId. &datasetId. &uniqueIdentifier. ERR_CODE ERR_MESSAGE ERR_TYPE;
	run;

	proc transpose data=&DEAV_errorTable.
	        out=&BR_errorTable.(drop=_NAME_);
	  id ERR_COLUMN;
	  by &dcfId. &datasetId. &uniqueIdentifier. ERR_CODE ERR_MESSAGE ERR_TYPE;
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

%MACRO getLoginsDomain_fromLib (libname, macroout) / store source des="Returns the authorization domain of the specified oracle library";


%logins;

%global &macroout;

proc sql noprint;
	select AuthDomName into :&macroout. trimmed
	from userAut 
	where libname="&libname.";
quit;

%if &sqlobs=0 %then %put Auth domain not found for library &libname.;



%MEND getLoginsDomain_fromLib;





%macro DEAV_INT_MOVE_TO_ORACLE(inputTable=, oracleTable=, dcfId=, datasetId=, uniqueIdentifier=, enrichedCols=)
	/ store source des="Move the enriched columns to a transposed oracle table which can be used by the ETL";

	/* Get only the enriched columns with dcf id and dataset id */
	data DEAV_ENRICHED_COLS;
		set &inputTable.;
		keep &dcfId. &datasetId. &uniqueIdentifier. &enrichedCols.;
	run;

	/* sort for transposing */
	proc sort data=DEAV_ENRICHED_COLS;
		by &dcfId. &datasetId. &uniqueIdentifier.;
	run;

	/* Transpose the enriched columns */
	proc transpose data=DEAV_ENRICHED_COLS out=T_DEAV_ENRICHED_COLS(rename=(col1=VALUE)) name=F_C_NAME;
		by &dcfId. &datasetId. &uniqueIdentifier.;
		var &enrichedCols.;
	run;
	

	/****************************/
	/*if the column value is numeric then it will be converted to string*/
	proc contents data=T_DEAV_ENRICHED_COLS out=content_deav;
	run;
	%let value_type=0;

	proc sql noprint;
	select type into :value_type
	from content_deav
	where name="VALUE";
	quit;


	%if  &value_type.=1 %then %do;
		data T_DEAV_ENRICHED_COLS;
		set T_DEAV_ENRICHED_COLS;
		   new = left(put(VALUE, best.));
		   drop VALUE;
		   rename new=VALUE;
		run;
	%end;
	/****************************/




	%local maxId;
	%let maxId = 0;

	/* If already present, remove it since it will be replaced */
	%if %sysfunc(exist(&oracleTable.)) %then %do;

		/* Get max to set new IDs for the new rows */
		proc sql noprint;
			select coalesce(max(ID),0) into :maxId from &oracleTable.;
		run;

		/* Delete old rows if related to the same dataset */

		%getLoginsDomain_fromLib (%scan(&oracleTable.,1,%str(.)),oracle_auth);

		proc sql noprint;

			select distinct &datasetId. into :ds_list separated by ','
			from &inputTable  
			;

		quit;


		proc sql;

			connect to ORACLE
			(
			   DBMAX_TEXT=32000 PATH=PRDDCDWH AUTHDOMAIN="&oracle_auth."
			      DBSERVER_MAX_BYTES=1 ADJUST_BYTE_SEMANTIC_COLUMN_LENGTHS=NO DBCLIENT_MAX_BYTES=1

			);

			execute (
					delete from %scan(&oracleTable.,2,%str(.))
					where &datasetId. in (&ds_list.)
						and F_C_NAME in (
							%let i = 1;
							%do %while (%scan(&enrichedCols., &i., %str( ))^=%str());
								%let col = %scan(&enrichedCols., &i., %str( ));
								%str(%')&col.%str(%')
								
								%let i = %eval(&i. + 1);

								%if %scan(&enrichedCols., &i., %str( ))^=%str() %then ,;
							%end;
						)
		) by oracle;

		disconnect from oracle;

		quit;



		/******* modified 20/05/2019 **********/
		/*proc sql;
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
		run;*/
	%end;

	/* Add additional columns. NOTE THAT THIS DOES NOT SUPPORT COMPOUND FIELDS!
	 * EDIT THIS PART OF CODE IN ORDER TO MAKE IT COMPATIBLE WITH COMPOUNDS.
	 * In particular it is necessary to split compound fields into several records
	 * , to assign different sorting and attr_name. */
	data T_DEAV_ENRICHED_COLS;

		retain ID F_ID F_C_NAME ATTR_NAME VALUE SORTING DATASET_ID RECORD_UNIQUE_IDENTIFIER;

		set T_DEAV_ENRICHED_COLS (rename=(&dcfId.=F_ID &datasetId.=DATASET_ID &uniqueIdentifier.=RECORD_UNIQUE_IDENTIFIER));

		ID = _N_ + &maxId.;
		
		length ATTR_NAME $4000;
		SORTING = 1;

		label ID = 'Row id' F_ID = 'Dcf id' F_C_NAME = 'Column name' ATTR_NAME = 'Attribute name for compounds'
			VALUE = 'Column/attribute value' SORTING = 'Order of application of compound attributes' DATASET_ID = 'Dataset id' 
			RECORD_UNIQUE_IDENTIFIER="Record unique identifier";
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




/***************************************************************************/

/*GDP: added 05-06-2019*/

/*
Create a macro whose parameters are:
a.	name of the input table
b.	name of the column containing the dataset ID (default DATASET_ID)
c.	name of the column containing the IDs of DCF (default ID)
d.	name of variables to serach in the enrichemnt table (comme separated)
e.	FACOLTATIVO: name of the output columns, one for each variable to be searched. If not reported, this field is set to be identical to the previous one.
f.	name of the enrichment table (default value ODS.DEAV_ENRICHMENT_TABLE)

Tha macro will transpose the enrichment table and recover the value of the variables requested.
It will add the new columns (with name specified in the not mandatory field) to the input table.
Hence, the output of the macro is the input itself.

This macro is then inserted in the interface DEAV, adding the the previous version just one parameter: 
the ID of the new dataset for which the enrichement has to be run. While for the remaining datasets the macro runs interface runs just the retrieval of the enrichement (it should be already performed).
If this new parameter is not reported, then the interface will act has the previous version (hence, it will run the enrichment for the entire input table).

*/



%macro DEAV_GET_ENRICHMENT(
	inputTable=, 
	parNames=,
	parNamesOut=,
	dcfId=ID,
	datasetId=DATASET_ID,
	oracleTable=ODS.DEAV_ENRICHMENT_TABLE
	) / store source des="Get the enrichment of already enriched datasets";

	options fmtsearch=(FMTLIB BRS_STG); /* Required to use BR formats for parents */
	options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.dcf MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables MSTORE.MAPPING_MATRIX MSTORE.Get_Domains);

	proc sql noprint;
		select distinct &datasetId. into :dataset_ids separated by ','
		from &inputTable.
		;
	quit;

	proc transpose data=&oracleTable.(where=(dataset_id in  (&dataset_ids.))) out=transp(drop=_name_ _label_);
		by F_ID dataset_ID;
		ID f_c_name;
		var value;
	quit;

/* it would be better to check whether the parnameout are not already names of columns of the input table*/
	%let i = 0;
	%let currentName = start;
	%let found = 0;
	%let select_var=;

	%if "&parNamesOut."^="" %then %do;
		%do %while(&currentName. ^= %str());

			%let i = %eval(&i. + 1);
			%let currentName = %scan(%quote(&parNames.), &i., %str(,));
			%let currentNameOut = %scan(%quote(&parNamesOut.), &i., %str(,));
			%if &currentName. ^= %str() %then %do;
				%let select_var=&select_var., t2.&currentName. as &currentNameOut. ;
			%end;
		%end;
	%end;
	%else %do;
		%do %while(&currentName. ^= %str());

			%let i = %eval(&i. + 1);
			%let currentName = %scan(%quote(&parNames.), &i., %str(,));
			%if &currentName. ^= %str() %then %do;
				%let select_var=&select_var., t2.&currentName.;
			%end;
		%end;
	%end;

	proc sql;
		create table &inputTable. as
		select t1.*  &select_var.
		from &inputTable. as t1
			left join transp as t2 
				on (t1.&datasetId.=t2.dataset_ID and t1.&dcfId.=t2.F_ID)
		;
	quit;
			

%mend DEAV_GET_ENRICHMENT;






%macro DEAV_ENRICH_AND_VALIDATE(
	inputTable=, 
	outputTable=,
	action=, 
	parNames=,
	parValues=,
	dcfId=ID,
	datasetId=DATASET_ID,
	uniqueIdentifier=,
	currentDatasetId=NULL) / store source des="Enrich and validate a dataset";

	options fmtsearch=(FMTLIB BRS_STG); /* Required to use BR formats for parents */
	options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.dcf MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables MSTORE.MAPPING_MATRIX MSTORE.Get_Domains);

	%local isValid;
	%local idColumns;

	%let isValid = 1;
	%let idColumns=&dcfId. &datasetId. &uniqueIdentifier.;

	%local enrichedCols; /* Set for each enrichment the list space separated of columns which will be created (this will be used to transpose the output table) */








	%if &inputTable. ^= %str() and &outputTable. ^= %str() and &action. ^= %str() %then %do;
	
		%if &currentDatasetId.^=NULL %then %do;

			data _DEAV_currentDS _DEAV_previousDS;
				set &inputTable.;
				if &datasetId. in (&currentDatasetId.) then output _DEAV_currentDS;
				else output _DEAV_previousDS;
			run;
			%let currentDS=_DEAV_currentDS;

		%end;
		%else %do;
			
			%let currentDS=&inputTable.;

		%end;
			


		%put NOTE: Enrichment and validation started with input table = &inputTable. error table = &outputTable.;
		%put NOTE: Performing action = &action., parameters = &parNames. values = &parValues.;

		/* If the foodex2 validation is required */
		%if &action. = "FOODEX2_VALIDATION" %then %do;

			%let foodex2Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Column);
			%let foodex2Hierarchy = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Hierarchy);

			%if &foodex2Column. ^= %str() %then %do;

				options nomprint;
				%put NOTE: Validating FoodEx2 column = &foodex2Column.;
				%DEAV_FOODEX2_VALIDATION(inputTable=&currentDS., outputTable=&outputTable., idColumns=&idColumns.,
					foodex2Column=&foodex2Column., foodex2Hierarchy=&foodex2Hierarchy.);
				options mprint;
			%end;
			%else %put ERROR: Cannot validate FoodEx2, missing parameter foodex2Column;
		%end;
			
		/* If foodex2 => matrix mapping is required */
		%else %if &action. = "FOODEX2_TO_MATRIX" %then %do;

			%let foodex2Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Column);
			%let matrixColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=matrixColumn);
			%let prodTrColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=prodTrColumn);
			%let prodMdColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=prodMdColumn);
			%let prodPacColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=prodPacColumn);
			%let foodex1Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex1Column);


			%if &foodex2Column. ^= %str() and &matrixColumn. ^= %str() %then %do;

				%put NOTE: Mapping FoodEx2 column = &foodex2Column. to matrix column = &matrixColumn.;
				%let enrichedCols = &matrixColumn. &prodTrColumn. &prodMdColumn. &prodPacColumn. &foodex1Column.;
			
				%DEAV_FOODEX2_TO_MATRIX(inputTable=&currentDS., outputTable=&outputTable., 
					foodex2Column=&foodex2Column.,
					matrixColumn=&matrixColumn.,
					prodTrColumn=&prodTrColumn.,
					prodMdColumn=&prodMdColumn.,
					prodPacColumn=&prodPacColumn.,
					foodex1Column=&foodex1Column.,
					idColumns=&idColumns.);
			%end;
			%else %put ERROR: Cannot map FoodEx2 to matrix, either foodex2Column or matrixColumn is missing. Found FoodEx = &foodex2Column. Matrix = &matrixColumn.;
		%end;

		%else %if &action. = "GET_CHEM_MON_DOMAIN" %then %do;

			%let ProgLegRefColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=progLegRefColumn);
			%let ParamColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=paramColumn);
			%let is_VetColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isVetColumn);
			%let is_AddColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isAddColumn);
			%let is_PestColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isPestColumn);
			%let is_ChemColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isOccColumn);


			%if &ProgLegRefColumn. ^= %str() and &ParamColumn. ^= %str() %then %do;

				%put NOTE: Mapping LegRef column = &ProgLegRefColumn. and Param column = &ParamColumn. to domains;
				%let enrichedCols = &is_VetColumn. &is_AddColumn. &is_PestColumn. &is_ChemColumn. ;
	

				%DEAV_GetChemMonDomain(inputTable=&currentDS., 
						outputTable=&outputTable., 
						ProgLegRefColumn=&ProgLegRefColumn.,
						ParamColumn=&ParamColumn.,
						is_VetColumn=&is_VetColumn.,
						is_AddColumn=&is_AddColumn.,
						is_PestColumn=&is_PestColumn.,
						is_ChemColumn=&is_ChemColumn.,
						idColumns=&idColumns.);



			%end;
			%else %put ERROR: Cannot map LegRef and Param to Domains, either ProgLegRefColumn or ParamColumn is missing. Found ProgLegRefColumn = &ProgLegRefColumn. ParamColumn = &ParamColumn.;
		%end;



		/* If not found */
		%else %do;
			%put ERROR: Unknown action = &action.;
			%let isValid = 0;
		%end;

		/* If valid macro call convert error table into BR format */
		%if %sysfunc(exist(&outputTable.)) %then %do;
			%if &isValid. %then %DEAV_INT_TO_BR_ERRORS(&outputTable., &dcfId., &datasetId., &uniqueIdentifier., &outputTable.);
		%end;


		/* If valid macro call for enrichment, append/create table also in ORACLE */
		%if &isValid. and &enrichedCols. ^= %str() %then %do;
			%DEAV_INT_MOVE_TO_ORACLE(inputTable=&currentDS., oracleTable=ODS.DEAV_ENRICHMENT_TABLE,
					dcfId=&dcfId., datasetId=&datasetId., uniqueIdentifier=&uniqueIdentifier., enrichedCols=&enrichedCols.);
		%end;


		%if &currentDatasetId.^=NULL %then %do;
			
			
			%let enrichedColsComma= %sysfunc(tranwrd(&enrichedCols.,%str( ),%str(,)));
			
			%DEAV_GET_ENRICHMENT(
				inputTable=_DEAV_previousDS, 
				parNames=%nrbquote(&enrichedColsComma.),
				parNamesOut=%nrbquote(&enrichedColsComma.)
				);
			
			%if &action. = "GET_CHEM_MON_DOMAIN" %then %do;

				data _DEAV_previousDS;

					set _DEAV_previousDS;

					%let c=1;
					%do %while (%scan(%nrbquote(&enrichedColsComma.),&c.,%str(,))^=%str());
						%scan(%nrbquote(&enrichedColsComma.),&c.,%str(,))_int = input(%scan(%nrbquote(&enrichedColsComma.),&c.,%str(,)),best.);
						drop %scan(%nrbquote(&enrichedColsComma.),&c.,%str(,));
						rename %scan(%nrbquote(&enrichedColsComma.),&c.,%str(,))_int=%scan(%nrbquote(&enrichedColsComma.),&c.,%str(,));
						%let c=%eval(&c.+1);
					%end;

				run;
					
			%end;

			data &inputTable.;
				set _DEAV_previousDS _DEAV_currentDS;
			run;

		%end;




	%end;
	%else %put ERROR: Cannot apply enrichment and validation. Missing at least one parameter among inputTable, errorTable and action;
%mend DEAV_ENRICH_AND_VALIDATE;


/* old version replaced 07-06-2019 */
/*
%macro DEAV_ENRICH_AND_VALIDATE(
	inputTable=, 
	outputTable=,
	action=, 
	parNames=,
	parValues=,
	dcfId=ID,
	datasetId=DATASET_ID,
	uniqueIdentifier=) / store source des="Enrich and validate a dataset";

	options fmtsearch=(FMTLIB BRS_STG); 
	options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.dcf MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables MSTORE.MAPPING_MATRIX MSTORE.Get_Domains);

	%local isValid;
	%local idColumns;

	%let isValid = 1;
	%let idColumns=&dcfId. &datasetId. &uniqueIdentifier.;

	%local enrichedCols; 

	%if &inputTable. ^= %str() and &outputTable. ^= %str() and &action. ^= %str() %then %do;

		%put NOTE: Enrichment and validation started with input table = &inputTable. error table = &outputTable.;
		%put NOTE: Performing action = &action., parameters = &parNames. values = &parValues.;

		

		%if &action. = "FOODEX2_VALIDATION" %then %do;

			%let foodex2Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Column);
			%let foodex2Hierarchy = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Hierarchy);

			%if &foodex2Column. ^= %str() %then %do;

				options nomprint;
				%put NOTE: Validating FoodEx2 column = &foodex2Column.;
				%DEAV_FOODEX2_VALIDATION(inputTable=&inputTable., outputTable=&outputTable., idColumns=&idColumns.,
					foodex2Column=&foodex2Column., foodex2Hierarchy=&foodex2Hierarchy.);
				options mprint;
			%end;
			%else %put ERROR: Cannot validate FoodEx2, missing parameter foodex2Column;
		%end;
			
	

		%else %if &action. = "FOODEX2_TO_MATRIX" %then %do;

			%let foodex2Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex2Column);
			%let matrixColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=matrixColumn);
			%let prodTrColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=prodTrColumn);
			%let prodMdColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=prodMdColumn);
			%let prodPacColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=prodPacColumn);
			%let foodex1Column = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=foodex1Column);


			%if &foodex2Column. ^= %str() and &matrixColumn. ^= %str() %then %do;

				%put NOTE: Mapping FoodEx2 column = &foodex2Column. to matrix column = &matrixColumn.;
				%let enrichedCols = &matrixColumn. &prodTrColumn. &prodMdColumn. &prodPacColumn. &foodex1Column.;
			
				%DEAV_FOODEX2_TO_MATRIX(inputTable=&inputTable., outputTable=&outputTable., 
					foodex2Column=&foodex2Column.,
					matrixColumn=&matrixColumn.,
					prodTrColumn=&prodTrColumn.,
					prodMdColumn=&prodMdColumn.,
					prodPacColumn=&prodPacColumn.,
					foodex1Column=&foodex1Column.,
					idColumns=&idColumns.);
			%end;
			%else %put ERROR: Cannot map FoodEx2 to matrix, either foodex2Column or matrixColumn is missing. Found FoodEx = &foodex2Column. Matrix = &matrixColumn.;
		%end;

		%else %if &action. = "GET_CHEM_MON_DOMAIN" %then %do;

			%let ProgLegRefColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=progLegRefColumn);
			%let ParamColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=paramColumn);
			%let is_VetColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isVetColumn);
			%let is_AddColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isAddColumn);
			%let is_PestColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isPestColumn);
			%let is_ChemColumn = %DEAV_INT_GET_PARAMETER(names=&parNames.,values=&parValues.,name=isOccColumn);


			%if &ProgLegRefColumn. ^= %str() and &ParamColumn. ^= %str() %then %do;

				%put NOTE: Mapping LegRef column = &ProgLegRefColumn. and Param column = &ParamColumn. to domains;
				%let enrichedCols = &is_VetColumn. &is_AddColumn. &is_PestColumn. &is_ChemColumn. ;
	

				%DEAV_GetChemMonDomain(inputTable=&inputTable., 
						outputTable=&outputTable., 
						ProgLegRefColumn=&ProgLegRefColumn.,
						ParamColumn=&ParamColumn.,
						is_VetColumn=&is_VetColumn.,
						is_AddColumn=&is_AddColumn.,
						is_PestColumn=&is_PestColumn.,
						is_ChemColumn=&is_ChemColumn.,
						idColumns=&idColumns.);



			%end;
			%else %put ERROR: Cannot map LegRef and Param to Domains, either ProgLegRefColumn or ParamColumn is missing. Found ProgLegRefColumn = &ProgLegRefColumn. ParamColumn = &ParamColumn.;
		%end;



	

		%else %do;
			%put ERROR: Unknown action = &action.;
			%let isValid = 0;
		%end;


		%if %sysfunc(exist(&outputTable.)) %then %do;
			%if &isValid. %then %DEAV_INT_TO_BR_ERRORS(&outputTable., &dcfId., &datasetId., &uniqueIdentifier., &outputTable.);
		%end;



		%if &isValid. and &enrichedCols. ^= %str() %then %do;
			%DEAV_INT_MOVE_TO_ORACLE(inputTable=&inputTable., oracleTable=ODS.DEAV_ENRICHMENT_TABLE,
					dcfId=&dcfId., datasetId=&datasetId., uniqueIdentifier=&uniqueIdentifier., enrichedCols=&enrichedCols.);
		%end;
	%end;
	%else %put ERROR: Cannot apply enrichment and validation. Missing at least one parameter among inputTable, errorTable and action;
%mend DEAV_ENRICH_AND_VALIDATE;
*/


