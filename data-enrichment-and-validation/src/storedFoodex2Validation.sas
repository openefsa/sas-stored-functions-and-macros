
options mstored sasmstore=MSTORE;
options fmtsearch=(FMTLIB BRS_STG); /* Required to use BR formats for parents */
options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables);

/*
 #==================================================================
 # FoodEx2 validation - CONFIGURATION
 # In this section it is possible to configure the error messages 
 # (it is needed to store again the macro after editing)
 #==================================================================
 */


/* Set the error messages for each type of error (see documentation for error codes) */
/* Get the error message starting from the code */
%macro DEAV_INT_GET_ERROR_MESSAGE(errorCode) / store source des="Mapping between FoodEx2 validation error code and message";

	%if(&errorCode = "FOODEX2.01") %then "Single source commodity is not allowed in raw primary commodity. Only multiple source commodities are allowed, for mixed raw commodities";
	%else %if(&errorCode = "FOODEX2.02") %then "The source facet is not allowed in mixed derivatives";
	%else %if(&errorCode = "FOODEX2.03") %then "The source facet is not allowed in composite food";
	%else %if(&errorCode = "FOODEX2.04") %then "The source commodity facet is not allowed in composite food";
	%else %if(&errorCode = "FOODEX2.05") %then "Reporting source commodities which are not children of the implicit one is not allowed. Please use the generic derivative for describing a mixed derivative.";
	%else %if(&errorCode = "FOODEX2.06") %then "The source facet is not allowed for derivatives without the (single) source commodity";
	%else %if(&errorCode = "FOODEX2.07") %then "Reporting more than one facet is forbidden for this category";
	%else %if(&errorCode = "FOODEX2.17") %then "Reporting a facet as base term is forbidden";

	%else %if(&errorCode = "FOODEX2.08") %then "The use of not reportable terms is forbidden";
	%else %if(&errorCode = "FOODEX2.09") %then "The use of hierarchies as base term is discouraged";
	%else %if(&errorCode = "FOODEX2.10") %then "The use of non-specific terms is discouraged";
	%else %if(&errorCode = "FOODEX2.11") %then "The use of generic terms is discouraged";
	%else %if(&errorCode = "FOODEX2.12") %then "Ingredient facet can only be used as minor ingredient for derivatives";
	%else %if(&errorCode = "FOODEX2.13") %then "The source facet is allowed for derivatives with only one source commodity just for better specifying the raw source";
	%else %if(&errorCode = "FOODEX2.14") %then "Some explicit descriptors were added twice for the same facet category";
	%else %if(&errorCode = "FOODEX2.15") %then "Reporting implicit facets is discouraged";
	%else %if(&errorCode = "FOODEX2.16") %then "Reporting facets less detailed than the implicit facets is discouraged";
	%else %if(&errorCode = "FOODEX2.18") %then "The use of ambiguous terms is discouraged";
	%else %if(&errorCode = "FOODEX2.19") %then "The reported processes cannot be applied to the raw commodity. The existing derivative must be reported.";
	%else %if(&errorCode = "FOODEX2.20") %then "The reported term cannot be used since it is deprecated.";

	%else %do;
		%put ERROR: No error label found for error with code = &errorCode.;
		"No error label found for error with code = " || &errorCode.;
	%end;

%mend;

/*
 #==================================================================
 # FoodEx2 validation - CORE PROCESS
 # This process validates a list of FoodEx2 codes giving as output
 # a list of errors and warnings. Use the macro validateFoodex2
 # to call the validation.
 # @author Valentino Avon
 #==================================================================
 */

/* Used with getForbiddenProcesses function */
%macro DEAV_INT_GET_FORBIDDEN_PROCS / store source des="Used in the getForbiddenProcesses FCMP function. Do not use directly!";

	%let configTable = %sysfunc(dequote(&configTable.));

	proc sql noprint;
		select FORBIDDEN_PROCS into :procs separated by '#'
		from &configTable.
		where HIERARCHY = &hierarchy. and ROOT_GROUP_CODE = &parentCode.;
	run;
%mend;

proc fcmp outlib = MSTORE.FOODEX2_VALIDATION.FOODEX2_19;

/* Get the forbidden processes related to a term in a specific hierarchy. It is necessary to specify
  also the level (tree depth) in which the term is present and the configuration table from
  which the forbidden processes are taken */
function getForbiddenProcesses(termCode $, termLevel, hierarchy $, configTable $) $4000;

	length procs $ 4000;

	parentLevel = termLevel - 1;

	do until (parentLevel = 0);
		parentCode = getAncestorAtLevel(termCode, hierarchy, parentLevel);
		parentLevel = parentLevel - 1;
		rc = run_macro('DEAV_INT_GET_FORBIDDEN_PROCS', parentCode, hierarchy, configTable, procs);
		if (^missing(procs)) then parentLevel = 0; /* exit from the loop */
	end;

	return(procs);
endsub;

/* Check if the considered process is forbidden for the chosen base term. It returns the forbidden process group
 if found or empty if not found. */
function areProcessesForbidden(term $, termLevel, hierarchy $, processes $ /* list of processes $ separated */, 
	configTable $) $4000;

	reportedProcs = translate(processes, " ", "$"); /* put spaces instead of $ otherwise indexw does not work */

	/* for each forbidden process group */
	%stringTokenizer(getForbiddenProcesses(term, termLevel, hierarchy, configTable), "#", procGroup)

		forbidden = 1;

		/* check if all the forbidden processes of the current group are reported */
		%stringTokenizer(procGroup, "$", proc)
			/* if the process is not present in the reported processes 
			Add the F28 header to the process code in the configuration since it is omitted*/
			if (indexw(reportedProcs, catx(".", "F28", proc)) = 0) then forbidden = 0;
		%endStringTokenizer

		/* if forbidden the entire proc group was reported */
		if forbidden then return(procGroup);

	%endStringTokenizer

	return(""); /* no forbidden group was reported */
endsub;
run;

/* Check facet cardinality and return errors if not respected */
%macro DEAV_INT_CHECK_ALL_FACETS_CARD(foodex2Col, facets, errorTable, outResult) / store source des="Check facets cardinality";
	
	&outResult. = 1;

	length headers $4000;
	length facetHeader $4000;

	headers = getFacetsHeaders();

	i = 1;
	do until (scan(headers, i, " ") = "");

		facetHeader = scan(headers, i, " ");

		/* if not respected add error to table */
		if ^checkFacetCardinality(&facets., facetHeader) then do;
			&outResult. = 0;
			%DEAV_INT_ADD_FX2_ERROR("FOODEX2.07", &foodex2Col., &errorTable., facetHeader);
		end;

		i = i + 1;
	end;

	drop i headers facetHeader;
%mend;

/* Clean the facets from repetitions and redundance */
%macro DEAV_INT_CLEAN_FACETS(foodex2Col, facets, implicitFacets, errorTable, outResult) / store source des="Clean a list of facets";

	length distinct_facets $ 1000;
	length filtered_facets $ 1000;
	length cleaned_facets $ 1000;
	length termCodes $ 1000;

	distinct_facets = getDistinctFacets(&facets.);

	/* If some facets are duplicated in the explicit */
	if (&facets. ^= distinct_facets) then do;
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.14", &foodex2Col., &errorTable., "");
	end;

	filtered_facets = removeElementsFromList(distinct_facets, &implicitFacets., "$");

	/* If some implicit facets were repeated in the explicit */
	if (filtered_facets ^= distinct_facets) then do;
		termCodes = removeElementsFromList(distinct_facets, filtered_facets, "$");
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.15", &foodex2Col., &errorTable., termCodes);
	end;

	cleaned_facets = removeRedundantExplicitFacets(filtered_facets, &implicitFacets.);

	/* If some explicit facets are parents of implicit facets */
	if (filtered_facets ^= cleaned_facets) then do;
		termCodes = removeElementsFromList(filtered_facets, cleaned_facets, "$");
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.16", &foodex2Col., &errorTable., termCodes);
	end;

	&outResult. = cleaned_facets;
%mend;

proc fcmp outlib = MSTORE.FOODEX2_VALIDATION.terms;

/* Check if current term in the data step is a generic term (it uses the configuration
to determine which are the generic terms)
*/
function isGeneric(termCode $);
	return(termCode in ("A0C0R", "A0CHR", "A0CHS"));
endsub;

/* Check if current term in the data step is an ambiguous term (it uses the configuration
to determine which are the ambiguous terms
*/
function isAmbiguous(termCode $);
	return(termCode in ("A00HQ"));
endsub;
run;

/* Add an error to the error table with foodex logic */
%macro DEAV_INT_WRITE_FX2_ERROR_TO(errorCode, foodexCol, errorType, errorTable, messagePrefix) / store source des="Add FOODEX2_VALIDATION error/warning to error table";

	%let foodexCol = %sysfunc(dequote(&foodexCol.));
	%let errorType = %sysfunc(dequote(&errorType.));

	%let suffix = %sysfunc(genRandomString(15)); /* Avoid multiple definitions */

	%let colsArray = cols&suffix.;
	%let valuesArray = values&suffix.;

	array &colsArray.(1) $4000 _temporary_;
	&colsArray.[1] = "&foodexCol.";

	array &valuesArray.(1) $4000 _temporary_;
	&valuesArray.[1] = &foodexCol.;

	length message $4000;

	%if %isMissing(&messagePrefix.) %then %do;
		message = %DEAV_INT_GET_ERROR_MESSAGE(&errorCode.);
	%end;
	%else %do;
		message = catt("(", &messagePrefix., ")", %DEAV_INT_GET_ERROR_MESSAGE(&errorCode.));
	%end;

	rc = writeErrorTo(_N_, &errorCode., message, "&errorType.", &errorTable., &colsArray., &valuesArray.);

	drop rc message;
%mend;

%macro DEAV_INT_ADD_FX2_ERROR(errorCode, foodexCol, errorTable, messagePrefix) / store source des="Add FOODEX2_VALIDATION error to error table";
	%DEAV_INT_WRITE_FX2_ERROR_TO(&errorCode., &foodexCol., "error", &errorTable., &messagePrefix.);
%mend;

%macro DEAV_INT_ADD_FX2_WARNING(errorCode, foodexCol, errorTable, messagePrefix) / store source des="Add FOODEX2_VALIDATION warning to error table";
	%DEAV_INT_WRITE_FX2_ERROR_TO(&errorCode., &foodexCol., "warning", &errorTable., &messagePrefix.);
%mend;


/*
Compute all the errors given a row composed of base term and facets and
the domain hierarchy
*/
%macro DEAV_INT_FX2V_EVAL_ERRORS(foodex_col /* name of the column which contains the initial foodex code */,
			base_col /* name of the column which contains the base term code */, 
			facets_col /* name of the column which contains all the explicit facets $ separated */, 
			implicit_facets_col /* name of the column which contains all the implicit facets $ separated */, 
			errorTable,
			foodex19ConfigTable /* configuration table for FOODEX2.19 check */,
			hierarchy /* The hierarchy of the domain */) / store source des="Check FOODEX2_VALIDATION errors";

	%local termTypeCol;
	%let termTypeCol = TERMTYPE;

	/* if base term is a facet */
	if (isFacet(&termTypeCol.)) then do;
		%DEAV_INT_ADD_FX2_ERROR("FOODEX2.17", "&foodex_col.", &errorTable., &base_col.);
	end;

	MERGED_FACETS = addImplicitFacets(&facets_col., &implicit_facets_col.);

	SOURCE_COMM_COUNT_MERGED = countFacets(MERGED_FACETS, "F27");
	SOURCE_COMM_COUNT = countFacets(&facets_col., "F27");

	/* Single F27 for RPC base term error (we count only the explicit since the RPC has
	 * in the implicit source commodities already itself, so we avoid to count it) */
	if (isRPC(&termTypeCol.) and SOURCE_COMM_COUNT = 1) then do;
		%DEAV_INT_ADD_FX2_ERROR("FOODEX2.01", "&foodex_col.", &errorTable., "F27");
	end;

	/* Check if base term is deprecated */
	if (isDeprecated(&base_col.)) then do;
		%DEAV_INT_ADD_FX2_ERROR("FOODEX2.20", "&foodex_col.", &errorTable., &base_col.);
	end;

	%iterateFacets(MERGED_FACETS, f, h, c)

		/* Check if facet is deprecated */
		if (isDeprecated(c)) then do;
			%DEAV_INT_ADD_FX2_ERROR("FOODEX2.20", "&foodex_col.", &errorTable., f);
		end;
		
		/* F01 for mixed derivative error */
		if (isDerivative(&termTypeCol.) and SOURCE_COMM_COUNT_MERGED > 1 and h = "F01") then do;
			%DEAV_INT_ADD_FX2_ERROR("FOODEX2.02", "&foodex_col.", &errorTable., f);
		end;

		/* F01 for composite base term error */
		if (isComposite(&termTypeCol.) and h = "F01") then do;
			%DEAV_INT_ADD_FX2_ERROR("FOODEX2.03", "&foodex_col.", &errorTable., f);
		end;

		/* F27 for composite base term error */
		if (isComposite(&termTypeCol.) and h = "F27") then do;
			%DEAV_INT_ADD_FX2_ERROR("FOODEX2.04", "&foodex_col.", &errorTable., f);
		end;

		/* FOODEX2.06 - F01 for derivative without F27 error */
		if (isDerivative(&termTypeCol.) and SOURCE_COMM_COUNT_MERGED = 0 and h = "F01") then do;
			%DEAV_INT_ADD_FX2_ERROR("FOODEX2.06", "&foodex_col.", &errorTable., f);
		end;

	%endIterateFacets

	/* FOODEX2.05 check if the source commodities are children of the implicit one (if present) */
	SOURCE_COMM_NOT_IMPL_CHILD = getExplicitNotChildOfImplicit(&facets_col., &implicit_facets_col., "F27");
	if (isDerivative(&termTypeCol.) and ^missing(SOURCE_COMM_NOT_IMPL_CHILD)) then do;
		%DEAV_INT_ADD_FX2_ERROR("FOODEX2.05", "&foodex_col.", &errorTable., SOURCE_COMM_NOT_IMPL_CHILD);
	end;
	
	/* If hierarchy dependent */
	%if not %isMissing(&hierarchy.) %then %do;

		%let hierarchy = %sysfunc(dequote(&hierarchy.));

		/* Only for the base term and if a hierarchy was selected */
		if isReportable(&base_col., "&hierarchy.") then do;
			%DEAV_INT_ADD_FX2_ERROR("FOODEX2.08", "&foodex_col.", &errorTable., &base_col.);
		end;

		/* Check reportability of explicit facets in their hierarchies */
		%iterateFacets(&facets_col., f, h, c)
			if isReportable(c, getHierarchyByAttributeCode("MTX", h)) then do;
				%DEAV_INT_ADD_FX2_ERROR("FOODEX2.08", "&foodex_col.", &errorTable., f);
			end;
		%endIterateFacets

		/* check forbidden processes for raw commodities */
		if (isRPC(&termTypeCol.)) then do;

			/* get the processes */
			PROCS = getFacetsByCategory(MERGED_FACETS, "F28");

			/* if at least one process was reported */
			if (^missing(PROCS)) then do;

				FORBIDDEN_GROUP = areProcessesForbidden(&base_col., TERM_LEVEL, "&hierarchy.", 
						PROCS, &foodex19ConfigTable.);

				/* if a forbidden group is found */
				if (^missing(FORBIDDEN_GROUP)) then do;
					%DEAV_INT_ADD_FX2_ERROR("FOODEX2.19", "&foodex_col.", &errorTable., FORBIDDEN_GROUP);
				end;

				drop FORBIDDEN_GROUP;
			end;

			drop PROCS;
		end;
	%end;

	/* drop temporary variables */
	drop SOURCE_COMM_COUNT SOURCE_COMM_COUNT_MERGED MERGED_FACETS SOURCE_COMM_NOT_IMPL_CHILD;

%mend;


/* Check non specific and generic terms */
%macro DEAV_INT_SINGLE_TERM_WARNINGS(foodex_col, termCode /* code to check */, 
		termCodeToPrint /* code which will be printed in the error message */) / store source des="Perform general checks for warnings";

	/* Non specific term warning */
	if isNonSpecific(&termCode.) then do;
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.10", "&foodex_col.", &errorTable., &termCodeToPrint.);
	end;

	/* Generic term warning */
	if isGeneric(&termCode.) then do;
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.11", "&foodex_col.", &errorTable., &termCodeToPrint.);
	end;

	/* Ambiguous term warning */
	if isAmbiguous(&termCode.) then do;
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.18", "&foodex_col.", &errorTable., &termCodeToPrint.);
	end;
%mend;

/* Compute all the warnings */
%macro DEAV_INT_FX2V_EVAL_WARNINGS(foodex_col /* Id of the current row (to know which row created the errors) */,
			base_col /* name of the column which contains the base term id */, 
			facets_col /* name of the column which contains all the explicit facets $ separated */, 
			implicit_facets_col /* name of the column which contains all the implicit facets $ separated */, 
			errorTable) / store source des="Check FOODEX2_VALIDATION warnings";

	%local termTypeCol;
	%local detailLevelCol;
	%let termTypeCol = TERMTYPE;
	%let detailLevelCol = DETAILLEVEL;

	MERGED_FACETS = addImplicitFacets(&facets_col., &implicit_facets_col.);

	if isHierarchyTerm(&detailLevelCol.) then do;
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.09", "&foodex_col.", &errorTable., &base_col.);
	end;

	SOURCE_COMM_COUNT = countFacets(MERGED_FACETS, "F27");
	INGRED_COUNT = countFacets(MERGED_FACETS, "F04");
	SOURCE_COUNT = countFacets(MERGED_FACETS, "F01");

	/* Minor ingredient warning */
	if ((isDerivative(&termTypeCol.) or isRPC(&termTypeCol.)) and INGRED_COUNT > 0) then do;
		F04_FACETS = getFacetsByCategory(MERGED_FACETS, "F04");
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.12", "&foodex_col.", &errorTable., F04_FACETS);
		drop F04_FACETS;
	end;

	/* F01 for derivative base term warning */
	if (isDerivative(&termTypeCol.) and SOURCE_COMM_COUNT = 1 and SOURCE_COUNT > 0) then do;
		F01_FACETS = getFacetsByCategory(MERGED_FACETS, "F01");
		%DEAV_INT_ADD_FX2_WARNING("FOODEX2.13", "&foodex_col.", &errorTable., F01_FACETS);
		drop F01_FACETS;
	end;

	/* check for both base term and facets */
	%DEAV_INT_SINGLE_TERM_WARNINGS(&foodex_col., &base_col., &base_col.);

	/* Check for explicit facets */
	%iterateFacets(&facets_col., f, h, c)
		%DEAV_INT_SINGLE_TERM_WARNINGS(&foodex_col., c, f);
	%endIterateFacets

	drop SOURCE_COUNT INGRED_COUNT SOURCE_COMM_COUNT MERGED_FACETS;
%mend;

/* Enrich dataset with MTX catalogue information */
%macro DEAV_INT_ADD_MTX_INFO(input /* Input dataset with a column named BASE (id of base terms) */, 
	base_col /* column which contains the base term code */,
	output /* Where the enriched dataset is created */, 
	foodex2Hierarchy /* OPTIONAL: Hierarchy of the domain */) / store source des="Add MTX information to input dataset";

	/* 
	 * Enrich the dataset with detail level, term type, all facets (MTX attributes) 
	 * and reportability in the domain hierarchy.
	 */
	proc sql;

		create table &output. as
		select t1.*, mtx.*
		from (
			select ID, 
				CODE, 

				/* Add hierarchy information if possible */
				%if not %isMissing(&foodex2Hierarchy.) %then %do;

					/* If hierarchy is MTX, we need to use MASTER as keyword */
					%if &foodex2Hierarchy. = MTX %then %let foodex2Hierarchy = MASTER;

					&foodex2Hierarchy.REPORTABLE as REPORTABLE, 
					&foodex2Hierarchy._N_LEVELS as N_LEVELS, 
					&foodex2Hierarchy._TERM_LEVEL as TERM_LEVEL, 
				%end;

				ALLFACETS, 
				TERMTYPE, 
				DETAILLEVEL

			from BRS_STG.FOODEX2_HIERARCHIES_DATA) mtx

		inner join &input. t1 on mtx.CODE = t1.&base_col.;
	run;
%mend;

/*
 * This macro evaluates several checks to assess the correctness of
 * a list of FoodEx2 codes.
 */
%macro DEAV_FOODEX2_VALIDATION(inputTable= /* Input table */, 
		outputTable= /* Table where errors and warnings will be put */,
		foodex2Column= /* Name of the column which contains the FoodEx2 codes to validate */,
		idColumns= /* List of space separated columns which identify a row of the inputTable */,
		foodex2Hierarchy= /* Optional. Add checks related to a specific hierarchy */,
		statistics=0 /* 1=compute performance statistics for the algorithm, 0=no statistics is computed*/) 
		/ store source des="Validate a list of FoodEx2 codes and returns errors in output";

	/* Add row identifier */
	data INPUT_WITH_IDS;
		set &inputTable.;
		keep &idColumns. &foodex2Column.;
	run;

	/* Take distinct set of foodex2 code to save validation time */
	proc sql noprint;
		create table INPUT_DISTINCT as
		select distinct &foodex2Column.
		from INPUT_WITH_IDS;
	run;

	/* Split foodex2 code into base term and facets */
	data INPUT_DISTINCT;
		set INPUT_DISTINCT;
		if (^missing(&foodex2Column.)) then do;
			BASE = getBaseTermFromCode(&foodex2Column.);
			EXPLICIT_FACETS = getFacetsFromCode(&foodex2Column.);
		end;
	run;

	/* add mtx information */
	%DEAV_INT_ADD_MTX_INFO(INPUT_DISTINCT, BASE, INPUT_ENRICHED, &foodex2Hierarchy.);

	data _null_;
		nobsInput = getNobs("INPUT_DISTINCT");
		nobsAfterJoin = getNobs("INPUT_ENRICHED");

		if (nobsInput ^= nobsAfterJoin) then do;
			putlog 'ERROR: Number of records after join with MTX changed. Input=' nobsInput 'After join=' nobsAfterJoin 'Please check code existance';
		end;
	run;

	%local errorTable;
	%let errorTable = DEAV_FOODEX2_VALIDATION_ERR;

	/* Delete the error table if already exists */
	%deleteDataset(&errorTable.);

	%local allFacetsCol;
	%let allFacetsCol = ALLFACETS;

	%DEAV_CREATE_EMPTY_ERR_TABLE(&errorTable.);

	/* Start validation */
	data FOODEX_VALIDATION_INPUT;
		set INPUT_ENRICHED nobs=nobs;

		%if &statistics. %then %do;
			VALIDATION_TIME = time();
		%end;

		%DEAV_INT_CLEAN_FACETS(&foodex2Column., EXPLICIT_FACETS, ALLFACETS, "&errorTable.", CLEANED_EXPL_FACETS);
		%DEAV_INT_CHECK_ALL_FACETS_CARD("&foodex2Column.", CLEANED_EXPL_FACETS, "&errorTable.", PROCESSABLE);

		/* If facet cardinality is not correct, do not validate the code since it would produce wrong errors */
		if PROCESSABLE then do;

			putlog "(" _N_"/" nobs") FoodEx2 validation: processing code " &foodex2Column.;

			%DEAV_INT_FX2V_EVAL_ERRORS(&foodex2Column., BASE, CLEANED_EXPL_FACETS, &allFacetsCol., 
					"&errorTable.", "BRS_STG.FOODEX2_19_VALIDATION_CONFIG", "&foodex2Hierarchy.");

			%DEAV_INT_FX2V_EVAL_WARNINGS(&foodex2Column., BASE, CLEANED_EXPL_FACETS, &allFacetsCol., "&errorTable.");
		end;

		%if &statistics. %then %do;
			VALIDATION_TIME = time() - VALIDATION_TIME;
		%end;
	run;

	/* Merge the results in the output table */
	proc sql noprint;
		create table &outputTable. as
		select input.*, e.ERR_CODE, e.ERR_TYPE, e.ERR_MESSAGE, e.ERR_COLUMN, e.ERR_VALUE
		from &errorTable. e
		inner join INPUT_WITH_IDS input
		on input.&foodex2Column. = e.ERR_VALUE;
	run;

	data &outputTable.;
		set &outputTable.;
		drop &foodex2Column.;
	run;

	%if &statistics. %then %do;
		/* Diagnostic */
		proc sql;
			title 'FoodEx2 validation performances';
			select count(*) as 'Evaluated codes (distinct)'n, 
				sum(case WHEN PROCESSABLE = 1 THEN 1 ELSE 0 END) as 'Processed codes (valid facets)'n,
				sum(VALIDATION_TIME) as 'Estimated time (seconds)'n, 
				avg(VALIDATION_TIME) as 'Average time per code (seconds)'n
			from FOODEX_VALIDATION_INPUT;
		run;
	%end;

	proc sql noprint;
		/*drop table FOODEX_VALIDATION_INPUT;*/
		drop table &errorTable.;
		drop table INPUT_ENRICHED;
		drop table INPUT_DISTINCT;
		drop table INPUT_WITH_IDS;
	run;
%mend;