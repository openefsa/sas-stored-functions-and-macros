%let debug = 0;  /* 1 = enable dubug messages, 0 = disable (it is needed to recompile all the functions if changed) */

options mstored sasmstore=MSTORE;
options fmtsearch=(FMTLIB BRS_STG); /* Required to use BR formats for parents */
options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.dcf MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables);

/* Functions which are related to random things */
proc fcmp outlib = MSTORE.strings.rand;

/* Generate a random arbitrary long string */
function genRandomString(length) $;

	length string $ 4000;

    do j = 1 to length;
    	substr(string, j) = byte(int(65+26 * ranuni(0)));
    end;
	
	return(compress(string));
endsub;
run;

proc fcmp outlib = MSTORE.strings.lists;
	
/* Remove a set of elements from a list separated by a specific separator */
function removeElementsFromList(source $ /* Input list */, 
	elementsToRemove $ /* List of elements which will be removed from the source */,
	separator $ /* separator between the elements of the lists */) $ 4000;

	if (missing(elementsToRemove)) then return(source);

	length currentElem $2000;
	length outputList $2000;

	/* 
	   Initialize cleaned explicit facets with the input value
	   Add also a $ at the beginning in order to make the algorithm easier
	   since it will search implicit facets with a $ as starting character
	   (in order to remove directly also the $ related to the implicit facet)
	 */
	outputList = separator || source;

	/* Remove implicit facets from the explicit facets */
	i = 1;
	do until (scan(elementsToRemove, i, separator) = "");

		currentElem = separator || scan(elementsToRemove, i, separator);

		/* Remove implicit facet (with the related dollar) */
		outputList = tranwrd(outputList, trim(currentElem), "");

		i = i + 1;
	end;

	/* Remove added $ at the beginning if still present */
	outputList = compress(outputList);
	if (substr(outputList, 1, 1) = separator) then do;
		outputList = substr(outputList, 2, length(outputList));
	end;

	if (&debug.) then put 'removeElementsFromList: remove' elementsToRemove 'from' source '=>' outputList;

	return(outputList);
endsub;
run;

%macro getHierarchyByAttributeCode / store source des="Used in the getHierarchyByAttributeCode FCMP function. Do not use directly!";

	%let attrCode = %sysfunc(dequote(&attrCode.));
	%let catalogueCode = %sysfunc(dequote(&catalogueCode.));

	/* Get the hierarchy code starting from the attribute code */
	proc sql noprint;
		select hier.CODE into :hierarchyCode
		from CATALOG.CATALOGUE_ATTRIBUTE_DATA attr_data
			inner join CATALOG.CATALOGUE_ATTRIBUTE attr on attr_data.CATALOGUE_ATTRIBUTE_ID = attr.ID
			inner join CATALOG.HIERARCHY hier on hier.ID = attr_data.HIERARCHY_ID
			inner join CATALOG.CATALOGUE cat on cat.ID = attr.CATALOGUE_ID
		where attr.CODE = "&attrCode." and cat.CODE = "&catalogueCode."
		group by attr.CODE 
		having max(attr_data.ID) = attr_data.ID;
	quit;
%mend;

%macro isSingleOrRepeatable / store source des="Used in the isAttributeRepeatable FCMP function. Do not use directly!";

	%let attrCode = %sysfunc(dequote(&attrCode.));
	%let catalogueCode = %sysfunc(dequote(&catalogueCode.));

	proc sql noprint;
		select attr_data.SINGLE_REPEATABLE_TYPE into :singleRepeatableType
		from CATALOG.CATALOGUE_ATTRIBUTE_DATA attr_data
			inner join CATALOG.CATALOGUE_ATTRIBUTE attr on attr_data.CATALOGUE_ATTRIBUTE_ID = attr.ID
			inner join CATALOG.CATALOGUE cat on cat.ID = attr.CATALOGUE_ID
		where attr.CODE = "&attrCode." and cat.CODE = "&catalogueCode."
		group by attr.CODE 
		having max(attr_data.ID) = attr_data.ID;
	quit;
%mend;

proc fcmp outlib = MSTORE.catalogues.attributes;

/* Check if an attribute is repeatable or not
 */
function isAttributeRepeatable(catalogueCode $, attrCode $ /* Code of the attribute */);

	length singleRepeatableType $ 200;

	rc = run_macro('isSingleOrRepeatable', catalogueCode, attrCode, singleRepeatableType);

	if rc ^= 0 then do;
		put 'ERROR: isSingleOrRepeatable macro failed';
		return("");
	end;

	return(singleRepeatableType = "R");
endsub;
run;

/* Functions which handle catalogues contents */
proc fcmp outlib = MSTORE.catalogues.hierarchies;

/* Get the code of an hierarchy using the code of the attribute
 * related to a hierarchy (as facets, using "F01" will return "source")
 */
function getHierarchyByAttributeCode(catalogueCode $, attrCode $) $ 400;

	length hierarchyCode $ 400;

	rc = run_macro('getHierarchyByAttributeCode', catalogueCode, attrCode, hierarchyCode);

	if rc ^= 0 then do;
		put 'ERROR: getHierarchyByAttributeCode macro failed';
		return("");
	end;

	return(hierarchyCode);
endsub;

/* Get all the ancestors of a term (- separated) */
function getAncestors(term $ /* baseline term code to get the ancestors */, 
		hierarchy $ /* hierarchy where to search for ancestors */) $ 4000;

	length ancestorFormat $ 100;

	ancestorFormat = compress(cat("$", upcase(hierarchy), "_ccatLEVEL."));

	if (&debug.) then put "ancestors format for hierarchy" hierarchy "is" ancestorFormat;

	/* 'put' runs at compile time, but here we have a variable for the format, so we need 'putc' to evaluate at runtime */
	return(putc(term, ancestorFormat));
endsub;

/* Check if a term is ancestor of another */
function isAncestor(term $ /* child term code */, 
	target $ /* code of the candidate ancestor term */, 
	hierarchy $ /* hierarchy where to search for ancestors */);

	length ancestors $ 1000;

	ancestors = getAncestors(term, hierarchy);

	found = indexw(ancestors, target) > 0; /* indexw is the only function which works nested in other functions */

	if (&debug.) then put term "is child of" target "?" found "where ancestors are" ancestors "in the hierarchy" hierarchy;

	return(found);
endsub;

/* Get the parent of a term in a specific hierarchy. It returns empty if no parent is found */
function getAncestorAtLevel(term $, hierarchy $, level $) $4000;

	length f $ 100;

	f = compress(cat("$", upcase(hierarchy), "_LEVEL", level, "_."));

	if (&debug.) then put "Parent format for hierarchy" hierarchy "is" f;

	return(putc(term, f));
endsub;
run;

/* Iterate a list of elements */
%macro stringTokenizer(string /* string to parse */, 
	separator /* string separator */, token /* output */) / store source 
		des="Split a string separated by the 'separator' variable. 
			The macro will put at each iteration the current token in the 'token' variable. 
			It needs to be closed by %endStringTokenizer";

	/* Create a random name for the iterator variable (avoid conflicts in nested loops) */
	%local i;
	%let i = %sysfunc(genRandomString(10));

	&i. = 1;
	do until (scan(&string., &i., &separator.) = "");
		&token. = scan(&string., &i., &separator.);
		&i. = &i. + 1;
%mend;

%macro endStringTokenizer / store source des="Close the %stringTokenizer statement";
	end;
%mend;

/* Iterate a list of facets */
%macro iterateFacets(facets /* variable which contains the list to iterate */, 
	facet /* variable which will contain the current facet (header.code)*/, 
	facetHeader /* Variable which will contain the current facet header */, 
	facetCode /* Variable which will contain the current facet code */) / store source 
		des="Iterate list of facet separated by '$'. At each iteration, 'facet' will contain the header and code separated by the dot
			'facetHeader' will contain the header of the facet and 'facetCode' only the facet code. It needs to be closed by %endIterateFacets";
	
	%stringTokenizer(&facets., "$", token)
		&facet. = token;
		&facetHeader. = getfacetHeader(&facet.);
		&facetCode. = getFacetCode(&facet.);
%mend;

/* Close the iterateFacets statement */
%macro endIterateFacets / store source des="Close the %iterateFacets statement";
	%endStringTokenizer;
%mend;

%macro getFacetsHeaders / store source des="Used in the getFacetsHeaders FCMP function. Do not use directly!";
	proc sql noprint;
		select distinct attr.CODE as CODE into :facetsCodes separated by ' '
		from CATALOG.CATALOGUE_ATTRIBUTE_DATA attr_data
			inner join CATALOG.CATALOGUE_ATTRIBUTE attr on attr_data.CATALOGUE_ATTRIBUTE_ID = attr.ID
			inner join CATALOG.HIERARCHY hier on attr_data.HIERARCHY_ID = hier.ID
			inner join CATALOG.CATALOGUE cat on hier.CATALOGUE_ID = cat.ID
		where cat.ID = 75
		order by CODE asc;
	quit;
%mend;

proc fcmp outlib = MSTORE.mtx.attributes;

/* Check if a term is a hierarchy */
function isHierarchyTerm(detailLevel $);
	return(detailLevel = "H");
endsub;

/* Check if a term is a derivative */
function isDerivative(termType $);
	return(termType = "d");
endsub;

/* Check if a term is a composite */
function isComposite(termType $);
	return(termType = "c" or termType = "s");
endsub;

/* Check if a term is a raw primary commodity */
function isRPC(termType $);
	return(termType = "r");
endsub;

/* Check if a term is a facet */
function isFacet(termType $);
	return(termType = "f");
endsub;

/* Check if current term in the data step is a non specific term */
function isNonSpecific(termCode $);
	detailLevel = put(termCode, $DETAILLEVEL_MTX.);
	return(detailLevel = "P");
endsub;
run;

/*
 * Functions which handle MTX terms properties
 */
proc fcmp outlib = MSTORE.mtx.terms;
function isDeprecated(termCode $);
	deprecated = input(termCode, DEPRECATED_MTX.);
	return(deprecated);
endsub;
function isReportable(termCode $, hierarchyCode $);

	length repFormat $100;

	repFormat = "REP_MTX";

	/* If not master, add hierarchy code */
	if (upcase(hierarchyCode)^="MTX") then 
		repFormat = compress(cat(repFormat, "_", upcase(hierarchyCode)));

	/* add the format 'dot' */
	repFormat = compress(cat(repFormat, "."));

	reportability = inputn(termCode, repFormat);

	return(reportability);
endsub;
run;

/*
 * Functions which handle facets
 */
proc fcmp outlib = MSTORE.mtx.facets;

/* Extract the base term from a foodex 2 code */
function getBaseTermFromCode(foodexCode $ /* foodex code to parse */) $ 1000;
	return(scan(foodexCode, 1, "#"));
endsub;

/* Extract the facets ($ separated) from a foodex 2 code */
function getFacetsFromCode(foodexCode $ /* foodex code to parse */) $ 1000;
	return(scan(foodexCode, 2, "#"));
endsub;

function getFacetHeader(facet $ /* facetHeader.facetCode to parse */) $ 1000;
	return(scan(facet, 1, "."));
endsub;

function getFacetCode(facet $ /* facetHeader.facetCode to parse */) $ 1000;
	return(scan(facet, 2, "."));
endsub;

/* Get all the headers of the MTX facets separated by spaces */
function getFacetsHeaders() $ 4000;
	
	length facetsCodes $ 4000;

	rc = run_macro('getFacetsHeaders', facetsCodes);

	return(facetsCodes);
endsub;

/* Count the number of descriptors related to a single facet category inside a foodex2 code */
function countFacets(facets $ /* list of facets */, facetHeader $ /* header of the facets (F01 or F02 or...) we want to count */);
	
	/* handle exceptional cases */
	if (missing(facets) or missing(facetHeader)) then do;
		if (&debug.) then put "WARNING: countFacets: either facets or facetHeader argument is empty. Returning 0";
		return(0);
	end;

	count = 0;
	%iterateFacets(facets, f, currHeader, c)
		if facetHeader = currHeader then count = count + 1;
	%endIterateFacets

	if (&debug.) then put "countFacets: number of facets with category" facetHeader "are" count "in the FoodEx2 code" facets;

	return(count);
endsub;

/* Count the number of descriptors related to a single facet category inside a foodex2 code */
function countFacetsNotChildOfImplicit(facets $ /* list of facets */, implicitFacets $ /* implicit facets */,
		facetHeader $ /* header of the facets (F01 or F02 or...) we want to count */);

	length hierarchy $500;

	hierarchy = getHierarchyByAttributeCode("MTX", facetHeader);

	count = 0;
	%iterateFacets(facets, expl, explH, explC)
		if explH = facetHeader then do;
			isChild = 0;

			%iterateFacets(implicitFacets, impl, implH, implC)
				if explH = implH and isAncestor(explC, implC, hierarchy) then do;
					isChild = 1;
				end;
			%endIterateFacets

			if (^isChild) then count = count + 1;
		end;
	%endIterateFacets

	return(count);
endsub;

/* Check if the cardinality of the facets identified by the header is respected */
function checkFacetCardinality(facets $ /* list of explicit facets */,
	facetHeader $ /* header of the facet we want to check (F01, F02, ...)*/);

	correct = 1;

	repeatable = isAttributeRepeatable("MTX", facetHeader);

	/* if single cardinality but with more than one explicit facet set */
	if ^repeatable then do;
		count = countFacets(facets, facetHeader);
		if count > 1 then do;
			correct = 0;
		end;
	end;

	return(correct);
endsub;

function getFacetsByCategory(facets $, facetHeader $) $ 4000;

	length out $ 4000;

	%iterateFacets(facets, f, h, c)
		if facetHeader = h then do;
			if(missing(compress(out))) then out = f;
			else out = catx("$", compress(out), f);
		end;
	%endIterateFacets

	if (&debug.) then put 'getFacetsByCategory: for category' facetHeader 'retrieved' facets;

	return(out);
endsub;

/* Concerning only not repeatable facets, remove the explicit facets which 
 * are parent of the implicit facets, since they are redundant */
function removeRedundantExplicitFacets(explicits $, implicits $) $ 4000;

	length mergedFacets $ 4000;
	length hierarchy $400;

	/* Create a copy */
	mergedFacets = explicits;

	%iterateFacets(implicits, impl, implHeader, implCode)
		%iterateFacets(explicits, expl, explHeader, explCode)

			/* Do not compare the same facet and facets related to different categories 
			 * do not remove parents in the explicits if repeatable facet (see documentation) */
			if (implHeader = explHeader and implCode ^= explCode and ^isAttributeRepeatable("MTX", explHeader)) then do;

				/* get the facet hierarchy */
				hierarchy = getHierarchyByAttributeCode("MTX", explHeader);

				if (isAncestor(implCode, explCode, hierarchy)) then do;	/* remove explicit (implicit is more detailed) */
					mergedFacets = removeElementsFromList(mergedFacets, expl, "$");
				end;
			end;
		%endIterateFacets
	%endIterateFacets

	if (&debug.) then put 'removeRedundantExplicitFacets: ' explicits '=>' outputList;

	return(mergedFacets);
endsub;

/* Get a list of explicit facets which are not children of the implicit facets for a specific facet header */
function getExplicitNotChildOfImplicit(explicits $, implicits $, facetHeader $) $ 4000;

	length out $ 4000;
	length hierarchy $400;

	count = countFacets(implicits, facetHeader);

	if (count = 0) then return(out); /* If no implicit with the selected header return */

	%iterateFacets(explicits, expl, explHeader, explCode)
	
		if (explHeader = facetHeader) then do;

			isChild = 0;
			
			%iterateFacets(implicits, impl, implHeader, implCode)

				if (implHeader = explHeader and implCode ^= explCode) then do;

					hierarchy = getHierarchyByAttributeCode("MTX", explHeader);

					if (isAncestor(explCode, implCode, hierarchy)) then do;
						isChild = 1;
					end;
				end;
			%endIterateFacets
			
			if (^isChild) then do;
				if (missing(out)) then out = expl;
				else out = catx("$", compress(out), expl);
			end;
		end;

	%endIterateFacets

	return(out);
endsub;

/* Get distinct facets */
function getDistinctFacets(facets $) $ 4000;

	if (missing(facets)) then return("");

	length distinct $ 1000;
	
	/* add facet only if not already present */
	%iterateFacets(facets, facet, h, c)
		if(missing(compress(distinct))) then distinct = facet; /* first value */
		else do;

			found = indexw(distinct, facet);

			if(^found) then do;
				distinct = catx(" ", distinct, facet); /* only spaces work with indexw */
			end;
		end;
	%endIterateFacets

	/* Concert spaces into $ */
	distinct = translate(trim(distinct), "$", " ");

	return(distinct);
endsub;

/* Merge the explicit facets with the implicit facets. In particular, if an explicit facet
 * is child of an implicit, the implicit facet will not be included */
function addImplicitFacets(explicits $, implicits $) $ 4000;

	length mergedFacets $4000;
	length hierarchy $400;

	/* Create an initial facet list with everything */
	mergedFacets = catx("$", explicits, implicits);

	%iterateFacets(implicits, impl, implHeader, implCode)
		%iterateFacets(explicits, expl, explHeader, explCode)

			/* Do not compare the same facet and facets related to different categories */
			if (implHeader = explHeader and implCode ^= explCode) then do;

				/* get the facet hierarchy */
				hierarchy = getHierarchyByAttributeCode("MTX", explHeader);

				if (isAncestor(explCode, implCode, hierarchy)) then do;	/* if explicit child of implicit remove the implicit */
					mergedFacets = removeElementsFromList(mergedFacets, impl, "$");
				end;
			end;
		%endIterateFacets
	%endIterateFacets

	return(getDistinctFacets(mergedFacets));
endsub;
run;

%macro getRecordUniqueIdColumn / store source des="Used in the getUniqueIdentifierColumn FCMP function. Do not use directly!";

	%let dataCollectionCode = %sysfunc(dequote(&dataCollectionCode.));
	%let deav_tableName = %sysfunc(dequote(&deav_tableName.));

	proc sql noprint;
		select distinct dm.COLUMNNAME
		into :uniqueIdentifier
		from dcf.DATA d
			inner join dcf.DATAMETADATA dm on dm.DATAID = d.ID
		    inner join dcf.FACT f on d.ID = f.ID
		    left join dcf.DATAPACK dp on dp.DATAID = d.ID
		    left join dcf.DCPACKAGE dcp on dcp.ID = dp.PKGID
		    left join dcf.DATACOLLECTION dc on dc.ID = dcp.DCID
		    left join catalog.CATALOGUE cat on dm.CATALOG_ID = cat.ID
			left join dcf.PACK_MET_CV pmc on pmc.datametadata_id = dm.ID and pmc.DCPACKAGE_ID = dcp.ID
	        left join catalog.HIERARCHY h1 on h1.ID = dm.HIERARCHY_ID
	        left join catalog.CATALOGUE cat1 on h1.CATALOGUE_ID = cat1.ID
	        left join catalog.HIERARCHY h2 on h2.ID = pmc.H_ID
	        left join catalog.CATALOGUE cat2 on h2.CATALOGUE_ID = cat2.ID
		where dc.COLLECTIONTYPE = 'F' and dc.CODE = "&dataCollectionCode." and d.deav_tableName = "&deav_tableName." and ISUNIQUEIDENTIFIER = "1";
	quit;
%mend;

%macro getHierarchyFromColumn / store source des="Used in the getHierarchyFromColumn FCMP function. Do not use directly!";

	%let dataCollectionCode = %sysfunc(dequote(&dataCollectionCode.));
	%let deav_tableName = %sysfunc(dequote(&deav_tableName.));
	%let columnName = %sysfunc(dequote(&columnName.));

	proc sql noprint;
		select distinct
			case when dm.COLUMNTYPE = "COMPOUND" then "" else coalesce(h1.CODE, h2.CODE, cat.CODE, cat1.CODE, cat2.CODE) end as HIERARCHYCODE
		into :hierarchyCode
		from dcf.DATA d
			inner join dcf.DATAMETADATA dm on dm.DATAID = d.ID
		    inner join dcf.FACT f on d.ID = f.ID
		    left join dcf.DATAPACK dp on dp.DATAID = d.ID
		    left join dcf.DCPACKAGE dcp on dcp.ID = dp.PKGID
		    left join dcf.DATACOLLECTION dc on dc.ID = dcp.DCID
		    left join catalog.CATALOGUE cat on dm.CATALOG_ID = cat.ID
			left join dcf.PACK_MET_CV pmc on pmc.datametadata_id = dm.ID and pmc.DCPACKAGE_ID = dcp.ID
	        left join catalog.HIERARCHY h1 on h1.ID = dm.HIERARCHY_ID
	        left join catalog.CATALOGUE cat1 on h1.CATALOGUE_ID = cat1.ID
	        left join catalog.HIERARCHY h2 on h2.ID = pmc.H_ID
	        left join catalog.CATALOGUE cat2 on h2.CATALOGUE_ID = cat2.ID
		where dc.COLLECTIONTYPE = 'F' and dc.CODE = "&dataCollectionCode." and d.TABLENAME = "&deav_tableName." and dm.COLUMNNAME = "&columnName.";
	quit;
%mend;

/* DCF functions */
proc fcmp outlib = MSTORE.dcf.dcf;

/* Get the hierarchy code related to a specific column inside a table inside a data collection */
function getHierarchyFromColumn(dataCollectionCode $, deav_tableName $, columnName $) $;

	length hierarchyCode $ 4000;

	rc = run_macro('getHierarchyFromColumn', dataCollectionCode, deav_tableName, columnName, hierarchyCode);
	
	return(compress(hierarchyCode));
endsub;

function getUniqueIdentifierColumn(dataCollectionCode $, deav_tableName $) $;

	length uniqueIdentifier $ 4000;

	rc = run_macro('getRecordUniqueIdColumn', dataCollectionCode, deav_tableName, uniqueIdentifier);
	
	return(compress(uniqueIdentifier));
endsub;
run;

%macro isMissing(value) / store source des="Check if a macro variable is missing or not.";
	%if &value. = %str() or &value. = "" or &value. = . %then 1; %else 0;
%mend;

/* Append the data to the table. If the table does not exist it will be created */
%macro appendDataset(data, out) / store source des="Append data to out table. If out does not exist it will be created";

	%if %sysfunc(exist(&out.)) %then %do;
		proc append base=&out. data=&data. force;
		run;
	%end;
	%else %do;
		data &out.;
			set &data.;
		run;
	%end;
%mend;

%macro deleteDataset(data) / store source des="Delete a dataset. If dataset does not exist no action is performed";
	%if %sysfunc(exist(&data.)) %then %do;
		proc sql;
			drop table &data.;
		run;
	%end;
%mend;

%macro getNobs / store source des="Used in the getNobs FCMP function. Do not use directly!";

	%let deav_tableName = %sysfunc(dequote(&deav_tableName.));

	proc sql noprint;
		select count(*) into :nobs
		from &deav_tableName.;
	run;
%mend;

proc fcmp outlib = MSTORE.tables.queries;

/* Get the number of rows of a table */
function getNobs(deav_tableName $);
	rc = run_macro('getNobs', deav_tableName, nobs);
	return(nobs);
endsub;
run;