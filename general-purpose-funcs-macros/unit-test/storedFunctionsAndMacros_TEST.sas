
options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.dcf MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables);
data _null_;
	rm = removeRedundantExplicitFacets("F01.A058Y$F01.A16MN", "F01.A13VM");
	%assertEqualsValue("", rm);
run;

data _null_;
	merged = addImplicitFacets("F01.A13VM", "F01.A058Y$F01.A16MN");
	%assertEqualsValue("F01.A13VM", merged);
run;

data _null_;
	distinct = getDistinctFacets("F01.A058Y$F01.A16MN$F01.A058Y");
	%assertEqualsValue("F01.A058Y$F01.A16MN", distinct);

	distinct = getDistinctFacets("F01.A001A$F02.B2718A$F02.B2718A");
	%assertEqualsValue("F01.A001A$F02.B2718A", distinct);
run;

data _null_;
	consistent = checkFacetCardinality("F01.A058Y$F01.A16MN$F01.A058Y", "F01");
	%assertEqualsValue(0, consistent);
run;

data _null_;
	filtered = getFacetsByCategory("F01.A058Y$F27.A16MN$F01.A058Y", "F01");
	%assertEqualsValue("F01.A058Y$F01.A058Y", filtered);
	
	filtered = getFacetsByCategory("F01.A058Y$F27.A16MN$F01.A058Y", "F27");
	%assertEqualsValue("F27.A16MN", filtered);
run;

data _null_;
	count = countFacetsNotChildOfImplicit("F27.A026X", "F27.A026V", "F27");  /* explicit child of implicit */
	%assertEqualsValue(0, count);

	count = countFacetsNotChildOfImplicit("F27.A026X$F27.A00KR", "F27.A026V", "F27");  /* one explicit child of implicit */
	%assertEqualsValue(1, count);

	count = countFacetsNotChildOfImplicit("F27.A026X$F27.A00KR$F01.A0BXL$F01.A0BXL", "F27.A026V", "F27");  /* one explicit child of implicit */
	%assertEqualsValue(1, count);

	count = countFacetsNotChildOfImplicit("F27.A00PB$F27.A026X$F27.A00KR$F01.A0BXL$F01.A0BXL", "F27.A026V", "F27");  /* one explicit child of implicit */
	%assertEqualsValue(2, count);
run;

data _null_;
	v = getExplicitNotChildOfImplicit("F27.A000L$F27.A00HS$F01.HSIHS", "F27.A0DMX$F03.FSSJD", "F27");
	%assertEqualsValue("F27.A000L", v);

	v = getExplicitNotChildOfImplicit("F27.A00HS$F01.HSIHS", "F27.A0DMX$F03.FSSJD", "F27");
	%assertEqualsValue("", v);

	v = getExplicitNotChildOfImplicit("F27.A00HS$F01.HSIHS", "F03.FSSJD", "F27");
	%assertEqualsValue("", v);

	v = getExplicitNotChildOfImplicit("F27.A00HS$F01.HSIHS", "F27.AHAHA$F03.FSSJD", "F27");
	%assertEqualsValue("F27.A00HS", v);
run;

data _null_;
	hierarchy = getHierarchyFromColumn("ADD_USAGE.2013PROD", "ADD_USAGE", "AddProdCode");
	%assertEqualsValue("ADDFOOD", hierarchy);
run;


/* Test is ancestor function */
data _null_;
	ancestor = isAncestor("A0DJJ", "A0EZM", "REPORT");
	notAncestor = isAncestor("A0DJJ", "A0BYQ", "REPORT");

	%assertEqualsValue(1, ancestor);
	%assertEqualsValue(0, notAncestor);
run;

data _null_;
	getUID = getUniqueIdentifierColumn("TSE.2018", "SSD2_CENTRAL_2017");
	%assertEqualsValue("resId", getUID);
run;

data _null_;
	%stringTokenizer("F01.BBB$F02.CCC", "$", token);
		putlog token;
	%endStringTokenizer;
run;

data _null_;
	%iterateFacets("F01.BBB$F02.CCC", f, h, c);
		putlog f h c;
	%endIterateFacets;
run;

data GET_NOBS_TABLE;
	A = 1;
	output;
	A = 2;
	output;
run;

data _null_;
	nobs = getNobs("GET_NOBS_TABLE");
	%assertEqualsValue(2, nobs);
run;

proc sql;
	drop table GET_NOBS_TABLE;
run;