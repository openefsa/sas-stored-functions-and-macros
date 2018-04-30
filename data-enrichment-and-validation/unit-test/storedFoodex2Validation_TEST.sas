

%let input_temp_table = TEST_INPUT;


/* Print a message into the log */
%macro print(message);
	data _null_;
		putlog &message.;
	run;
%mend;

/* E001 - Single F27 for RPC base term error */
%macro testSingleF27ForRPCFX01;

	%print('TEST: testSingleF27ForRPCFX01');

	data &input_temp_table.;
		BASE = "HSAHS";
		FACETS = "F01.HSJHS$F27.A0D9Y";
		ALLFACETS = "";
		&termTypeCol = "r";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, REPORT, "TESTE001", "");
	run;

	%assertNobsEquals(TESTE001, 1);
	%assertEquals(TESTE001, 1, BR_CODE, "FOODEX2.01");
	%deleteTable(TESTE001);
%mend;


/* E002 - F01 for mixed derivative error */
%macro testF01ForMixedDerivativeFX02;

	%print('TEST: testF01ForMixedDerivativeFX02');

	data &input_temp_table.;
		BASE = "HSAHS";
		FACETS = "F01.HSJHS$F27.A0D9Y$F27.AKL9Y";
		ALLFACETS = "";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTE002", "", REPORT);
	run;

	%assertNobsEquals(TESTE002, 1);
	%assertEquals(TESTE002, 1, BR_CODE, "FOODEX2.02");
	%deleteTable(TESTE002);
%mend;


/* E003 - F01 for composite base term error */
%macro testF01ForCompositeFX03;

	%print('TEST: testF01ForCompositeFX03');

	data &input_temp_table.;
		BASE = "HSAHS";
		FACETS = "F01.HSJHS";
		ALLFACETS = "";
		&termTypeCol = "c";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTE003", "", REPORT);
	run;

	%assertNobsEquals(TESTE003, 1);
	%assertEquals(TESTE003, 1, BR_CODE, "FOODEX2.03");
	%deleteTable(TESTE003);
%mend;

/* E004 - F27 for composite base term error */
%macro testF27ForCompositeFX04;

	%print('TEST: testF27ForCompositeFX04');

	data &input_temp_table.;
		BASE = "HSAHS";
		FACETS = "F27.A0D9Y$F27.AKL9Y";
		ALLFACETS = "";
		&termTypeCol = "c";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTE004", "", REPORT);
	run;

	/* two errors, one per source commodity */
	%assertNobsEquals(TESTE004, 2);
	%assertEquals(TESTE004, 1, BR_CODE, "FOODEX2.04");
	%assertEquals(TESTE004, 2, BR_CODE, "FOODEX2.04");
	%deleteTable(TESTE004);
%mend;

/* E006 - F01 for derivative without F27 error */
%macro testF01ForDerivWithNoF27FX06;

	%print('TEST: testF01ForDerivWithNoF27FX06');

	data &input_temp_table.;
		BASE = "HSAHS";
		FACETS = "F01.HSJHS";
		ALLFACETS = "";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTE006", "", REPORT);
	run;

	%assertNobsEquals(TESTE006, 1);
	%assertEquals(TESTE006, 1, BR_CODE, "FOODEX2.06");
	%deleteTable(TESTE006);
%mend;

/* E005 - F27 for derivative not child of implicit F27 error 
This catches also the case of a F27 parent of the implicit F27 */
%macro F27DerivNotChildOfImplF27FX05;

	%print('TEST: F27DerivNotChildOfImplicitF27E005');

	data &input_temp_table.;
		BASE = "A00ZF";
		FACETS = "F27.A00GX$F27.A000L$F27.A00HS";
		ALLFACETS = "F27.A0DMX";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTE005", "", REPORT);
	run;

	%assertNobsEquals(TESTE005, 1);
	%assertEquals(TESTE005, 1, BR_CODE, "FOODEX2.05");
	%assertEquals(TESTE005, 1, ERROR_IDS, "F27.A00GX$F27.A000L");
	%deleteTable(TESTE005);
%mend;

/* Check FOODEX.05 for F27 parent of the implicit F27 */
%macro testF27ParentOfImplicitFX05;

	%print('TEST: testF27ParentOfImplicitFX05');

	data &input_temp_table.;
		BASE = "A00ZF";
		FACETS = "F27.A00SF";
		ALLFACETS = "F27.A00SH";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTE005", "", REPORT);
	run;

	%assertNobsEquals(TESTE005, 1);
	%assertEquals(TESTE005, 1, BR_CODE, "FOODEX2.05");
	%deleteTable(TESTE005);
%mend;

/* E006 - F01 for derivative without F27 error */
%macro testF01ForDerivImplicitF27FX06;

	%print('TEST: testF01ForDerivImplicitF27FX06');

	data &input_temp_table.;
		BASE = "HSAHS";
		FACETS = "F01.HSJHS";
		ALLFACETS = "F27.HJHJH";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTE006", "", REPORT);
	run;

	%assertNotExist(TESTE006);
%mend;

/* FOODEX.17 - facet as a base term */
%macro testFacetForBaseTermFX17;

	%print('TEST: testFacetForBaseTermFX17');

	data &input_temp_table.;
		BASE = "HSAHS";
		FACETS = "F01.HSJHS";
		ALLFACETS = "F27.HJHJH";
		&termTypeCol = "f";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TEST17", "", REPORT);
	run;

	%assertNobsEquals(TEST17, 1);
	%assertEquals(TEST17, 1, BR_CODE, "FOODEX2.17");
	%deleteTable(TEST17);
%mend;

/* W001 - Not reportable base term warning */
%macro testNotReportableWarningFX08;

	%print('TEST: testNotReportableWarningFX08');

	data &input_temp_table.;
		BASE = "A07H2";
		FACETS = "";
		ALLFACETS = "";
		REPORTABLE = 0;
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW001_1");
	run;

	%assertNobsEquals(TESTW001_1, 1);
	%assertEquals(TESTW001_1, 1, BR_CODE, "FOODEX2.08");
	%deleteTable(TESTW001_1);
%mend;

/* W002 - H base term warning */
%macro testHierarchyBaseTermFX09;

	%print('TEST: testHierarchyBaseTermFX09');

	data &input_temp_table.;
		BASE = "AAAAA";
		&detailLevelCol = "H";
		FACETS = "";
		ALLFACETS = "";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW002");
	run;

	%assertNobsEquals(TESTW002, 1);
	%assertEquals(TESTW002, 1, BR_CODE, "FOODEX2.09");
	%deleteTable(TESTW002);
%mend;

/* W003 - Non specific term warning */
%macro testNonSpecificTermFX10;

	%print('TEST: testNonSpecificTermFX10');

	%createDetailLevelFormat;

	data &input_temp_table.;
		BASE = "A001Y"; /* non specific */
		FACETS = "F27.A0F1C";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW003");
	run;

	%assertNobsEquals(TESTW003, 2);
	%assertEquals(TESTW003, 1, BR_CODE, "FOODEX2.10");
	%assertEquals(TESTW003, 2, BR_CODE, "FOODEX2.10");
	%deleteTable(TESTW003);
%mend;

/* W004 - Generic facet warning */
%macro testGenericTermFX11;

	%print('TEST: testGenericTermFX11');

	data &input_temp_table.;
		BASE = "A0C0R";
		FACETS = "F27.A0CHR$F01.HHHHH";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW004");
	run;

	%assertNobsEquals(TESTW004, 2);
	%assertEquals(TESTW004, 1, BR_CODE, "FOODEX2.11");
	%assertEquals(TESTW004, 2, BR_CODE, "FOODEX2.11");
	%deleteTable(TESTW004);
%mend;

/* W005 - Minor ingredient rpc/d */
%macro testMinorIngredientForRPCFX12;
	%print('TEST: testMinorIngredientForRPCFX12');

		/* for RPC */
	data &input_temp_table.;
		BASE = "A000S";
		FACETS = "F04.A001X";
		&termTypeCol = "r";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW005_1");
	run;

	%assertNobsEquals(TESTW005_1, 1);
	%assertEquals(TESTW005_1, 1, BR_CODE, "FOODEX2.12");
	%deleteTable(TESTW005_1);
%mend;


%macro testMinorIngredientForDFX12;
	%print('TEST: testMinorIngredientForDFX12');

		/* for RPC */
	data &input_temp_table.;
		BASE = "A000S";
		FACETS = "F04.A001X";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW005_2");
	run;

	%assertNobsEquals(TESTW005_2, 1);
	%assertEquals(TESTW005_2, 1, BR_CODE, "FOODEX2.12");
	%deleteTable(TESTW005_2);
%mend;

%macro testMinorMultipleIngrForDFX12;
	%print('TEST: testMinorMultipleIngrForDFX12');

		/* for RPC */
	data &input_temp_table.;
		BASE = "A000S";
		FACETS = "F04.A001X$F04.A001N";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW005_3");
	run;

	%assertNobsEquals(TESTW005_3, 1);
	%assertEquals(TESTW005_3, 1, BR_CODE, "FOODEX2.12");
	%assertEquals(TESTW005_3, 1, ERROR_IDS, "F04.A001X$F04.A001N");
	%deleteTable(TESTW005_3);
%mend;

/* W006 - F01 for derivative base term with one F27 warning */
%macro testDerivF01AndSingleF27FX13;
	%print('TEST: testDerivF01AndSingleF27FX13');
	data &input_temp_table.;
		BASE = "A000S";
		FACETS = "F01.AHSIU$F27.SJOIS";
		&termTypeCol = "d";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTW006_1");
	run;

	%assertNobsEquals(TESTW006_1, 1);
	%assertEquals(TESTW006_1, 1, BR_CODE, "FOODEX2.13");
	%deleteTable(TESTW006_1);
%mend;

%macro testExplicitParentImplicitFX16;

	%print('TEST: testExplicitParentImplicitFX16');	

	data &input_temp_table.;
		resId = "test1";
		BASE = "A001X";
		FACETS = "F01.A058Y";
		ALLFACETS = "F01.A058Z";
	run;

	data _null_;
		set &input_temp_table.;
		CLEANED_EXPL_FACETS = cleanFacets("BASE", BASE, FACETS, ALLFACETS, "TESTFX16");
	run;

	%assertNobsEquals(TESTFX16, 1);
	%assertEquals(TESTFX16, 1, BR_CODE, "FOODEX2.16");
	%deleteTable(TESTFX16);
%mend;

%macro testCheckCardinalityFX07;

	%print('TEST: testCheckCardinalityFX07');	

	data &input_temp_table.;
		resId = "test1";
		BASE = "A001X";
		FACETS = "F01.A058Y$F01.A058K";
		ALLFACETS = "";
	run;

	data _null_;
		set &input_temp_table.;
		CLEANED_EXPL_FACETS = checkAllFacetsCardinality("BASE", BASE, FACETS, "TESTFX7");
	run;

	%assertNobsEquals(TESTFX7, 1);
	%assertEquals(TESTFX7, 1, BR_CODE, "FOODEX2.07");
	%deleteTable(TESTFX7);
%mend;

%macro testRepeatedExplicitFX14;

	%print('TEST: testRepeatedExplicitFX14');	

	data &input_temp_table.;
		resId = "test1";
		BASE = "A001X";
		FACETS = "F01.A058Y$F01.A058Y";
		ALLFACETS = "";
	run;

	data _null_;
		set &input_temp_table.;
		CLEANED_EXPL_FACETS = cleanFacets("BASE", BASE, FACETS, ALLFACETS, "TESTFX14");
	run;

	%assertNobsEquals(TESTFX14, 1);
	%assertEquals(TESTFX14, 1, BR_CODE, "FOODEX2.14");
	%deleteTable(TESTFX14);
%mend;

%macro testExplRepeatedInImplFX15;

	%print('TEST: testExplRepeatedInImplFX15');	

	data &input_temp_table.;
		resId = "test1";
		BASE = "A001X";
		FACETS = "F01.A058Y";
		ALLFACETS = "F01.A058Y";
	run;

	data _null_;
		set &input_temp_table.;
		CLEANED_EXPL_FACETS = cleanFacets("BASE", BASE, FACETS, ALLFACETS, "TESTFX15");
	run;

	%assertNobsEquals(TESTFX15, 1);
	%assertEquals(TESTFX15, 1, BR_CODE, "FOODEX2.15");
	%deleteTable(TESTFX15);
%mend;

%macro testAmbiguousTermFX18;

	%print('TEST: testAmbiguousTermFX18');	

	data &input_temp_table.;
		resId = "test1";
		BASE = "A00HQ";
		FACETS = "F01.A00HQ";
		ALLFACETS = "";
	run;

	data _null_;
		set &input_temp_table.;
		%evalWarnings(BASE, BASE, FACETS, ALLFACETS, "TESTFX18");
	run;

	%assertNobsEquals(TESTFX18, 2);
	%assertEquals(TESTFX18, 1, BR_CODE, "FOODEX2.18");
	%assertEquals(TESTFX18, 2, BR_CODE, "FOODEX2.18");
	%deleteTable(TESTFX18);
%mend;

%macro testForbiddenProcessFX19;

	%print('TEST: testForbiddenProcessFX19');

	/* Test get forbidden processes */
	data CONFIG;
		HIERARCHY = "REPORT";
		GROUP_PARENT_CODE = "A000J";
		FORBIDDEN_PROCS = "A07LG";
	run;

	data &input_temp_table.;
		resId = "test1";
		&termTypeCol. = "r";  /* it works only with RPC */
		BASE = "A002K";
		FACETS = "F28.A07LG";
		TERM_LEVEL = 6;
		ALLFACETS = "";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTFX19_0", "CONFIG", REPORT);
	run;

	%assertNobsEquals(TESTFX19_0, 1);
	%assertEquals(TESTFX19_0, 1, BR_CODE, "FOODEX2.19");
	%assertEquals(TESTFX19_0, 1, ERROR_IDS, "A07LG");
	%deleteTable(TESTFX19_0);
	%deleteTable(CONFIG);
%mend;

%macro testMultipleForbiddenProcessFX19;

	%print('TEST: testMultipleForbiddenProcessFX19');

	/* Test get forbidden processes */
	data CONFIG;
		HIERARCHY = "REPORT";
		GROUP_PARENT_CODE = "A000J";
		FORBIDDEN_PROCS = "A07LG$A0C03";
	run;

	data &input_temp_table.;
		resId = "test1";
		&termTypeCol. = "r";  /* it works only with RPC */
		BASE = "A002K";
		FACETS = "F28.A0C03$F28.A07LG";
		TERM_LEVEL = 6;
		ALLFACETS = "";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTFX19", "CONFIG", REPORT);
	run;

	%assertNobsEquals(TESTFX19, 1);
	%assertEquals(TESTFX19, 1, BR_CODE, "FOODEX2.19");
	%assertEquals(TESTFX19, 1, ERROR_IDS, "A07LG$A0C03");
	%deleteTable(TESTFX19);
	%deleteTable(CONFIG);
%mend;

%macro testMultipleForbiddenProc_2_FX19;

	%print('TEST: testMultipleForbiddenProcess_2_FX19');

	/* Test get forbidden processes */
	data CONFIG;
		HIERARCHY = "REPORT";
		GROUP_PARENT_CODE = "A000J";
		FORBIDDEN_PROCS = "A07LG$A0C03";
		output;
		HIERARCHY = "REPORT";
		GROUP_PARENT_CODE = "A000J";
		FORBIDDEN_PROCS = "A07LN$A07LA";
	run;

	data &input_temp_table.;
		resId = "test1";
		&termTypeCol. = "r";  /* it works only with RPC */
		BASE = "A002K";
		FACETS = "F28.A0C03$F28.A07LG$F28.A07LN$F28.A07LA";
		TERM_LEVEL = 6;
		ALLFACETS = "";
	run;

	data _null_;
		set &input_temp_table.;
		%evalErrors(BASE, BASE, FACETS, ALLFACETS, "TESTFX19_2", "CONFIG", REPORT);
	run;

	/* Only the first forbidden group is shown in the error */
	%assertNobsEquals(TESTFX19_2, 1);
	%assertEquals(TESTFX19_2, 1, BR_CODE, "FOODEX2.19");
	%assertEquals(TESTFX19_2, 1, ERROR_IDS, "A07LG$A0C03");
	%deleteTable(TESTFX19_2);
	%deleteTable(CONFIG);
%mend;


/* Execute all the tests above defined */
%macro executeTests;

	/* Check errors */
	%testSingleF27ForRPCFX01;
	%testF01ForMixedDerivativeFX02;
	%testF01ForCompositeFX03;
	%testF27ForCompositeFX04;
	%F27DerivNotChildOfImplF27FX05;
	%testF27ParentOfImplicitFX05;
	%testF01ForDerivWithNoF27FX06;
	%testF01ForDerivImplicitF27FX06;

	%testFacetForBaseTermFX17;

	%testForbiddenProcessFX19;
	%testMultipleForbiddenProcessFX19;
	%testMultipleForbiddenProc_2_FX19;


	/* Check warnings */
	%testNotReportableWarningFX08;
	%testHierarchyBaseTermFX09;
	%testNonSpecificTermFX10;
	%testGenericTermFX11;
	%testMinorIngredientForRPCFX12;
	%testMinorIngredientForDFX12;
	%testMinorMultipleIngrForDFX12;
	%testDerivF01AndSingleF27FX13;
	%testAmbiguousTermFX18;

	/* warnings and errors for facets preprocessing */
	%testCheckCardinalityFX07;
	%testRepeatedExplicitFX14;
	%testExplRepeatedInImplFX15;
	%testExplicitParentImplicitFX16;
%mend;

%executeTests;

/* Test remove implicit facets from explicit facets */
data _null_;

	length test $1000;

	test = removeElementsFromList("22222$11111$22222", "22222$33333", "$");
	%assertEqualsValue("11111", test);

	test = removeElementsFromList("", "22222$33333", "$");
	%assertEqualsValue("", test);

	test = removeElementsFromList("11111$22222", "22222$33333", "$"); /* only first remains */ 
	%assertEqualsValue("11111", test);
	
	test = removeElementsFromList("11111", "22222$33333", "$");
	%assertEqualsValue("11111", test);

	test = removeElementsFromList("22222", "22222$33333", "$");
	%assertEqualsValue("", test);

	test = removeElementsFromList("11111$22222", "", "$");
	%assertEqualsValue("11111$22222", test);

	test = removeElementsFromList("11111$22222", "11111", "$"); /* only last remains */ 
	%assertEqualsValue("22222", test);

	test = removeElementsFromList("11111$11111$22222", "11111", "$");
	%assertEqualsValue("22222", test);

	test = removeElementsFromList("11111-11111-22222", "11111", "-");
	%assertEqualsValue("22222", test);
run;

/*
data input;
	BASE = "AUHGV";
	FACETS = "F01.A058Y$F01.A16MN$F01.A058Y$F01.A098Y";
	TERMTYPE = "r";
	DETAILLEVEL = "h";
	ALLFACETS = "F01.A058Y";
	REPORTREPORTABLE = 0;
	output;

	BASE = "A08HJ";
	FACETS = "F27.A058Y$F27.A16MN$F27.A058Y$F27.A098Y";
	TERMTYPE = "r";
	DETAILLEVEL = "h";
	ALLFACETS = "F27.A058Y";
	REPORTREPORTABLE = 0;
	output;
run;
data AAA;
	set input;
	%evalWarnings(BASE, BASE, FACETS, REPORT, "TEST");
run;*/

/* Test is generic */
data _null_;
	generic = isGeneric("A0C0R");
	%assertEqualsValue(1, generic);
run;

data _null_;
	ambiguous = isAmbiguous("A00HQ");
	%assertEqualsValue(1, ambiguous);
run;

data _null_;
	nonspecific = isNonSpecific("A001Y");
	%assertEqualsValue(1, nonspecific);
run;

data _null_;
	facets = cleanFacets(BASE, "F01.A058Y$F01.A16MN$F01.A058Y", "F01.A058Y", "AAA");
	%assertEqualsValue("F01.A16MN", facets);
run;

/* Test get forbidden processes */
data CONFIG;
	HIERARCHY = "REPORT";
	GROUP_PARENT_CODE = "A000J";
	FORBIDDEN_PROCS = "A07LG$A07LH";
	output;
	HIERARCHY = "REPORT";
	GROUP_PARENT_CODE = "A000J";
	FORBIDDEN_PROCS = "A07LL$A0C03";
	output;
run;

data _null_;
	procs = getForbiddenProcesses("A002K", 7, "REPORT", "CONFIG");
	%assertEqualsValue("A07LG$A07LH#A07LL$A0C03", procs);
run;

data _null_;
	procs = getForbiddenProcesses("ZZZZZ", 7, "REPORT", "CONFIG");
	%assertEqualsValue("", procs);
run;

data _null_;
	procs = getForbiddenProcesses("A07XJ", 7, "REPORT", "CONFIG");
	%assertEqualsValue("", procs);
run;

/* test areProcessesForbidden */
data _null_;
	forbidden = areProcessesForbidden("A002K", 7, "REPORT", "F28.A07LG$F28.A07LH", "CONFIG");
	%assertEqualsValue("A07LG$A07LH", forbidden);

	forbidden = areProcessesForbidden("A002K", 7, "REPORT", "F28.A07LL$F28.A0C03", "CONFIG");
	%assertEqualsValue("A07LL$A0C03", forbidden);

	forbidden = areProcessesForbidden("A002K", 7, "REPORT", "F28.A07LG", "CONFIG");
	%assertEqualsValue("", forbidden);

	forbidden = areProcessesForbidden("A002K", 7, "REPORT", "", "CONFIG");
	%assertEqualsValue("", forbidden);
run;

/*
data input;
	resId = "10239.20";
	FXCODE = "A0B6Z#F01.A058Y$F01.A16MN$F01.A058Y$F01.A098Y";
	output;

	resId = "32132.13";
	FXCODE = "A0B6Z#F27.A058Y$F27.A16MN$F27.A058Y$F27.A098Y";
	output;
run;

%DEAV_FOODEX2_VALIDATION(input, FAILED_RECORDS, FXCODE);
%deleteTable(input);

data input;
	resId = "test1";
	FXCODE = "A001X#F01.A000J";
	output;
run;
%DEAV_FOODEX2_VALIDATION(input, FAILED_RECORDS, FXCODE);
%deleteTable(input);*/