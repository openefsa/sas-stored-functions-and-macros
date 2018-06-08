options mstored sasmstore=MSTORE;

%macro DEAV_FOODEX2_TO_MATRIX(inputTable= /* Input table */, 
		outputTable= /* Table where errors and warnings will be put */,
		foodex2Column= /* Name of the column which contains the FoodEx2 codes to validate */,
		matrixColumn= /* Name of the column which will be created containing the matrix code */,
		idColumns= /* List of space separated columns which identify a row of the inputTable */,
		statistics=0 /*Optional, 1=compute performance statistics*/)
	/ store source des="Map FoodEx2 to matrix code";

	data INPUT_WITH_IDS;
		set &inputTable.;
		keep &idColumns. &foodex2Column.;
	run;

	%local errorTable;
	%let errorTable = DEAV_FOODEX2_TO_MATRIX_ERR;

	/* Delete the error table if already exists */
	%deleteDataset(&errorTable.);

	%DEAV_CREATE_EMPTY_ERR_TABLE(&errorTable.);

	data &inputTable.;
		set &inputTable.;
		&matrixColumn. = "dummyMatrixCode";
	run;

	/* Merge the results in the output table (in order to have row identifiers in the error table ) */
	proc sql noprint;
		create table &outputTable. as
		select input.*, e.ERR_CODE, e.ERR_TYPE, e.ERR_MESSAGE, e.ERR_COLUMN, e.ERR_VALUE
		from &errorTable. e
		inner join INPUT_WITH_IDS input
		on input.&foodex2Column. = e.ERR_VALUE;

		drop table &errorTable.;
		drop table INPUT_WITH_IDS;
	run;

	/* remove foodex2 column because it is not needed in the error table */
	data &outputTable.;
		set &outputTable.;
		drop &foodex2Column.;
	run;
%mend;
