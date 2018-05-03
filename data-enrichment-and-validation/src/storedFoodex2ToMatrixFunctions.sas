options mstored sasmstore=MSTORE;

%macro DEAV_FOODEX2_TO_MATRIX(input /* Input table */, 
		output /* Table where errors and warnings will be put */,
		foodex2Column /* Name of the column which contains the FoodEx2 codes to validate */,
		matrixColumn /* Name of the column which will be created containing the matrix code */)
	/ store source des="Map FoodEx2 to matrix code";

	%local errorTable;

	%let errorTable = ERR_TABLE;
	%DEAV_CREATE_EMPTY_ERR_TABLE(&errorTable.);

	data &input.;
		set &input.;
		&matrixColumn. = "dummyMatrixCode";
	run;

	data _null_;
		inputNobs = getNobs("&input.");
		errNobs = getNobs("&errorTable.");
		maxNobs = inputNobs * errNobs;
		putlog "NOTE: Inner join between &input. and &errorTable. on &foodex2Column.: &input. rows = " inputNobs ", &errorTable. rows = " errNobs ", expected maximum number of rows = " maxNobs;
	run;

	/* Merge the results in the output table */
	proc sql;
		create table &output. as
		select input.*, e.*
		from &errorTable. e
		inner join &input. input
		on input.&foodex2Column. = e.ERR_VALUE;
	run;

	proc sql;
		drop table &errorTable.;
	run;

%mend;
