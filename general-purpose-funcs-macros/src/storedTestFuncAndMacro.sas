options mstored sasmstore=MSTORE;

%macro assertExists(dataset) / store source des="Check if dataset exists";
	%if ^%sysfunc(exist(&dataset.)) %then %put ERROR: Assertion error, table &dataset. does not exist;
%mend;

%macro assertNotExist(dataset) / store source des="Check if dataset does not exist";
	%if %sysfunc(exist(&dataset.)) %then %put ERROR: Assertion error, table &dataset. exist;
%mend;

options cmplib=mstore.tables;
/* Assert that the number of rows of the dataset is the expected one */
%macro assertNobsEquals(dataset, expectedNum) / store source des="Check dataset nobs";
	%let nobs = %sysfunc(getNobs(&dataset.));
	%if &nobs. ^= &expectedNum. %then %put ERROR: Expected &expectedNum. rows found &nobs.;
%mend;

%macro assert(dataset, nrow /* which row should be considered */, 
	column /* which column should be considered */, expected /* the expected value */, operator /*'eq' or 'neq'*/) 
	/ store source des="Assert values are equal/not equal in a dataset";

	data _null_;
		set &dataset.;

		if (_N_ = symget("nrow")) then do;
			expected = &expected.;
			operator = &operator.;
			if (operator = 'eq' and &column. ^= expected) then do;
				putlog 'ERROR: Expected ' expected ', found ' &column.;
			end;
			if (operator = 'neq' and &column. = expected) then do;
				putlog 'ERROR: Expected not equals=' expected ' and ' &column.;
			end;
		end;
	run;
%mend;

%macro assertEquals(dataset, nrow /* which row should be considered */, 
	column /* which column should be considered */, expected /* the expected value */) / store source des="Check if values are equal in a dataset";
	%assert(&dataset., &nrow., &column., &expected., 'eq');
%mend;

%macro assertNotEquals(dataset, nrow /* which row should be considered */, 
	column /* which column should be considered */, notExpected /* the not expected value */) 
		/ store source des="Check if values are not equal in a dataset";
	%assert(&dataset., &nrow., &column., &notExpected., 'neq');
%mend;

%macro assertEqualsValue(expected, found) / store source des="Check if values are equal";

	%local e;
	%local f;

	if (&expected. ^= &found.) then do;

		/* apply quotes for non character variables */
		%if(%datatyp(&expected.) = CHAR) %then %let e = &expected.; %else %let e = "&expected.";
		%if(%datatyp(&found.) = CHAR) %then %let f = &found.; %else %let f = "&found.";

		putlog 'ERROR: expected=' &e. ' found=' &f.;
		
	end;
%mend;