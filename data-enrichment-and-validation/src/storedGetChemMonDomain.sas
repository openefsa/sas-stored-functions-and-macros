options mstored sasmstore=MSTORE;
options fmtsearch=(FMTLIB BRS_STG); * Required to use BR formats for parents ;
options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.dcf MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables MSTORE.MAPPING_MATRIX MSTORE.Get_Domains);


%macro DEAV_GetChemMonDomain(inputTable= /* Input table */, 
		outputTable= /* Table where errors and warnings will be put */,
		ProgLegRefColumn= /* Name of the column of the input table which contains the Program Legal Reference codes to validate */,
		ParamColumn=/* Name of the column of the input table which contains theParam codes to validate */,
		is_VetColumn=is_Vet /* Name of the column which will be created containing the 0/1 flag for the Veterinary Drugs Domain */,
		is_AddColumn=is_Add /* Name of the column which will be created containing the 0/1 flag for the Additives Domain */,
		is_PestColumn=is_Pest /* Name of the column which will be created containing the 0/1 flag for the Pesticides Domain */,
		is_ChemColumn=is_Chem /* Name of the column which will be created containing the 0/1 flag for the Chemical Contaminants Domain */,
		idColumns= /* List of space separated columns which identify a row of the inputTable */,
		statistics=0 /*Optional, 1=compute performance statistics*/)
		/ store source des="Get Chemical Monitoring domain from LegRef and Param codes"
		;



	%local errorTable;
	%let errorTable = DEAV_GetCheMonDomain_ERR;

	%deleteDataset(&errorTable.);
	%DEAV_CREATE_EMPTY_ERR_TABLE(&errorTable.);

	proc sql noprint;
		create table distinct_combination as
		select distinct &ProgLegRefColumn.,  &ParamColumn.
		from &inputTable.
		;
	quit;

	data mapping_applied;
		set distinct_combination;
		length	&is_VetColumn.  3
					&is_AddColumn.  3
					&is_PestColumn.  3
					&is_ChemColumn.  3
					ERROR_COL $100
					;

		_rc_=GetChemMonDomain(&ProgLegRefColumn.,
						&ParamColumn.,
						&is_VetColumn.,
						&is_AddColumn.,
						&is_PestColumn.,
						&is_ChemColumn.,
						ERROR_COL);
		drop _rc_;

	run;


	proc sql;
		create table mapping_applied_err as
		select * 
		from mapping_applied 
		where ERROR_COL^="";
	quit;

	%let n_err=&sqlobs;

	%if &n_err>0 %then %do;


		/* if the input table contains a dataset_id columns, then include in the error table the column*/
		proc contents data=&inputTable. out=contents_input;
		run;

		proc sql;
			select * from contents_input where upcase(name) = "DATASET_ID";
		quit;

		%let dataset_exists=&sqlobs.;

		%if &dataset_exists.>0 %then %do;
			proc sql;
				select distinct dataset_id into :ds_id_tmp1-
				from &inputTable.
			quit;

			data mapping_applied_err;
				set mapping_applied_err;
				/* add the first dataset_id, in case of more then one dataset_id only the first one is kept*/
				DATASET_ID=&ds_id_tmp1.;
				TIMESTAMP=datetime();
				format TIMESTAMP datetime20.;
			run;
		%end;

		* send e-mail with the mapping errors;
		%let toid=Data.ETL@efsa.europa.eu;
		%let toid2=Valentina.BOCCA@efsa.europa.eu;

		%let currenttime=%sysfunc(compress(%sysfunc(tranwrd(%sysfunc(datetime()),%quote(.),%quote()))));
		%put currenttime = &currenttime.;
		/* &server_path is a custom macro variable created in the autoexec */
		%let exportfile=&server_path.\DataShare\Data\DATA\SSD2\Errors_GettingDomains\LegRef_and_Param_err_&currenttime..xlsx;	
		proc export data=mapping_applied_err dbms=xlsx outfile=%str("&exportfile.");
		quit;
		
		%send_email(toAddress=&toid.,ccAddress=&toid2.,subject=Warning: Getting Domains failed, message="Prog Reference and Param with issues are stored in &exportfile.",attachFile= );

		* this is a global macro variable of the business rules engine;
		* setting this macro variable to 1 means that this external process had some error ; 
		%let errorFlag=1;

	%end;

	proc sql noprint;
		create table &inputTable. as
		select 	t2.&is_VetColumn.,
						t2.&is_AddColumn.,
						t2.&is_PestColumn.,
						t2.&is_ChemColumn.,
					t1.*
		from &inputTable. as t1
			left join mapping_applied as t2
				on (t1.&ProgLegRefColumn.=t2.&ProgLegRefColumn.
						and 
					t1.&ParamColumn.=t2.&ParamColumn.)
				;
	quit;


/**************************/
/*GDP: I don't understand this piece of code... it seems that it creates a dummy table &outputtable. */

	* Delete and recreate the error table if already exists ;
	%deleteDataset(&errorTable.);
	%DEAV_CREATE_EMPTY_ERR_TABLE(&errorTable.);

	*create dummy table;
	data INPUT_WITH_IDS;
		set &inputTable (OBS=0);
		keep &idColumns &ProgLegRefColumn. &ParamColumn.;
	run;

	/* Merge the results in the output table */
	proc sql noprint;
		create table &outputTable. as
		select input.*, e.ERR_CODE, e.ERR_TYPE, e.ERR_MESSAGE, e.ERR_COLUMN, e.ERR_VALUE
		from &errorTable. e
		inner join INPUT_WITH_IDS input
		on input.&ProgLegRefColumn. = e.ERR_VALUE;
	run;

/**************************/


%mend DEAV_GetChemMonDomain;

* Functions which are related to mapping of the ProgLegRef and Param into the domain flags ;
proc fcmp outlib = MSTORE.Get_Domains.Get_Domains ;


function commonDomain(String1 $ /* */,
							String2 $ /* */, 
							separator $ /* */
							)
						$4000 
						;


	length 	out $4000 token1 $100 token2 $100;

%stringTokenizer(string1, separator, token1);
	%stringTokenizer(string2, separator, token2);
		if token2=token1 then out=CATT(out,separator,token1);
	%endStringTokenizer;
%endStringTokenizer;

return(out);

endsub;

function distinctDomain(String1 $ /* */,
							separator $ /* */
							)
						$4000 
						;


	length 	distinct $4000 token1 $100 found $100;

%stringTokenizer(string1, separator, token1);
	
		if(missing(compress(distinct))) then distinct = token1; /* first value */
		else do;

			found = indexw(distinct, token1);

			if(^found) then do;
				distinct = catx(" ", distinct, token1); /* only spaces work with indexw */
			end;
		end;
%endStringTokenizer;

return(distinct);

endsub;

function GetChemMonDomain(ProgLegRefColumn $ /* */,
							ParamColumn $ /* */, 
							is_VetColumn  /* */, 
							is_AddColumn  /* */, 
							is_PestColumn  /* */,  
							is_ChemColumn  /* */,  
						errorMessage $ /* column where to retrive an error message */)
						$15 
						;


	outargs is_VetColumn,
			is_AddColumn,
			is_PestColumn, 
			is_ChemColumn,
			errorMessage;
	length 	errorMessage $200 
			PLR_Domain $1000  
			common $1000  
			token1 $1000  
			Par_Domain $1000  
			distinct $1000  
			union $1000;

			union="";
			is_VetColumn = 0;
			is_AddColumn = 0;
			is_PestColumn = 0;
			is_ChemColumn = 0;

	if input(ParamColumn,VMPRANALYSIS_REPORTABLE_.)=1 then do;
		if put(ParamColumn,$VMPRANALYSIS_LEVEL2_.) in (	"RF-00004855-PAR", 
														"RF-00004868-PAR", 
														"RF-00004875-PAR", 
														"RF-00004869-PAR", 
														"RF-00004876-PAR", 
														"RF-00004879-PAR") 
		then Par_Domain=CATT(Par_Domain,"$VMPR2");
		else Par_Domain=CATT(Par_Domain,"$VMPR1");
	end;
	if input(ParamColumn,chemAnalysis_REPORTABLE_.)=1 then Par_Domain=CATT(Par_Domain,"$CHEMOCC");
	if input(ParamColumn,addAnalysis_REPORTABLE_.)=1 then Par_Domain=CATT(Par_Domain,"$ADD");
	/* this hier does not exist... to be added in 2020
	if input(ParamColumn,PESTANALYSIS_REPORTABLE_.)=1 then Par_Domain=CATT(Par_Domain,"$PEST");
	*/

	%stringTokenizer(ProgLegRefColumn, "$", token1);
		PLR_Domain=put(token1,$LEGREF_domain_.);
		PLR_Domain=tranwrd(PLR_Domain,"VMPR","VMPR1$VMPR2");
		common=commonDomain(PLR_Domain, Par_Domain, "$");
		union=CATT(union,common);
	%endStringTokenizer;


	/*distinct=distinctDomain(union,"$");*/

	%stringTokenizer(union, "$", token1);
		if token1="ADD" then is_AddColumn = 1;
		else if token1="CHEMOCC" then is_ChemColumn = 1;
		else if token1="PEST" then is_PestColumn = 1;
		else if token1="VMPR1" then is_VetColumn = 1;
		else if token1="VMPR2" then is_VetColumn = 2;
	%endStringTokenizer;

return(" ");

endsub;


run;
