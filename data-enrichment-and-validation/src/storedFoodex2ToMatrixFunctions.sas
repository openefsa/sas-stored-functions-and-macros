%let debug = 0;  /* 1 = enable dubug messages, 0 = disable (it is needed to recompile all the functions if changed) */
/*
options mstored sasmstore=MSTORE;

%Sysmstoreclear;
*/
options mstored sasmstore=MSTORE;
options fmtsearch=(FMTLIB BRS_STG); * Required to use BR formats for parents ;
options cmplib=(MSTORE.strings MSTORE.catalogues MSTORE.mtx MSTORE.dcf MSTORE.DEAV MSTORE.FOODEX2_VALIDATION MSTORE.tables MSTORE.MAPPING_MATRIX);


%macro send_email(toAddress=, ccAddress=, subject=, message=, attachFile=) / store source des="Utility macro for sending emails";

                filename mail_dm email;
                data _null_;
                                file mail_dm
                                                to=("&toAddress.")
                                %if ("&ccAddress." ne "") %then %do;
                                                cc=("&ccAddress.")
                                %end;
                                                subject="SASJOBS Automated Mail: &subject"
                                %if ("&attachFile." ne "") %then %do;
                                               attach=("&attachFile.")
                                %end;
                                ;
                                put &message.;
                                put ;
                                
                                /*%if ("&logFileStorage" ne "") %then %do;*/ put "See Log for details."; /*%end;*/
                run;

%mend send_email;

%macro DEAV_FOODEX2_TO_MATRIX(inputTable= /* Input table */, 
		outputTable= /* Table where errors and warnings will be put */,
		foodex2Column= /* Name of the column of the input table which contains the FoodEx2 codes to validate */,
		matrixColumn=MatrixCode /* Name of the column which will be created containing the matrix code, the defalut value is MatrixCode */,
		foodex1Column=Foodex1Code /* Name of the column which will be created containing the foodex1 code, the defalut value is Foodex1Code */,
		prodTrColumn=ProdTrCode /* Name of the column which will be created containing the production treatement code, the defalut value is ProdTrCode */,
		prodMdColumn=ProdMdCode /* Name of the column which will be created containing the Production method code, the defalut value is ProdProdMethCode */,
		prodPacColumn=ProdPacCode /* Name of the column which will be created containing the production packaging code, the defalut value is ProdPacCode */,
		idColumns= /* List of space separated columns which identify a row of the inputTable */,
		statistics=0 /*Optional, 1=compute performance statistics*/)
		/ store source des="Map FoodEx2 to matrix code"
		;




	%local errorTable;
	%let errorTable = DEAV_FOODEX2_TO_MATRIX_ERR;


	proc sql noprint;
		create table distinct_foodex2 as
		select distinct &foodex2Column. 
		from &inputTable.
		;
	quit;




	data mapping_applied;
		set distinct_foodex2;
		length	&matrixColumn.  $100
					&foodex1Column.  $100
					&prodTrColumn.  $100
					&prodMdColumn.  $100
					&prodPacColumn.  $100
					ERROR_COL $100
					;
		_rc_=toSSD1Product(&foodex2Column., 
						&matrixColumn.,
						&foodex1Column.,
						&prodTrColumn.,
						&prodMdColumn.,
						&prodPacColumn.,
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
		
		%let exportfile=\\Efsa.eu.int\fileserver\SASOperational\DataShare\Data\DATA\SSD2\Errors_MATRIX_mapping\foodex2codes_w_errors_&currenttime..xlsx;	
		proc export data=mapping_applied_err dbms=xlsx outfile=%str("&exportfile.");
		quit;
		
		%send_email(toAddress=&toid.,ccAddress=&toid2.,subject=Warning: MATRIX mapping failed, message="MTX codes with issues are stored in &exportfile.",attachFile= );

		* this is a global macro variable of the business rules engine;
		* setting this macro variable to 1 means that this external process had some error ; 
		%let errorFlag=1;
	%end;



	proc sql noprint;
		create table &inputTable. as
		select 	t2.&matrixColumn.,
					t2.&foodex1Column.,
					t2.&prodTrColumn.,
					t2.&prodMdColumn.,
					t2.&prodPacColumn.,
					t1.*
		from &inputTable. as t1
			left join mapping_applied as t2
				on (t1.&foodex2Column.=t2.&foodex2Column.)
				;
	quit;

	%local errorTable;
	%let errorTable = DEAV_MAPPING_MATRIX_ERR;

	* Delete and recreate the error table if already exists ;
	%deleteDataset(&errorTable.);
	%DEAV_CREATE_EMPTY_ERR_TABLE(&errorTable.);

	*create dummy table;
	data INPUT_WITH_IDS;
		set &inputTable (OBS=0);
		keep &idColumns &foodex2Column.;
	run;

	/* Merge the results in the output table */
	proc sql noprint;
		create table &outputTable. as
		select input.*, e.ERR_CODE, e.ERR_TYPE, e.ERR_MESSAGE, e.ERR_COLUMN, e.ERR_VALUE
		from &errorTable. e
		inner join INPUT_WITH_IDS input
		on input.&foodex2Column. = e.ERR_VALUE;
	run;




%mend;


* Functions which are related to mapping of the foodex2code into the SSD1 product descriptor ;
proc fcmp outlib = MSTORE.MAPPING_MATRIX.MAPPING_MATRIX ;


function toMatrix(MTXterm $ /*MTX term to be mapped to the matrix catalog*/, 
						Excluding_visible_fat_flag $ /*flag used for "Muscle" exception in the Matrix catalog*/,
						errorMessage $ /* column where to retrive an error message */)
						$15 
						;
	outargs errorMessage;
	length 	errorMessage $200;

	length MatrixCode $30;

	*Get matrix code from the term attributes;
	MatrixCode = put(MTXterm,$MTX_matrixCode_.);

	if MatrixCode="" then do;
		* return "not in list" code;
		return("XXXXXXA");
	end;
	else do;

		matrixcodefound:

		if Excluding_visible_fat_flag=1 then do;
			* Search in the MATRIX catalogue if the matrix code with suffix ‘B’ is present;
			if input(CATT(MatrixCode,"B"),imatrix_W_HLO.)>0 then do;
				return(CATT(MatrixCode,"B"));
			end;
			else do;
				goto no_B_exception; 
			end;
		end;
		else do;
			
			no_B_exception:

			* Search in the MATRIX catalogue if the matrix code with suffix ‘A’ is present;
			if input(CATT(MatrixCode,"A"),imatrix_W_HLO.)>0 then do;
				return(CATT(MatrixCode,"A"));
			end;
			* Search in the MATRIX catalogue if the matrix code is present;
			else if input(MatrixCode,imatrix_W_HLO.)>0 then do;
				return(MatrixCode);
			end;
			else if index(MatrixCode,"-")>0 then do;
				MatrixCode=scan(MatrixCode,1,"-");
				goto matrixcodefound;
			end;
			else do;
				errorMessage="Internal issue with the matrixCode attribute of the MTX catalog";

				* return "not in list" code;
				return("XXXXXXA");
			end;
		end;
	end;
endsub;


* this function removes from a list of facets the ones flagged "unprocessed" using the format iMTX_PRODTR_PESTPROCESSED_ ;
function removeUnprocessedFacets(facets $) $4000;
	
	length out $ 4000
		process $2;

	%iterateFacets(facets, f, h, c)
		f_code=substr(f,5);
		process=input(f_code,iMTX_PRODTR_PESTPROCESSED_.);
		if h ^= "F28" or process=1 then do;
			if (missing(compress(out))) then out = f;
			else out = catx("$", compress(out), f);
		end;
	%endIterateFacets

	return(out);

endsub;


* this function removes from a list of facets the facets F28.A07KG ;
function removeFacetA07KG(facets $) $4000;
	
	length out $ 4000;

	%iterateFacets(facets, f, h, c)
		if f ^= "F28.A07KG" then do;
			if (missing(compress(out))) then out = f;
			else out = catx("$", compress(out), f);
		end;
	%endIterateFacets

	return(out);

endsub;



* this function removes from a list of facets the facets F28.A07KQ, F28.A07KP, and F28.A07JF ;
function removeFacetA07KQ_A07KP_A07JF(facets $) $4000;
	
	length out $ 4000;

	%iterateFacets(facets, f, h, c)
		if f ^= "F28.A07KQ" and f ^= "F28.A07KP" and f ^= "F28.A07JF" then do;
			if (missing(compress(out))) then out = f;
			else out = catx("$", compress(out), f);
		end;
	%endIterateFacets

	return(out);

endsub;







function toProdTr(MTXterm $ /*MTX term to be mapped to the matrix catalog*/,
						errorMessage $ /* column where to retrive an error message */)
						$15 
						;
	outargs errorMessage;
	length 	errorMessage $200;

	*Get ProdTR code from the term;
	if put(MTXterm,$MTX_prodTreat_.)^="" then do;
		return(put(MTXterm,$MTX_prodTreat_.));
	end;
	else do;
		errorMessage="Internal issue with the prodTreat attribute of the MTX catalog";


		* return unprocess ;
		return("T999A");
	end;
			
endsub;


function toProdPAC(MTXterm $ /*MTX term to be mapped to the matrix catalog*/,
						errorMessage $ /* column where to retrive an error message */)
						$15 
						;
	outargs errorMessage;
	length 	errorMessage $200;

	*Get ProdTR code from the term;
	if put(MTXterm,$MTX_prodPack_.)^="" then do;
		return(put(MTXterm,$MTX_prodPack_.));
	end;
	else do;
	
		errorMessage="Internal issue with the prodPack attribute of the MTX catalog";

		*return a missing value;
		return("");
	end;
			
endsub;


function toProdMD(MTXterm $ /*MTX term to be mapped to the matrix catalog*/,
						errorMessage $ /* column where to retrive an error message */)
						$15 
						;
	outargs errorMessage;
	length 	errorMessage $200;

	*Get ProdTR code from the term;
	if put(MTXterm,$MTX_prodMeth_.)^="" then do;
		return(put(MTXterm,$MTX_prodMeth_.));
	end;
	else do;
		
		errorMessage="Internal issue with the prodMeth attribute of the MTX catalog";

		*return a missing value;
		return("");
	end;
			
endsub;


function getMostRelevantFacet(facets $/*MTX term to be mapped to the ssd1 catalog*/,
							facet_category $ /* it could be PROCESS_IMPORTANCE, METHOD_IMPORTANCE, PACKAGIN_IMPORTANCE*/)
						$15 
						;
	length i 8
			maxindex 8
			minorder 8
			order 8;
	array facet[1000] $15;
	i=1;
	minorder=100000;
	maxindex=1;

	do until (scan(facets,i,"$")="");
		facet[i]=scan(scan(facets,i,"$"),2,".");
		if facet_category="PROCESS_IMPORTANCE" then do;
			order=input(facet[i],iMTX_PRODTR_ORDER_.);
		end;
		if facet_category="METHOD_IMPORTANCE" then do;
			order=input(facet[i],iMTX_PRODMD_ORDER_.);
		end;
		if facet_category="PACKAGING_IMPORTANCE" then do;
			order=input(facet[i],iMTX_PRODPAC_ORDER_.);
		end;
		if 	order<minorder then  do;
			minorder=order;
			maxindex=i;
		end;
		i=i+1;
	end;
	
	return(facet[maxindex]);

			
endsub;



function getProdTreatment(foodex2Column $ /*foodex2 to be mapped to ssd1 product values*/, 
						ERROR_message $ /*name of column containing concatenate error messages*/)
						$15 
						;

	outargs	ERROR_message;
	length 	ERROR_message $200
			baseterm $5
			facetslist $4000
			MERGED_FACETS $4000
			MERGED_FACETS_process $4000
			MERGED_FACETS_only_pestProcess $4000
			implicit_facets $4000
			allfacets $4000
			process_selected $15
			count_f27 2;

	*ERROR_message="test errors";
	
	* comupte the base term of the foodex2 code ;
	baseterm=getBaseTermFromCode(foodex2Column);
	facetslist=getFacetsFromCode(foodex2Column);
	facetslist=removeFacetA07KQ_A07KP_A07JF(facetslist);

	* add implicits facets to the code ;
	allfacets=put(baseterm,$MTX_allfacets_.);
	implicit_facets=getFacetsFromCode(allfacets);
	implicit_facets=removeFacetA07KQ_A07KP_A07JF(implicit_facets);
	MERGED_FACETS = addImplicitFacets(facetslist,implicit_facets);

	*get only process facets;
	MERGED_FACETS_process=getFacetsByCategory(MERGED_FACETS,"F28");
	MERGED_FACETS_only_pestProcess=removeUnprocessedFacets(MERGED_FACETS_process);
	implicit_facets_only_pestProcess=removeUnprocessedFacets(implicit_facets);




	*******************************;
	*in the documentation is written to remove not relevant process to pesticides... i think it is not updated;
	*******************************;



	* look in the parent codes (exposure hierarchy) of the base term for the codes A03GG (Coffee, cocoa, tea and infusions), A016S (Spices), or A012R (Pulses (dried legume seeds) ) ;
	if  index(put(baseterm,$EXPO_ccatLEVEL.),"A012R")>0 or
		index(put(baseterm,$EXPO_ccatLEVEL.),"A016S")>0 or	
		index(put(baseterm,$EXPO_ccatLEVEL.),"A03GG")>0 
	then do;

		*Remove from implicit and explicit facets the process A07KG;
		MERGED_FACETS_only_pestProcess=removeFacetA07KG(MERGED_FACETS_only_pestProcess);
		implicit_facets_only_pestProcess=removeFacetA07KG(implicit_facets_only_pestProcess);
		implicit_facets=removeFacetA07KG(implicit_facets);
		MERGED_FACETS_process=removeFacetA07KG(MERGED_FACETS_process);

		goto noexception;
	end;
	else do;
	
		* look in the parent codes (exposure hierarchy) of the base term for the code A03PV (babyfood) ;
		if index(put(baseterm,$EXPO_ccatLEVEL.),"A03PV")>0 then do;
		
			count_processed=countFacets(MERGED_FACETS_only_pestProcess,"F28");
			if count_processed>0 then do;
				goto noexception;
			end;
			else do;
				* return Processed ;
				return("T100A");
			end;
		end;
		else do;
			
			noexception:
			
			count_processed_ALL=countFacets(MERGED_FACETS_only_pestProcess,"F28");
			count_processed_implicit=countFacets(implicit_facets_only_pestProcess,"F28");
			count_implicit=countFacets(implicit_facets,"F28");
			count_ALL=countFacets(MERGED_FACETS_process,"F28");
			
			if count_processed_implicit>0 then do;
				
				process_selected=getMostRelevantFacet(implicit_facets, "PROCESS_IMPORTANCE");
			end;
			else if count_processed_ALL>0 then do;

				process_selected=getMostRelevantFacet(MERGED_FACETS_process, "PROCESS_IMPORTANCE");
			end;
			else if count_implicit>0 then do;

				process_selected=getMostRelevantFacet(implicit_facets, "PROCESS_IMPORTANCE");
			end;
			else if count_ALL>0 then do;

				process_selected=getMostRelevantFacet(MERGED_FACETS_process, "PROCESS_IMPORTANCE");
			end;
			else do;
				* return unprocessed ;
				return("T999A");
			end;

			return(toProdTr(process_selected,ERROR_message));

		end;
	end;

endsub;



function getProdMethod(foodex2Column $ /*foodex2 to be mapped to ssd1 product values*/, 
						ERROR_message $ /*name of column containing concatenate error messages*/)
						$15 
						;

	outargs	ERROR_message;
	length 	ERROR_message $200
			baseterm $5
			facetslist $4000
			MERGED_FACETS $4000
			implicit_facets $4000
			allfacets $4000
			;

	*ERROR_message="test errors";
	
	* comupte the base term of the foodex2 code ;
	baseterm=getBaseTermFromCode(foodex2Column);
	facetslist=getFacetsFromCode(foodex2Column);

	* add implicits facets to the code ;
	allfacets=put(baseterm,$MTX_allfacets_.);
	implicit_facets=getFacetsFromCode(allfacets);
	MERGED_FACETS = addImplicitFacets(facetslist,implicit_facets);

	* look in the parent codes (exposure hierarchy) of the base term for the code A03PV (babyfood) ;
	count_prod=countFacets(MERGED_FACETS,"F21");
	if count_prod>0 then do;
		prod_selected=getMostRelevantFacet(MERGED_FACETS, "METHOD_IMPORTANCE");
		return(toProdMD(prod_selected,ERROR_message));
	end;
	else do;
		* return Non-organic production ;
		return("PD09A");
	end;
	

endsub;




function getProdPackaging(foodex2Column $ /*foodex2 to be mapped to ssd1 product values*/, 
						ERROR_message $ /*name of column containing concatenate error messages*/)
						$15 
						;

	outargs	ERROR_message;
	length 	ERROR_message $200
			baseterm $5
			facetslist $4000
			MERGED_FACETS $4000
			implicit_facets $4000
			allfacets $4000
			;

	
	* comupte the base term of the foodex2 code ;
	baseterm=getBaseTermFromCode(foodex2Column);
	facetslist=getFacetsFromCode(foodex2Column);

	* add implicits facets to the code ;
	allfacets=put(baseterm,$MTX_allfacets_.);
	implicit_facets=getFacetsFromCode(allfacets);
	MERGED_FACETS = addImplicitFacets(facetslist,implicit_facets);

	* look in the parent codes (exposure hierarchy) of the base term for the code A03PV (babyfood) ;
	count_prodpac=countFacets(MERGED_FACETS,"F19");
	if count_prodpac>0 then do;
		prodpac_selected=getMostRelevantFacet(MERGED_FACETS, "PACKAGING_IMPORTANCE");
		return(toProdPAC(prodpac_selected,ERROR_message));
	end;
	else do;
		* return a missing value;
		return("");
	end;
	

endsub;






function getFoodEx1(foodex2Column $ /*foodex2 to be mapped to ssd1 product values*/, 
						ERROR_message $ /*name of column containing concatenate error messages*/)
						$15 
						;

	outargs	ERROR_message;
	length 	ERROR_message $200
			baseterm $5
			baseterm_hier $500
			foodex1 $15
			i $3
			;

	* comupte the base term of the foodex2 code ;
	baseterm=getBaseTermFromCode(foodex2Column);
	baseterm_hier=put(baseterm,$EXPO_ccatLEVEL.);
	if baseterm_hier="" then do;
		baseterm_hier=put(baseterm,$FEED_ccatLEVEL.);
	end;

	i=1;
	do until (compress(scan(baseterm_hier,i, "-","b"))="");

		foodex1=put(compress(scan(baseterm_hier,i, "-","b")),$MTX_foodexOldCode_.);
		if foodex1="" then do;
			i=i+1;
		end;
		else do;
			return(foodex1);
		end;
	end;
	* return a missing value;
	ERROR_message="failed to map foodex1";
	return("");

	

endsub;


function getMatrix(foodex2Column $ /*foodex2 to be mapped to ssd1 product values*/, 
						ERROR_message $ /*name of column containing concatenate error messages*/)
						$15 
						;

	outargs	ERROR_message;
	length 	ERROR_message $200
			baseterm $5
			facetslist $4000
			MERGED_FACETS $4000
			implicit_facets $4000
			allfacets $4000
			Excluding_visible_fat_flag 2
			count_f27 2;

	*ERROR_message="test errors";

	* compute Excluding_visible_fat_flag used for "Muscle" exception in the Matrix catalog;
	if index(foodex2Column,"F20.A0F4V")>0 then Excluding_visible_fat_flag=1;
	else Excluding_visible_fat_flag=0;
	
	* comupte the base term of the foodex2 code ;
	baseterm=getBaseTermFromCode(foodex2Column);
	facetslist=getFacetsFromCode(foodex2Column);

	* add implicits facets to the code ;
	allfacets=put(baseterm,$MTX_allfacets_.);
	implicit_facets=getFacetsFromCode(allfacets);
	MERGED_FACETS = addImplicitFacets(facetslist,implicit_facets);


	*if the base term is part of the feed hierarchy;
	if compress(put(baseterm,$FEED_ccatLEVEL.))^="" then do;
		return("P1200000A");
	end;


	* look in the parent codes (exposure hierarchy) of the base term for the code A03PV (babyfood) ;
	if index(put(baseterm,$EXPO_ccatLEVEL.),"A03PV")>0 then do;
		return(toMatrix(baseterm,Excluding_visible_fat_flag,ERROR_message));
	end;
	else do;
		count_f27=countFacets(MERGED_FACETS,"F27");
		if count_f27=0 then do;	
			*apply th toMatrix function to the base term ;
			return(toMatrix(baseterm,Excluding_visible_fat_flag,ERROR_message));
		end;
		else if count_f27>1 then do;	
			* return "not in list" code;
			return("XXXXXXA");
		end;
		else if count_f27=1 then do;	
			*apply th toMatrix function to the source commotity ;
			return(toMatrix(substr(MERGED_FACETS,index(MERGED_FACETS,"F27.")+4,5),Excluding_visible_fat_flag,ERROR_message));
		end;
	end;
endsub;

* this macro creates new columns with the mapping of a foodex2 code into the ssd1 product catalogue (i.e Foodex1, Matrix, ProdTr, ProdMd, ProdPac) ;
function toSSD1Product(foodex2Column $ /*foodex2 to be mapped to ssd1 product values*/, 
						matrixColumn $ /*name of the output column containing the matrix code, result of the mapping*/,
						foodex1Column $ /*name of the output column containing the foodex1 code, result of the mapping*/,
						prodTrColumn $ /*name of the output column containing the prodTreatement code, result of the mapping*/,
						prodProdMethColumn $ /*name of the output column containing the prodMethod code, result of the mapping*/,
						prodPacColumn $ /*name of the output column containing the prodPackagin code, result of the mapping*/,
						ERROR_COL $ /*name of the output column containing concatenate error messages*/);
	outargs	matrixColumn,
			foodex1Column,
			prodTrColumn,
			prodProdMethColumn,
			prodPacColumn,
			ERROR_COL
			;

	length 	matrixColumn $15
			foodex1Column $15
			prodTrColumn $15
			prodProdMethColumn $15
			prodPacColumn $15
			ERROR_message $200
			ERROR_COL $4000
			;
	ERROR_message="";
	matrixColumn=getMatrix(foodex2Column,ERROR_message);
		if ERROR_message^="" then do;
			ERROR_COL = CATX("$",ERROR_COL,ERROR_message);
		end;

	ERROR_message="";
	foodex1Column=getFoodex1(foodex2Column,ERROR_message);
		if ERROR_message^="" then do;
			ERROR_COL = CATX("$",ERROR_COL,ERROR_message);
		end;

	ERROR_message="";
	prodTrColumn=getProdTreatment(foodex2Column,ERROR_message);
		if ERROR_message^="" then do;
			ERROR_COL = CATX("$",ERROR_COL,ERROR_message);
		end;

	ERROR_message="";
	prodProdMethColumn=getProdMethod(foodex2Column,ERROR_message);
		if ERROR_message^="" then do;
			ERROR_COL = CATX("$",ERROR_COL,ERROR_message);
		end;

	ERROR_message="";
	prodPacColumn=getProdPackaging(foodex2Column,ERROR_message);
		if ERROR_message^="" then do;
			ERROR_COL = CATX("$",ERROR_COL,ERROR_message);
		end;

	return(1);
endsub;


run;



