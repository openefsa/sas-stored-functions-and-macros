*option mstored sasmstore=MSTORE;
/* 
###################### Logins ############################################################

assumptions:

Metadata server connected 

input macro variable : no input

Description:

This macro creates a table USERAUT containing for each SAS library 
the associated oracle authentication domain and userid.


########################################################################################################
*/


%MACRO LOGINS / store source des="Produces the output table USERAUT containing the oracle schemas(USERID) associated SAS library name(LIBNAME) and authorization name (AUTHDOMNAME)";
%if not (%sysfunc(exist(AXYPPD4_345)) and %sysfunc(exist(UserAut)))  %then %do;
	data AXYPPD4_345;
		  
		  length LoginObjId UserId IdentId AuthDomId $ 17
		         IdentType $ 32
		         Name DispName Desc uri uri2 uri3 AuthDomName $ 256;
		
		  call missing
		(LoginObjId, UserId, IdentType, IdentId, Name, DispName, Desc, AuthDomId, AuthDomName);
		  call missing(uri, uri2, uri3);
		  n=1;
		
		/* The METADATA_GETNOBJ function specifies to get the Login objects 
		in the repository. The n argument specifies to get the first object that
		matches the uri requested in the first argument. The uri argument is an output 
		variable. It will store the actual uri of the Login object that is returned. 
		The program prints an informational message if no objects are found. */
		
		  objrc=metadata_getnobj("omsobj:Login?@Id contains '.'",n,uri);
		  if objrc<=0 then put "NOTE: rc=" objrc 
		    "There are no Logins defined in this repository"
		    " or there was an error reading the repository.";
		
		/* The DO statement specifies a group of statements to be executed as a unit
		for the Login object that is returned by METADATA_GETNOBJ. The METADATA_GETATTR 
		function gets the values of the object's Id and UserId attributes. */
		
		  do while(objrc>0);
		     arc=metadata_getattr(uri,"Id",LoginObjId);
		     arc=metadata_getattr(uri,"UserId",UserId);
		  
		/* The METADATA_GETNASN function specifies to get objects associated 
		via the AssociatedIdentity association. The AssociatedIdentity association name 
		returns both Person and IdentityGroup objects, which are subtypes of the Identity
		metadata type. The URIs of the associated objects are returned in the uri2 variable. 
		If no associations are found, the program prints an informational message. */
		
		     n2=1;
		     asnrc=metadata_getnasn(uri,"AssociatedIdentity",n2,uri2);
		     if asnrc<=0 then put "NOTE: rc=" asnrc 
		       "There is no Person or Group associated with the " UserId "user ID.";
		
		/* When an association is found, the METADATA_RESOLVE function is called to 
		resolve the URI to an object on the metadata server. */
		
		     else do;
		       arc=metadata_resolve(uri2,IdentType,IdentId);
		
		     /* The METADATA_GETATTR function is used to get the values of each identity's 
		Name, DisplayName and Desc attributes. */
		
		       arc=metadata_getattr(uri2,"Name",Name);
		       arc=metadata_getattr(uri2,"DisplayName",DispName);
		       arc=metadata_getattr(uri2,"Desc",Desc);
		     end;
		  
		 /* The METADATA_GETNASN function specifies to get objects associated 
		via the Domain association. The URIs of the associated objects are returned in 
		the uri3 variable. If no associations are found, the program prints an 
		informational message. */ 
		  
		     n3=1;
		     autrc=metadata_getnasn(uri,"Domain",n3,uri3);
		     if autrc<=0 then put "NOTE: rc=" autrc 
		       "There is no Authentication Domain associated with the " UserId "user ID.";
		
		           /* The METADATA_GETATTR function is used to get the values of each 
		AuthenticationDomain object's Id and Name attributes. */
		
		     else do;
		       arc=metadata_getattr(uri3,"Id",AuthDomId);
		       arc=metadata_getattr(uri3,"Name",AuthDomName);
		     end;
		
		    output;
		
		  /* The CALL MISSING routine reinitializes the variables back to missing values. */
		
		  call missing(LoginObjId, UserId, IdentType, IdentId, Name, DispName, Desc, AuthDomId, 
		AuthDomName);
		
		n+1;
		  objrc=metadata_getnobj("omsobj:Login?@Id contains '.'",n,uri);
		  end;
		
		  keep  LoginObjId UserId IdentType Name DispName Desc AuthDomId AuthDomName; 
		run;

		proc sql;
		create table userAut as
		select t2.libname, t1.UserId, AuthDomName
		from	AXYPPD4_345 as t1 inner join sashelp.vlibnam as t2 on (t2.sysvalue=t1.userid)
		where t2.engine="ORACLE" and path="PRDDCDWH" /*the restriction on the path is not necessary*/
		order by t2.libname;
	    quit;
  %end;
  %else %put UserAut table already generated;
%MEND LOGINS;





/* 
###################### dataset_info ############################################################

assumptions:

			DCF.DATA
			DCF.DATACOLLECTION
			AUT_DWH.ETLJ_DOMAINS
		    AUT_DWH.ALL_TABLES_DWH_CONFIG

existing and  updated. 

input macro variable : DS_list -> list of dataset ids separated by blanks

This macro retrieve the tables in which the dataset belongs plus 
fields useful for parametric data manipulation, 

Description:

An output table (dataset_info) is created containing 
DATASET_ID
DATA_ID
DATACOLLECTION
DCF_TABLE
DCF_LIBREF
DCF_DATASET_IDENTIFIER
DCF_HAS_IS_VALID
DCF_HAS_AMEND
ODS_TABLE
ODS_LIBREF
ODS_DATASET_IDENTIFIER
ODS_HAS_IS_VALID
ODS_HAS_AMEND
DWH_TABLE
DWH_LIBREF
DWH_DATASET_IDENTIFIER
DWH_HAS_IS_VALID
DWH_HAS_AMEND
DWH_RECORD_IDENTIFIER
DWH_DC_IDENTIFIER
DWH_ORG_IDENTIFIER
DWH_FROM_IDENTIFIER
DWH_TO_IDENTIFIER
DWH_VALID_IDENTIFIER
DWH_SKEY_IDENTIFIER


########################################################################################################
*/
%MACRO dataset_info(ds=/*dataset list separated by spaces*/)
/ store source des="Valid for WF2, extract tables location + fields info of the dataset for ODS DCF and DWH";
/*extract tables location of the datset for ODS DCF and DWH*/
proc transpose data=aut_dwh.all_tables_DWH_config(where=(table_type="F" and 
              (etl_type1 in("IS_VALID",
                           "VALIDTO",
                           "VALIDFROM",
                           "ORGANIZATION_ID",
                           "DATACOLLECTION_ID",
                           "RECID",
                           "DATASET_ID",
                           "TRX_ID",
                           "SKEY")))) 
               out=field_config(keep=TABLE_NAME	
                                     TABLE_LIB	
                                     SKEY	
                                     DATASET_ID	
                                     RECID	
                                     DATACOLLECTION_ID	
                                     ORGANIZATION_ID
                                     VALIDFROM
                                     VALIDTO
                                     IS_VALID);
id  etl_type1;
var column_name;
by table_name table_lib;
quit;

proc sql noprint;
create table dataset_info as 
   select A.ID AS DATASET_ID,
          B.ID AS DATA_ID, 
          C.CODE AS DATACOLLECTION,
		  B.TABLENAME AS DCF_TABLE,
		  "DCFDATA" AS DCF_LIBREF,
          "DATASET_ID" as DCF_DATASET_IDENTIFIER,
		  "NO" as DCF_HAS_IS_VALID,
		  "YES" as DCF_HAS_AMEND,
		  D.ETLJ_ODS_TABLENAME AS ODS_TABLE,
		  D.ETLJ_ODS_LIBREF AS ODS_LIBREF,
		  "DATASET_ID" as ODS_DATASET_IDENTIFIER,
		  "YES" as ODS_HAS_IS_VALID,
		  "YES" as ODS_HAS_AMEND,
		  D.ETLJ_DWH_TABLENAME AS DWH_TABLE,
		  D.ETLJ_DWH_LIBREF AS DWH_LIBREF,
		  E.DATASET_ID as DWH_DATASET_IDENTIFIER,
		  "YES" as DWH_HAS_IS_VALID,
		  "NO" as DWH_HAS_AMEND,
		  E.RECID as DWH_RECORD_IDENTIFIER,
		  E.DATACOLLECTION_ID as DWH_DC_IDENTIFIER,
		  E.ORGANIZATION_ID as DWH_ORG_IDENTIFIER,
		  E.VALIDFROM  as DWH_FROM_IDENTIFIER,
		  E.VALIDTO as DWH_TO_IDENTIFIER,
		  E.IS_VALID as DWH_VALID_IDENTIFIER,
		  E.SKEY as DWH_SKEY_IDENTIFIER,
          a.status
   from  dcf.dataset a 
   left join dcf.data b
       on a.data_id = b.id 
   left join dcf.datacollection c
       on a.datacollection_id = c.id
   left join aut_dwh.etlj_domains(where=(is_current=1)) d
       on b.tablename=d.etlj_dcf_tablename and c.code=d.etlj_dc_code
   left join field_config e
       on  (e.table_lib=d.etlj_dwh_libref and e.table_name=d.etlj_dwh_tablename ) 
   where a.id in (&DS.)
   order by DCF_table,ODS_table,DWH_table;
quit;

%MEND dataset_info;
/* count records of a given list of datasets (max 400 ds) choosing among DCF ODS or DWH*/

/* 
###################### Count Records ############################################################

assumptions:

			macro DATASET_INFO defined

existing and  updated. 

input macro variable : DS_list

Description:

########################################################################################################
*/

%macro count_records(ds=/*DS list separated by blanks*/,stage=/*DCF,ODS or DWH*/)
/ store source des="Count the number of records of a list of dataset given the stage (DCF,ODS,DWH)";
%let stage=%sysfunc(upcase(&stage.));
/*extract tables location of the datset for ODS DCF and DWH*/
%dataset_info(ds=&ds.);

/*check that all fields are compiled;
 if there are missing values the macro will stop to execute*/
data _null_;
    RETAIN INVALID_TOT 0;
    set dataset_info end=last;
	invalid=0;
	array vars _CHARACTER_;
    do over vars;
	    if vars ="" then do;
		   field=vname(vars);
	       put "err: for dataset" dataset_id= "->" field= " is missing";
		   invalid=1;
		   invalid_tot= invalid_tot+1;
		end;
	end;
	if invalid then output;
    if last then do;
       call symput("semaphore",invalid_tot);
	end;
run;
%if &semaphore. ^= 0 %then %do;
 %put error! information missing in configuration table.;
 %return;
%end;

proc sort data=dataset_info;
by &Stage._libref &Stage._table;
run;

/* create a list with the tables info and the related datasets to check */

data aggregate_&Stage._ds;
   length DS_LIST $10000; /* This field contains the list of datasets for each table to be processed*/
   format ds_list $10000.;
   retain ds_list "";
   set dataset_info;
   by &Stage._libref &Stage._table;
   if first.&Stage._table then do;
       ds_list="";
   end; 
   ds_list= catx(" ",ds_list,put(dataset_id,10.));
   if last.&Stage._table then do; 
       output;
   end;
   keep DS_LIST &Stage._libref &Stage._table &Stage._dataset_identifier &Stage._HAS_IS_VALID &Stage._HAS_AMEND;
run;

/* COUNT records: dependence from STAGE macro variable*/

%MACRO count_rec(DS_LIST=, libref=, table=,id_identifier=,HAS_is_valid=,HAS_AMEND=);
    (select  a.&id_identifier. as dataset_id, 
	   %if &HAS_AMEND.= NO %then %do;
           count(*) as N_&Stage._records 
	   %end;
	   %else %do;
		   sum(case when upcase(a.amtype)  = "" then 1 else 0 end ) as N_&Stage._records,
		   %if &stage=DCF %then %do;
	            sum(case when upcase(a.amtype)  = "D" then 1 else 0 end ) as _delete,
           %end;
		   %else %if &stage=ODS %then %do;
                coalesce(b.delete,0) as _delete,
		   %end;
		   sum(case when upcase(a.amtype)  = "U" then 1 else 0 end ) as _update
	   %end;
   from &libref..&table. a
   %if &stage=ODS %then %do;
   left join 
        (select &id_identifier.,
                sum(case when upcase(amtype)  = "D" then 1 else 0 end ) as delete 
         from 
             &libref..&table._TOAMEND
		 group by &id_identifier.
         ) b
	on a.&id_identifier.=b.&id_identifier.
   %end;
	   where a.&id_identifier. in (&DS_LIST.)
		   %if &HAS_is_valid.=YES %then %do;
	        and a.is_valid=1
	       %end;
       group by a.&id_identifier.)
%MEND count_rec;

/* this step creates a summary table with dataset_id, number of records and ,if  stage=DCF , number of deletion */
data _null_;
    set aggregate_&Stage._ds end=last;
	if _N_=1 then do;
       call execute ('proc sql; Create table resume_&Stage. as ');
	end;
	if not last then do;
		call execute ('%count_rec(DS_LIST=%str('|| trim(DS_LIST) || '), libref=' 
                                  ||  compress(&Stage._libref) || ', table=' 
                                  ||  compress(&Stage._table) || ',id_identifier=' 
                                  ||  compress(&Stage._dataset_identifier) || ',HAS_is_valid='
                                  ||  compress(&Stage._HAS_is_valid) || ',HAS_AMEND='
                                  ||  compress(&Stage._HAS_AMEND) || ') union all' );
    end;
	else do;
		call execute ('%count_rec(DS_LIST=%str('|| trim(DS_LIST) || '), libref=' 
                                  ||  compress(&Stage._libref) || ', table=' 
                                  ||  compress(&Stage._table) || ',id_identifier=' 
                                  ||  compress(&Stage._dataset_identifier) || ',HAS_is_valid='
                                  ||  compress(&Stage._HAS_is_valid) || ',HAS_AMEND='
                                  ||  compress(&Stage._HAS_AMEND) || ') order by dataset_id; quit;' );
    end;
run;

%MEND count_records;

%*count_records(ds=11123,stage=ods);

/* select datasets having issues deleting them from ODS */

/* delete records of a given list of datasets (max 400 ds) choosing among DCF ODS or DWH*/

/* 
###################### delete_records_ODS ############################################################

assumptions:



existing and  updated. 

input macro variable : 


Description:


########################################################################################################
*/

%MACRO delete_records_ODS(ds= /*datasets list to be deleted separated by spaces*/,stage=ODS)
/ store source des="Removes a list of dataset from the associated ODS table";

/*extract tables location of the datset for ODS DCF and DWH*/
%dataset_info(ds=&ds.);

/*check that all fields are compiled;
 if there are missing values the macro will stop to execute*/
data _null_;
    RETAIN INVALID_TOT 0;
    set dataset_info end=last;
	invalid=0;
	array vars _CHARACTER_;
    do over vars;
	    if vars ="" then do;
		   field=vname(vars);
	       put "error: " field= " is missing";
		   invalid=1;
		   invalid_tot= invalid_tot+1;
		end;
	end;
    if last then do;
       call symput("semaphore",invalid_tot);
	end;
run;
%if &semaphore. ^= 0 %then %do;

 %put error! information missing in configuration table.;
 %return;

%end;

proc sort data=dataset_info;
by &Stage._libref &Stage._table;
run;

/* create a list with the tables info and the related datasets to check */

data aggregate_&Stage._ds;
   length DS_LIST $10000; /* This field contains the list of datasets for each table to be processed*/
   format ds_list $10000.;
   retain ds_list "";
   set dataset_info;
   by &Stage._libref &Stage._table;
   if first.&Stage._table then do;
       ds_list="";
   end; 
   ds_list= catx(" ",ds_list,put(dataset_id,10.));
   if last.&Stage._table then do; 
       output;
   end;
   keep DS_LIST &Stage._libref &Stage._table &Stage._dataset_identifier &Stage._HAS_IS_VALID &Stage._HAS_DEL;
run;

/* Delete records*/

%MACRO Delete_rec(DS_LIST=, libref=, table=,id_identifier=,HAS_is_valid=,HAS_DEL=);
    delete from &libref..&table.
	where &id_identifier. in (&DS_LIST.);

    delete from &libref..&table._TOAMEND
    where &id_identifier. in (&DS_LIST.);
%MEND Delete_rec;

/* this step creates a summary table with dataset_id, number of records and ,if  stage=DCF , number of deletion */

data _null_;
    set aggregate_&Stage._ds end=last;
	if _N_=1 then do;
       call execute ('proc sql;');
	end;
	if not last then do;
		call execute ('%Delete_rec(DS_LIST=%str('|| trim(DS_LIST) || '), libref=' 
                                  ||  compress(&Stage._libref) ||', table=' 
                                  ||  compress(&Stage._table) || ',id_identifier=' 
                                  ||  compress(&Stage._dataset_identifier) || ',HAS_is_valid='
                                  ||  compress(&Stage._HAS_is_valid) || ',HAS_DEL='
                                  ||  compress(&Stage._HAS_DEL) || ') ' );
    end;
	else do;
		call execute ('%Delete_rec(DS_LIST=%str('|| trim(DS_LIST) || '), libref=' 
                                  ||  compress(&Stage._libref) || ', table=' 
                                  ||  compress(&Stage._table) || ',id_identifier=' 
                                  ||  compress(&Stage._dataset_identifier) || ',HAS_is_valid='
                                  ||  compress(&Stage._HAS_is_valid) || ',HAS_DEL='
                                  ||  compress(&Stage._HAS_DEL) || ') quit;' );
    end;
run;

%mend delete_records_ODS;
%*delete_records_ODS(ds=3310);

/* 
###################### delete_records_DWH ############################################################

assumptions:



existing and  updated. 

input macro variable : 


Description:

########################################################################################################
*/
%macro delete_records_DWH(ds= /*datasets list to be deleted separated by spaces*/,Stage=DWH) /
store source des="Removes a list of dataset from the associated DWH table";

%dataset_info(ds=&ds.);

/*check that all fields in dataset_info are compiled;
 if there are missing values the macro will stop to execute*/

data _null_;
    RETAIN INVALID_TOT 0;
    set dataset_info end=last;
	invalid=0;
	array vars _CHARACTER_;
    do over vars;
	    if vars ="" then do;
		   field=vname(vars);
	       put "error: " field= " is missing";
		   invalid=1;
		   invalid_tot= invalid_tot+1;
		end;
	end;
    if last then do;
       call symput("semaphore",invalid_tot);
	end;
run;
%if &semaphore. ^= 0 %then %do;

 %put error! information missing in configuration table.;
 %return;

%end;

proc sort data=dataset_info;
by DWH_libref DWH_table ODS_libref ODS_table;
run;

/* create a list with the tables info and the related datasets to check */

data aggregate_&Stage._ds;
   length DS_LIST $10000; /* This field contains the list of datasets for each table to be processed*/
   format ds_list $10000.;
   retain ds_list "";
   set dataset_info;
   by DWH_libref DWH_table ODS_libref ODS_table;
   if first.DWH_table and first.ODS_table then do;
       ds_list="";
   end; 
   ds_list= catx(",",ds_list,put(dataset_id,10.));
   if last.DWH_table and last.ODS_table then do; 
       output;
   end;
   keep DS_LIST
        DWH_libref
DWH_table
ODS_libref 
ODS_table 
DWH_dataset_identifier
DWH_RECORD_IDENTIFIER
DWH_ORG_IDENTIFIER
DWH_DC_IDENTIFIER
DWH_FROM_IDENTIFIER
DWH_TO_IDENTIFIER
DWH_VALID_IDENTIFIER
DWH_SKEY_IDENTIFIER
;
run;


/*################################################################################################*/

%MACRO delete_rec   (	DS_LIST=,
						ODS_libref=, 
						ODS_table=,
						DWH_libref=,
						DWH_table= ,
						ODS_id_identifier=,
						DWH_id_identifier=,
						DWH_record_identifier=,
						DWH_ORG_IDENTIFIER=,
						DWH_DC_IDENTIFIER=,
						DWH_FROM_IDENTIFIER=,
						DWH_TO_IDENTIFIER=,
						DWH_VALID_IDENTIFIER=,
						DWH_SKEY_IDENTIFIER=
);
/*********** prepare amendments table ***********/
%put login;
%logins;
data _null_;
    set userAut;
	where libname ="&DWH_libref.";
	call symput('auth_dom',trim(AuthDomName));
run;
%put end login, libname &dwh_libref. auth = &auth_dom.;
%put amendments;


/* extract amendments from _toamend ods table */
data amendments;
	length &DWH_id_identifier. 8 
           &DWH_ORG_IDENTIFIER. 8 
           &DWH_DC_IDENTIFIER. 8 ;
	set &ODS_libref..&ODS_table._TOAMEND;
	where dataset_id in (&DS_LIST.);
    &DWH_id_identifier. = dataset_id;
    &DWH_ORG_IDENTIFIER.= organization_id;
    &DWH_DC_IDENTIFIER.=  datacollection_id;    	
	keep  &DWH_id_identifier. 
          &DWH_record_identifier. 
          &DWH_ORG_IDENTIFIER.
          &DWH_DC_IDENTIFIER.;
run;

proc sql noprint;
	select coalesce(count(1),0) into :count_amend
	from amendments;
quit;

proc datasets lib= &dwh_libref. nolist;
delete tempETL_amend_tab_rej dcf_accepted_dwh;
quit;
data &DWH_libref..tempETL_amend_tab_rej;
	length &DWH_id_identifier. 8 
           &DWH_ORG_IDENTIFIER. 8 
           &DWH_DC_IDENTIFIER. 8 ;  
	format &DWH_id_identifier. 10. 
           &DWH_ORG_IDENTIFIER. 4.
           &DWH_DC_IDENTIFIER. 4. ; 
	set amendments;
run;
/*select datasets in status ACCEPTED_DWH */
proc sql noprint;
      create table &DWH_libref..dcf_accepted_dwh as
      select distinct id as DATASET_ID
      from  dcf.dataset 
      where status in ("ACCEPTED_DWH") ;
 quit;

options mprint;
%put delete from &DWH_table.;
/*data deleted from DWH table*/
/*1)revalidate pre amendment data*/
/*2)delete new records and updated records */
/*3)drop temporary tables */
	 	proc sql; 
	       connect to ORACLE
	       ( 
	           PATH=PRDDCDWH AUTHDOMAIN="&auth_dom." 
	       ); 
	       reset noprint; 

	       execute 
	       ( 
				update &DWH_table.
				set &DWH_VALID_IDENTIFIER.=1, &DWH_TO_IDENTIFIER.=TO_DATE('5999/01/01 00:00:00', 'YYYY/MM/DD HH24:MI:SS')
				where &DWH_SKEY_IDENTIFIER.  in (
						select max(&DWH_SKEY_IDENTIFIER.) 
						from &DWH_table.
						where CONCAT(
						       CONCAT(
							    CONCAT(&DWH_record_identifier.,'-'),
						        CONCAT(trim(TO_CHAR(&DWH_ORG_IDENTIFIER.,'9999')),'-')
							   ),
						       trim(TO_CHAR(&DWH_DC_IDENTIFIER.,'9999'))
							  ) in (
							    select
									  CONCAT(
									   CONCAT(
										CONCAT(&DWH_record_identifier.,'-'),
										CONCAT(trim(TO_CHAR(&DWH_ORG_IDENTIFIER.,'9999')),'-')
									   ),
									   trim(TO_CHAR(&DWH_DC_IDENTIFIER.,'9999'))
									  ) 
									  FROM tempETL_amend_tab_rej
							  )
						and &DWH_id_identifier. not in (&DS_LIST.) 
                        and &DWH_id_identifier. in (select dataset_id from dcf_accepted_dwh)
						group by 
						      CONCAT(
						       CONCAT(
							    CONCAT(&DWH_record_identifier.,'-'),
						        CONCAT(trim(TO_CHAR(&DWH_ORG_IDENTIFIER.,'9999')),'-')
							   ),
						       trim(TO_CHAR(&DWH_DC_IDENTIFIER.,'9999'))
							  )
						)
				and &DWH_VALID_IDENTIFIER.=0 
	       ) by ORACLE;

	       execute 
	       ( 
				delete from &DWH_table.
				where &DWH_id_identifier. in (&DS_LIST.)
						and &DWH_VALID_IDENTIFIER.=1
	       ) by ORACLE;      

	       execute 
	       ( 
				drop table tempETL_amend_tab_rej
	       ) by ORACLE; 
	       execute 
	       ( 
				drop table dcf_accepted_dwh
	       ) by ORACLE; 

	       disconnect from ORACLE; 
	    quit; 

%MEND delete_rec;	

/*#######################################################################################*/

data _null_;
    set aggregate_&Stage._ds end=last;
		call execute (
'%nrstr(%delete_rec(DS_LIST=%str('||compress(DS_LIST) ||'), 
ODS_libref='||compress(ODS_libref) ||', 
ODS_table='||compress(ODS_table) ||',
DWH_libref='||compress(DWH_libref) ||', 
DWH_table='||compress(DWH_table) ||',
ODS_id_identifier='||compress(ODS_DATASET_IDENTIFIER) ||',
DWH_id_identifier='||compress(DWH_DATASET_IDENTIFIER) ||',
DWH_record_identifier='||compress(DWH_RECORD_IDENTIFIER) ||',
DWH_ORG_IDENTIFIER='||compress(DWH_ORG_IDENTIFIER) ||',
DWH_DC_IDENTIFIER='||compress(DWH_DC_IDENTIFIER) ||',
DWH_FROM_IDENTIFIER='||compress(DWH_FROM_IDENTIFIER) ||',
DWH_TO_IDENTIFIER='||compress(DWH_TO_IDENTIFIER) ||',
DWH_VALID_IDENTIFIER='||compress(DWH_VALID_IDENTIFIER) ||',
DWH_SKEY_IDENTIFIER='||compress(DWH_SKEY_IDENTIFIER) ||'));'
);
run;

%MEND delete_records_DWH;



