-------------------------------------------------------------------------------------------------------------------------
--                                      Run script in OLAP database 
-- Script creates a function olapts.panegf_report which takes a date as parameter and populate olapts.panegf_report table
-------------------------------------------------------------------------------------------------------------------------

-- FUNCTION: olapts.panegf_report(date)

-- DROP FUNCTION olapts.panegf_report(date);

CREATE OR REPLACE FUNCTION olapts.panegf_report(
	ref_date date)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
 pl_count integer :=0;
 pl_sourcecount integer :=0;
 pl_targetcount integer :=0;
 pl_status boolean:=FALSE;
 pl_function varchar;
 pl_targettablename varchar;
 pl_jobid varchar;
 pl_sourcetablename varchar;
 pl_message varchar:='';
 stack text;
 pl_maxdate varchar;
 pl_maxsourcedate timestamp;
 pl_maxtargetdate timestamp;
 v_sql varchar;
 pl_schema varchar:='olapts'; 
begin
	
    pl_jobid:='populate_report'||TO_CHAR(now(), 'yyyymmddHH24MI')::varchar;
	
	GET DIAGNOSTICS stack = PG_CONTEXT;
	pl_function:= substring(stack from 'function (.*?) line');
	pl_function:= substring(pl_function,1,length(pl_function)-2);
	-- RAISE Notice 'pl_function (%)',pl_function;
	
	pl_targettablename:='panegf_report';
	
	truncate TABLE olapts.panegf_report; 
	
-----------------------------------------------------------------------------------------------
--                               PANEGF Report Code/ Sign Off 11.01.2024
-----------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------
--
--              Find all ratings in the reference date and flag the retrieved data as Ralph or Legacy
--
--------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
--                                     Ralph 
----------------------------------------------------------------------------------------

-----------------------------------
--Find only the approved ratings
-----------------------------------

drop table if exists approvals_ralph;
create temporary table approvals_ralph as
select  a.pkid_,
		a.FinancialContext  as FinancialContext,
        a.EntityId  as Entityid ,
        a.ApprovedDate  as ApprovedDate,  
		a.approveid, 
        a.sourcepopulateddate_ as sourcepopulateddate_rating,
		a.modelid as modelid,
		d.ratingscalevalue,
		a.nextreviewdate as nextreviewdate,
        case when d.ratingscalevalue is not null then 'true' else 'false' end as overrideflag,
        a.creditcommitteedate as creditcommitteedate,
		a.islatestapprovedscenario,a.approvalstatus,
		entity_version_match AS entityVersion,
		a.versionid_,
		'Ralph' flag
from olapts.abRatingScenario a 
left join olapts.abentityrating f on a.approveid=f.approveid
left join olapts.dimratingscale d on f.FinalGrade=d.ratingscalekey_
where  1=1 and cast(a.ApprovedDate as date) >='2021-01-06'
and a.ApprovedDate <= cast(ref_date as date) + time '23:59:59'
and a.isdeleted_ = 'false' 
and a.islatestapprovedscenario::boolean 
and a.isprimary::boolean 
and a.approvalstatus = '2'                         
and a.ApprovedDate is not null 
order by a.EntityId,a.ApprovedDate desc;

-------------------------------------------------------------------------------------------------------------------
-- Find the entity information for this version where the ratings were approved and keep only the latest afm, cdi 
-------------------------------------------------------------------------------------------------------------------

-------------------------------------
-- Ratings with financials
-------------------------------------

drop table if exists approvals_ralph_entity_with_financials;
create temporary table approvals_ralph_entity_with_financials as
select distinct on (a.entityid) --keep only the latest afm, cdi per entityid 
        a.* ,
		gc18 as afm,
		cdicode,
		'with_financials_ralph' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from approvals_ralph a
left join olapts.abfactentity b on a.entityid=b.entityid and a.entityVersion::int = b.versionid_
	 and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'	 
where 1=1 
	  and a.modelid in ('FA_FIN','PdModelCcategory')
	  and b.sourcepopulateddate_ is not null
order by a.entityid,sourcepopulateddate_entity desc  ; 
	  	  
-------------------------------------
-- Ratings with non financials
-------------------------------------

drop table if exists approvals_ralph_entity_with_non_financials;
create temporary table approvals_ralph_entity_with_non_financials as
select distinct on (pkid_, entityid) *
from (
select  distinct on (pkid_, a.entityid, gc18, cdicode) 
        a.* ,
		gc18 as afm,
		cdicode,
		'non_financials_ralph' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from approvals_ralph a	 
join olapts.abfactentity b on b.entityid::int  = a.entityid::int
     and b.sourcepopulateddate_ < a.sourcepopulateddate_rating	
	 and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'	 
where 1=1 
      and a.modelid not in ('FA_FIN','PdModelCcategory')
order by pkid_, a.entityid, gc18, cdicode, b.sourcepopulateddate_ desc 	 
)x
order by pkid_, entityid, sourcepopulateddate_entity desc;

--------------------------------------------
-- Union Financials / Non Financials
--------------------------------------------

drop table if exists approvals_ralph;
create temporary table approvals_ralph as 
select distinct * from (
select * from approvals_ralph_entity_with_financials
union all
select * from approvals_ralph_entity_with_non_financials
)x;

----------------------------------------------------------------------------------------
--                                     Legacy 
----------------------------------------------------------------------------------------

----------------------------------------------
-- Find all ratings / Approved & Non approved
----------------------------------------------

drop table if exists ratings_legacy;
create temporary table ratings_legacy as
select distinct  a.pkid_,
           a.FinancialContext  as FinancialContext,
           a.EntityId  as Entityid ,
           a.ApprovedDate  as ApprovedDate,  
	       a.approveid,
           a.sourcepopulateddate_ as sourcepopulateddate_rating,
		   a.modelid as modelid,
		   b.ratingscalevalue,
		   a.nextreviewdate as nextreviewdate,
           case when b.ratingscalevalue is not null then 'true' else 'false' end as overrideflag,
           a.creditcommitteedate as creditcommitteedate,
		   a.islatestapprovedscenario,
		   a.approvalstatus,
		   cast(SUBSTRING((REGEXP_MATCHES(FinancialContext,';([^;#]*)#'))[1], 1) as int) AS entityVersion,
		   a.versionid_,
		   'Legacy' flag
from olapts.factratingscenario a 
left join olapts.factentityrating f on a.approveid=f.approveid
left join olapts.dimratingscale b on f.FinalGrade=b.ratingscalekey_
where a.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'	 
and cast(a.sourcepopulateddate_ as date) >='2021-01-06'
and a.isdeleted_ = 'false' 
and a.isprimary::boolean 
;

-------------------------------------------------------------------------------------------------------------------
-- Find the entity information for this version where the ratings were approved and keep only the latest afm, cdi 
-------------------------------------------------------------------------------------------------------------------

-------------------------------------
-- Ratings with financials
-------------------------------------

drop table if exists approvals_legacy_entity_with_financials;
create temporary table approvals_legacy_entity_with_financials  as
select distinct on (a.entityid) 
        a.* ,
		gc18 as afm,
		cdicode,
		'with_financials_legacy' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from ratings_legacy a
left join olapts.factentity b on b.entityid  = a.entityid::int
     and b.versionid_ = a.entityversion 
	 and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'
	 and a.modelid in ('FA_FIN','PdModelCcategory')
where 1=1 
      and b.sourcepopulateddate_ is not null
order by a.entityid, sourcepopulateddate_entity desc 
	 ;
	 
-------------------------------------
-- Ratings with non financials
-------------------------------------	 

drop table if exists approvals_legacy_entity_with_non_financials;
create temporary table approvals_legacy_entity_with_non_financials as
select distinct on (pkid_, entityid) *
from (
select distinct on (pkid_, a.entityid, gc18, cdicode) 
        a.* ,
		gc18 as afm,
		cdicode,
		'non_financials_legacy' flag_entity, b.sourcepopulateddate_ sourcepopulateddate_entity
from ratings_legacy a	 
join olapts.factentity b on b.entityid  = a.entityid::int
     and b.sourcepopulateddate_ < a.sourcepopulateddate_rating 
     and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'		
where  a.modelid not in ('FA_FIN','PdModelCcategory')
order by pkid_, a.entityid, gc18, cdicode, b.sourcepopulateddate_ desc	
)x
order by pkid_, entityid, sourcepopulateddate_entity desc;

--------------------------------------------
-- Union Financials / Non Financials
--------------------------------------------

drop table if exists ratings_legacy;
create temporary table ratings_legacy as 
select distinct * from (
select * from approvals_legacy_entity_with_financials
union all
select * from approvals_legacy_entity_with_non_financials
)x;

--------------------------------------------------------------------------------------------------------------
--
--                                Data Consolidation / Legacy & Ralph
--
--------------------------------------------------------------------------------------------------------------

drop table if exists ratings_union;
create temporary table ratings_union as
select distinct *
from (
select pkid_,financialcontext,entityid::int,approveddate,approveid, sourcepopulateddate_rating,modelid,ratingscalevalue,nextreviewdate,overrideflag,creditcommitteedate,
       islatestapprovedscenario,approvalstatus,entityversion::int,flag,afm,cdicode,flag_entity,versionid_
from approvals_ralph
union all
select pkid_,financialcontext,entityid,approveddate,approveid, sourcepopulateddate_rating,modelid,ratingscalevalue,nextreviewdate,overrideflag,creditcommitteedate,
       islatestapprovedscenario,approvalstatus,entityversion,flag,afm,cdicode,flag_entity,versionid_
from ratings_legacy
)x;

--------------------------------------------------------------------------------------------------------------
--
--                            Choose Final Rating/ Ranking per entityid
--
-- When a rating approved in Legacy take the information from legacy tables. 
-- When a rating began in Legacy and approved in Ralph take the information from Ralph tables.
-- When a rating began in Ralph and approved in Ralph take the information from Ralph tables. 
--------------------------------------------------------------------------------------------------------------

drop table if exists final_rating; 
create temporary table final_rating as 
select * from (
select row_number() over (partition by entityid order by entityid, sourcepopulateddate_rating desc, flag asc) rn, *
from ratings_union 
where approveddate is not null and islatestapprovedscenario 
) x
where rn = 1;

----------------------------------------------
--          DataWithFinancials
----------------------------------------------
-------------------------------------------------------------------------
--find entityversion, financialid, statementid based on FinancialContext
-------------------------------------------------------------------------

drop table if exists perimeter_financials;
create temporary table perimeter_financials as
select *,
       --cast(SUBSTRING((REGEXP_MATCHES(FinancialContext,';([^;#]*)#'))[1], 1) as int) AS entityVersion,
       cast((REGEXP_MATCHES(FinancialContext, '^[^:]*'))[1] as int) AS FinancialId
from (select  * 
	  ,  (REGEXP_MATCHES(unnest(STRING_TO_ARRAY(REGEXP_REPLACE(FinancialContext, '.*#([^:*]+)', '\1'), ';')), '^(\d+)'))[1] AS statementid
      from final_rating
	  where modelid in ('FA_FIN','PdModelCcategory')
	  order by FinancialContext
	 )x;
	 
----------------------------------------------
--          DataWithoutFinancials
----------------------------------------------

drop table if exists perimeter_non_financials;
create temporary table perimeter_non_financials as
select * 
from final_rating
where modelid not in ('FA_FIN','PdModelCcategory');
	 
----------------------------------------------------------------------------------------------------------------------------------------------------------
--
--                                              Final Table -  with financials
--
-- Choose max sourcepopulateddate of financials where sourcepopulateddate_financials <= sourcepopulateddate_rating (mh eteroxronismenh)
-- If no data exists, search for min sourcepopulateddate of financials where sourcepopulateddate_financials>sourcepopulateddate_rating (eteroxronismenh)
--
----------------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------
--         Legacy
---------------------------

drop table if exists final_table_legacy;
create temporary table final_table_legacy as
select distinct on (entityid,cdi, afm, fiscalyear) * 
from (
	select *,
	case when flag='mh eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial desc,ApprovedDate desc ) 
	     when flag='eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial asc,ApprovedDate desc ) 
	end as rn
	from (		
select 
	   c.pkid_,
       per.cdicode as  cdi,
       afm,
       per.ratingscalevalue as grade,
       cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate,
       to_char(cast(cast(per.nextreviewdate as varchar(15)) as date),'dd-MM-yyyy') as nextreviewdate,
       per.modelid as ratingmodelname,
       c.statementyear::text as fiscalyear,
       per.statementid, 
       d.salesrevenues::text as salesrevenues,
       d.totalassets::text   as totalassets,
       per.entityid::text as entityid,
        case when f.FinalGrade is not null then 'true' else 'false' end as overrideflag, --new add
        to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate      --new add
       ,per.entityversion 
       ,per.versionid_
       ,c.versionid_ versionid_financial
       ,c.sourcepopulateddate_ sourcepopulateddate_financial
       ,per.sourcepopulateddate_rating sourcepopulateddate_rating
       ,per.flag flag_source_table
	   ,case when c.sourcepopulateddate_ <= per.sourcepopulateddate_rating then 'mh eteroxronismenh'
	          else 'eteroxronismenh' 
	    end as flag		
from olapts.factuphiststmtfinancial c
inner join perimeter_financials per on c.entityid::int = per.entityid 
      and c.financialid = per.financialid 
	  and c.statementid=per.statementid::int
join olapts.factuphiststmtfinancialgift d on c.pkid_ = d.pkid_ and c.versionid_ = d.versionid_ 
left join olapts.factentityrating f on per.approveid=f.approveid 
where 1=1 
and  per.islatestapprovedscenario 
and per.approvalstatus = '2'                         
and per.ApprovedDate is not null  
and per.flag = 'Legacy'
and per.flag_entity = 'with_financials_legacy'
order by c.pkid_, c.sourcepopulateddate_ desc,per.ApprovedDate desc
)x1
	)y where rn=1
order by entityid,cdi, afm, fiscalyear asc,sourcepopulateddate_financial desc, 
statementid desc, versionid_financial desc  --choose max statementid if 2 same years exist
;

---------------------------
--         Ralph
---------------------------

drop table if exists final_table_ralph;
create temporary table final_table_ralph as
select distinct on (entityid,cdi, afm, fiscalyear) * 
from (
	select *,
	      case when flag='mh eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial desc,ApprovedDate desc ) 
	          when flag='eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial asc,ApprovedDate desc ) 
	      end as rn
	from (	
select    c.pkid_,
            per.cdicode as cdi,
            afm,
            per.ratingscalevalue as grade,
            cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate,
            to_char(cast(cast(per.nextreviewdate as varchar(15)) as date),'dd-MM-yyyy') as nextreviewdate,
            per.modelid as ratingmodelname,
            c.statementyear::text as fiscalyear,
            per.statementid, 
            c.salesrevenues::text as salesrevenues,
            c.totalassets::text as totalassets,
            per.entityid::text as entityid,
            case when f.FinalGrade is not null then 'true' else 'false' end as overrideflag, --new add
			to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate               --new add
            ,per.entityversion, 
            per.versionid_
           ,c.versionid_ versionid_financial
           ,c.sourcepopulateddate_ sourcepopulateddate_financial
           ,per.sourcepopulateddate_rating sourcepopulateddate_rating 
            ,per.flag flag_source_table
	       ,case when c.sourcepopulateddate_ <= per.sourcepopulateddate_rating then 'mh eteroxronismenh'
	              else 'eteroxronismenh' 
	        end as flag		
from olapts.abuphiststmtfinancials c
inner join perimeter_financials per on c.entityid::int = per.entityid 
      and c.financialid::int = per.financialid 
	  and c.statementid::int=per.statementid::int
left join olapts.abentityrating f on per.approveid=f.approveid --new add
where  1=1 
       and per.islatestapprovedscenario::boolean 
       and per.approvalstatus = '2'                         
       and per.ApprovedDate is not null  
      and per.flag = 'Ralph'
 and per.flag_entity = 'with_financials_ralph'
order by c.pkid_, c.sourcepopulateddate_ desc, per.ApprovedDate desc
)x1
	)y where rn=1
order by entityid,cdi, afm, fiscalyear asc,   sourcepopulateddate_financial desc, 
statementid desc , versionid_financial desc 
;

----------------------------------------------------------------------
--                   UNION final tables / Legacy & Ralph
----------------------------------------------------------------------
			  
drop table if exists final_table1;
create temporary table final_table1 as
select distinct on (entityid,cdi, afm, fiscalyear) * 
from (
      select cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate
      from final_table_legacy 
      union all
      select cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate	   
      from final_table_ralph  
) x
order by entityid,cdi, afm, fiscalyear asc;

---------------------------------------------------------------------------------
--                       Final Table - union
---------------------------------------------------------------------------------

insert into olapts.panegf_report (
select distinct on (entityid,cdi,case when afm ~ '^[0]+$' then lpad(afm,8,'0') else afm end) --manipulation of same cdis with afms zeros
	   cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate
from final_table1 
order by entityid,cdi,case when afm ~ '^[0]+$' then lpad(afm,8,'0') else afm end, fiscalyear desc		
);

	
GET DIAGNOSTICS pl_targetcount := ROW_COUNT;
--RAISE Notice 'Number of rows upserted: %',pl_targetcount;	

--RAISE NOTICE 'Success in function %',pl_function;
pl_message:=pl_schema||'.'||pl_targettablename||' has been populated ';

--RAISE NOTICE 'Success in function %',pl_function;
pl_message:=pl_schema||'.'||pl_targettablename||' has been populated ';
perform  olapts.save_olapetllog(jsonb_build_object('id_',(SELECT nextval(pl_schema||'.olapetllog_sequence'))
		,'t_',pl_schema||'.olapetllog','JobId',pl_jobid,'ETLFunction',pl_function
		,'SourceTableName',pl_sourcetablename,'SourceTableCount',pl_sourcecount
		,'TargetTableName',pl_targettablename,'TargetTableCount',pl_targetcount
		,'LogMessage',pl_message,'SchemaName',pl_schema
		));

pl_status:=TRUE;
RETURN pl_status;	

EXCEPTION 
	WHEN OTHERS THEN
		pl_message:='Failure in function: '||pl_function||':'||SQLSTATE||'-'||SQLERRM;
		perform  olapts.save_olapetllog(jsonb_build_object('id_',(SELECT nextval(pl_schema||'.olapetllog_sequence'))
		,'t_',pl_schema||'.olapetllog','JobId',pl_jobid,'ETLFunction',pl_function
		,'SourceTableName',pl_sourcetablename,'SourceTableCount',pl_sourcecount
		,'TargetTableName',pl_targettablename,'TargetTableCount',pl_targetcount
		,'LogMessage',pl_message,'SchemaName',pl_schema
		));
		RAISE Notice '%',pl_message;
		Return pl_status;
	
end;
$BODY$;

ALTER FUNCTION olapts.panegf_report(date) OWNER TO olap;

-------------------------------------------------------------
--Check if the function was created and table was populated
-------------------------------------------------------------

--select * from olapts.panegf_report('2023-09-29')	
--select * from olapts.panegf_report
 
