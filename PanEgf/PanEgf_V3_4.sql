---------------------------------------------PanEgf---------------------------------------------

--Εύρεση approvals (non migrated) από τις νεες δομες του workaround για την ημερομηνια αναφορας και σηματοδοτηση ότι είναι από το την νεα reporting
--Εύρεση entity information από τις νεες δομες του workaround και προσοχή στις περιπτώσεις όπου δεν υπάρχει financial context,καθώς πρέπει 
--να βρεθεί η αντίστοιχη έκδοση του entity από audit trail scanning για την ημερομηνια αναφορας και αντιστοιχη σηματοδοτηση

drop table if exists approvals_ralph1;
create temporary table approvals_ralph1 as
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
--a.ApprovedDate <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 
and a.ApprovedDate <= '2023-09-29 23:59:59' 
and a.isdeleted_ = 'false' 
and a.islatestapprovedscenario::boolean 
and a.isprimary::boolean 
and a.approvalstatus = '2'                         
and a.ApprovedDate is not null 
order by a.EntityId,a.ApprovedDate desc;

--select * from approvals_ralph where entityid = '126764' --5.696

--------------------------------------------------

drop table if exists approvals_ralph_entity_with_financials;
create temporary table approvals_ralph_entity_with_financials as
select distinct on (a.entityid) --added 04/01/2024 8eloume mono ena zeugos afm, cdi to pio prosfato
        a.* ,
		gc18 as afm,
		cdicode,
		'with_financials_ralph' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from approvals_ralph1 a
left join olapts.abfactentity b on a.entityid=b.entityid and a.entityVersion::int = b.versionid_
     and b.sourcepopulateddate_ <= '2023-09-29 23:59:59'
	 --and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 	 
where 1=1 
      --and a.entityid = '92419'	 
	  and a.modelid = 'FA_FIN'
	  and b.sourcepopulateddate_ is not null
order by a.entityid,sourcepopulateddate_entity desc  ; --added 04/01/2024
	  	  
--select * from approvals_ralph_entity_with_financials --1.265

--------------------------------------------------

drop table if exists approvals_ralph_entity_with_non_financials;
create temporary table approvals_ralph_entity_with_non_financials as
select distinct on (pkid_, entityid) *
from (
select  distinct on (pkid_, a.entityid, gc18, cdicode) 
        a.* ,
		gc18 as afm,
		cdicode,
		'non_financials_ralph' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from approvals_ralph1 a	 
join olapts.factentity b on b.entityid  = a.entityid::int
     and b.sourcepopulateddate_ < a.sourcepopulateddate_rating	
     and b.sourcepopulateddate_ <= '2023-09-29 23:59:59'
	 --and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 	 
where 1=1 
     -- and a.entityid = '126764' 
      and a.modelid != 'FA_FIN'
order by pkid_, a.entityid, gc18, cdicode, b.sourcepopulateddate_ desc 	 
)x
order by pkid_, entityid, sourcepopulateddate_entity desc;

--select * from approvals_ralph_entity_with_non_financials where entityid = '126764' --1.317

-----------------------------------------------

drop table if exists approvals_ralph;
create temporary table approvals_ralph as 
select distinct * from (
select * from approvals_ralph_entity_with_financials
union all
select * from approvals_ralph_entity_with_non_financials
)x;

--select * from approvals_ralph where entityid = '89865'--2.582

-----------------------------------------------

--Εύρεση approvals (non migrated) από τις legacy δομες για την ημερομηνια αναφοράς και αντιστοιχη σηματοδοτηση ότι είναι από την legacy
--Εύρεση entity information από τις legacy δομες και προσοχή στις περιπτώσεις όπου δεν υπάρχει financial context,καθώς πρέπει 
--να βρεθεί η αντίστοιχη έκδοση του entity από audit trail scanning για την ημερομηνια αναφορας και αντιστοιχη σηματοδοτηση

drop table if exists ratings_legacy1;
create temporary table ratings_legacy1 as
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
--where coalesce(a.ApprovedDate,'1900-01-01') <= '2023-09-29 23:59:59'
where a.sourcepopulateddate_ <= '2023-09-29 23:59:59'
      --a.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 	 
and cast(a.sourcepopulateddate_ as date) >='2021-01-06'
and a.isdeleted_ = 'false' 
and a.isprimary::boolean 
--and  a.EntityId in ('120414') 
;

--select * from ratings_legacy1 where entityid = '89865'--89.616

--------------------------------------------------

drop table if exists approvals_legacy_entity_with_financials;
create temporary table approvals_legacy_entity_with_financials  as
select distinct on (a.entityid) --added 04/01/2024 8eloume mono ena zeugos afm, cdi to pio prosfato
        a.* ,
		gc18 as afm,
		cdicode,
		'with_financials_legacy' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from ratings_legacy1 a
left join olapts.factentity b on b.entityid  = a.entityid::int
     and b.versionid_ = a.entityversion 
	 and b.sourcepopulateddate_ <= '2023-09-29 23:59:59'  
	 --and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 	
	 and a.modelid = 'FA_FIN' 
where 1=1 
      --and a.entityid = '31075' 
      and b.sourcepopulateddate_ is not null
order by a.entityid, sourcepopulateddate_entity desc --added 04/01/2024 8eloume mono ena zeugos afm, cdi to pio prosfato
	 ;
	 
--select * from approvals_legacy_entity_with_financials where cdicode in ('4836644','12403191','15617860') --1.420

--------------------------------------------------	 

drop table if exists approvals_legacy_entity_with_non_financials;
create temporary table approvals_legacy_entity_with_non_financials as
select distinct on (pkid_, entityid) *
from (
select distinct on (pkid_, a.entityid, gc18, cdicode) 
        a.* ,
		gc18 as afm,
		cdicode,
		'non_financials_legacy' flag_entity, b.sourcepopulateddate_ sourcepopulateddate_entity
from ratings_legacy1 a	 
join olapts.factentity b on b.entityid  = a.entityid::int
     and b.sourcepopulateddate_ < a.sourcepopulateddate_rating 
     and b.sourcepopulateddate_ <= '2023-09-29 23:59:59'	
	 --and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 		
where  a.modelid != 'FA_FIN'	
	--and b.entityid = '120414'
order by pkid_, a.entityid, gc18, cdicode, b.sourcepopulateddate_ desc	
)x
order by pkid_, entityid, sourcepopulateddate_entity desc;

--select * from approvals_legacy_entity_with_non_financials where entityid = '126764' --349

-----------------------------------------------

drop table if exists ratings_legacy;
create temporary table ratings_legacy as 
select distinct * from (
select * from approvals_legacy_entity_with_financials
union all
select * from approvals_legacy_entity_with_non_financials
)x;

--select * from ratings_legacy where entityid ='89865' --1.769

-----------------------------------------------

--Consolidation των δεδομενων από τις legacy και non legacy δομες

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

--select * from ratings_union where entityid = '120414' --4.351

-----------------------------------------------

--Ranking σε επιπεδο entityid,afm,cdicode με προτεραιοποίηση της legacy εναντι της νεας

--dikia mas ulopoihsh
drop table if exists final_rating; 
create temporary table final_rating as 
select * from (
select row_number() over (partition by entityid order by entityid, sourcepopulateddate_rating desc, flag asc) rn, *
from ratings_union 
where approveddate is not null and islatestapprovedscenario --and entityid = '112233'
) x
where rn = 1;

--select * from final_rating --1.858

--karakoulis prodiagrafes
--drop table if exists final_rating; 
--create temporary table final_rating as 
--select *
--from (
--select row_number() over (partition by entityid,pkid_, versionid_ order by entityid, sourcepopulateddate_rating desc, flag asc) rn, *
--from ratings_union 
--where approveddate is not null and islatestapprovedscenario 
--	--and entityid = '127792'
--) x
--where rn = 1;

--Που έχουμε φθάσει μέχρι στιγμης? Εχουν συγκεντρωθεί approvals από legacy/non legacy, μαζι με τα αντιστοιχα στοιχεια entity και εχει γινει προτεραιοποιηση καθως και σηματοδοτηση

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
	 
--select * from perimeter_financials where entityid = '89865' --4.353

----------------------------------------------
--          DataWithoutFinancials
----------------------------------------------

drop table if exists perimeter_non_financials;
create temporary table perimeter_non_financials as
select * 
from final_rating
where modelid not in ('FA_FIN','PdModelCcategory');
	 
--select * from perimeter_non_financials --598

---------------------------------------------------------------------------------
--                       Final Table -  with financials
---------------------------------------------------------------------------------

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
select --distinct on (c.pkid_) --per.approveid, statementmonths, 
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
       ,per.entityversion --tbd
       ,per.versionid_--tbd
       ,c.versionid_ versionid_financial
       ,c.sourcepopulateddate_ sourcepopulateddate_financial
       ,per.sourcepopulateddate_rating sourcepopulateddate_rating
       ,per.flag flag_source_table
	   ,case when c.sourcepopulateddate_ <= per.sourcepopulateddate_rating then 'mh eteroxronismenh'
	          else 'eteroxronismenh' 
	    end as flag		
       -- ,c.sourcepopulateddate_
       --,statementmonths
       --,financialcontext
       --,c.pkid_
       --,c.factuphiststmtfinancialid_
from olapts.factuphiststmtfinancial c
inner join perimeter_financials per on c.entityid::int = per.entityid 
      and c.financialid = per.financialid 
	  and c.statementid=per.statementid::int
	  --and c.sourcepopulateddate_ <= per.sourcepopulateddate
join olapts.factuphiststmtfinancialgift d on c.pkid_ = d.pkid_ and c.versionid_ = d.versionid_ 
--join olapts.factratingscenario a on a.entityid   = c.entityid::int
--     and a.financialcontext = per.financialcontext and a.approveddate = per.approveddate
--join olapts.factentity b on b.entityid  = per.entityid::int
--     and b.versionid_ = per.entityversion
left join olapts.factentityrating f on per.approveid=f.approveid --new add
where 1=1 -- c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
--and a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}'       
--and b.versionid_ = {EntityVersion}                    
--and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
--and per.isdeleted_ = 'false'  
and  per.islatestapprovedscenario 
--and per.isprimary 
--and a.modelid = 'FA_FIN'
and per.approvalstatus = '2'                         
and per.ApprovedDate is not null  
and per.flag = 'Legacy'
and per.flag_entity = 'with_financials_legacy'
--and per.entityid = '100922' and statementyear = '2021'
order by c.pkid_, c.sourcepopulateddate_ desc,per.ApprovedDate desc
)x1
	)y where rn=1
order by entityid,cdi, afm, fiscalyear asc,sourcepopulateddate_financial desc, --added 09/01/2024
statementid desc , versionid_financial desc  --edit in 03/01/2024 8eloume to max statementid se periptwseis me 2 idia years
;

--select * from final_table_legacy--469

---------------------------
--         Ralph
---------------------------

--old

--drop table if exists final_table_ralph;
--create temporary table final_table_ralph as
--select distinct on (entityid,cdi, afm, fiscalyear) * 
--from (
--select distinct on(c.pkid_) 
--	        c.pkid_,
--            per.cdicode as cdi,
--            afm,
--            per.ratingscalevalue as grade,
--            cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate,
--            to_char(cast(cast(per.nextreviewdate as varchar(15)) as date),'dd-MM-yyyy') as nextreviewdate,
--            per.modelid as ratingmodelname,
--            c.statementyear::text as fiscalyear,
--            per.statementid, --per.financialcontext,
--            c.salesrevenues::text as salesrevenues,
--            c.totalassets::text as totalassets,
--            per.entityid::text as entityid,
--            case when f.FinalGrade is not null then 'true' else 'false' end as overrideflag, --new add
--			to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate               --new add
--            ,per.entityversion, --tbd
--            per.versionid_--tbd
--           ,c.versionid_ versionid_financial
--           ,c.sourcepopulateddate_ sourcepopulateddate_financial
--           ,per.sourcepopulateddate_rating sourcepopulateddate_rating 
--           ,per.flag flag_source_table
--from olapts.abuphiststmtfinancials c
--inner join perimeter_financials per on c.entityid::int = per.entityid 
--      and c.financialid::int = per.financialid 
--	  and c.statementid::int=per.statementid::int
--	  and c.sourcepopulateddate_ <= per.sourcepopulateddate_rating
----join olapts.abratingscenario a on cast(c.entityid as int) = cast(a.entityid as int)
----     and a.financialcontext = per.financialcontext and a.approveddate = per.approveddate
----join olapts.abfactentity b on cast(b.entityid as int)  = cast(per.entityid as int)
----     and b.versionid_ = per.entityversion
--left join olapts.abentityrating f on per.approveid=f.approveid --new add
--where  1=1 --c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
--       --and a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}'       
--       --and b.versionid_ = {EntityVersion}                    
--       --and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
--       --and a.isdeleted_ = 'false' 
--       and per.islatestapprovedscenario::boolean 
--       --and a.isprimary::boolean 
--       -- and a.modelid = 'FA_FIN'
--       and per.approvalstatus = '2'                         
--       and per.ApprovedDate is not null  
--      and per.flag = 'Ralph'
-- and per.flag_entity = 'with_financials_ralph'
----and per.entityid = '89865'
--order by c.pkid_, c.sourcepopulateddate_ desc, per.ApprovedDate desc
--) x2
--order by entityid,cdi, afm, fiscalyear asc,   sourcepopulateddate_financial desc, --added 09/01/2024
--statementid desc , versionid_financial desc  --edit in 03/01/2024 8eloume to max statementid se periptwseis me 2 idia years
--;

----new

drop table if exists final_table_ralph;
create temporary table final_table_ralph as
select distinct on (entityid,cdi, afm, fiscalyear) * 
from (
	select *,
	      case when flag='mh eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial desc,ApprovedDate desc ) 
	          when flag='eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial asc,ApprovedDate desc ) 
	      end as rn
	from (	
select --distinct on(c.pkid_) 
	        c.pkid_,
            per.cdicode as cdi,
            afm,
            per.ratingscalevalue as grade,
            cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate,
            to_char(cast(cast(per.nextreviewdate as varchar(15)) as date),'dd-MM-yyyy') as nextreviewdate,
            per.modelid as ratingmodelname,
            c.statementyear::text as fiscalyear,
            per.statementid, --per.financialcontext,
            c.salesrevenues::text as salesrevenues,
            c.totalassets::text as totalassets,
            per.entityid::text as entityid,
            case when f.FinalGrade is not null then 'true' else 'false' end as overrideflag, --new add
			to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate               --new add
            ,per.entityversion, --tbd
            per.versionid_--tbd
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
----	  and c.sourcepopulateddate_ <= per.sourcepopulateddate_rating
--join olapts.abratingscenario a on cast(c.entityid as int) = cast(a.entityid as int)
--     and a.financialcontext = per.financialcontext and a.approveddate = per.approveddate
--join olapts.abfactentity b on cast(b.entityid as int)  = cast(per.entityid as int)
--     and b.versionid_ = per.entityversion
left join olapts.abentityrating f on per.approveid=f.approveid --new add
where  1=1 --c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
       --and a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}'       
       --and b.versionid_ = {EntityVersion}                    
       --and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
       --and a.isdeleted_ = 'false' 
       and per.islatestapprovedscenario::boolean 
       --and a.isprimary::boolean 
       -- and a.modelid = 'FA_FIN'
       and per.approvalstatus = '2'                         
       and per.ApprovedDate is not null  
      and per.flag = 'Ralph'
 and per.flag_entity = 'with_financials_ralph'
--and per.entityid = '119925'  
order by c.pkid_, c.sourcepopulateddate_ desc, per.ApprovedDate desc
)x1
	)y where rn=1
order by entityid,cdi, afm, fiscalyear asc,   sourcepopulateddate_financial desc, --added 09/01/2024
statementid desc , versionid_financial desc  --edit in 03/01/2024 8eloume to max statementid se periptwseis me 2 idia years
;

--select * from final_table_ralph --3.881

----------------------------------------------------------------------
--                        UNION final_table
----------------------------------------------------------------------
			  
drop table if exists final_table;
create temporary table final_table as
select distinct on (entityid,cdi, afm, fiscalyear) * 
from (
      select cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate
      from final_table_legacy  -- where cdi like '%18228188%'
      union all
      select cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate	   
      from final_table_ralph  -- where cdi like '%18228188%'
) x
order by entityid,cdi, afm, fiscalyear asc;

--select * from final_table --4.350

---------------------------------------------------------------------------------
--                       Final Table - non financials
---------------------------------------------------------------------------------

drop table if exists final_table_non_financials;
create temporary table final_table_non_financials as
select distinct on (entityid,cdi, afm) * 
from (
(select distinct on (entityid,per.cdicode,afm)
       per.cdicode as  cdi,
       afm,
       per.ratingscalevalue as grade,
       cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate,
       to_char(cast(cast(per.nextreviewdate as varchar(15)) as date),'dd-MM-yyyy') as nextreviewdate,
       per.modelid as ratingmodelname,
       null as fiscalyear,
       null as statementid,--per.financialcontext,
       null as salesrevenues,
       null as totalassets,
       per.entityid::text as entityid,
       case when f.FinalGrade is not null then 'true' else 'false' end as overrideflag, --new add
       to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate      --new add
       ,per.entityversion, --tbd
       per.versionid_--tbd
from perimeter_non_financials per 
--join olapts.factratingscenario a on a.entityid   = per.entityid::int
--     and a.financialcontext = per.financialcontext and a.approveddate = per.approveddate
--join olapts.factentity b on b.entityid  = per.entityid::int
--     and b.sourcepopulateddate_ < per.sourcepopulateddate
left join olapts.factentityrating f on per.approveid=f.approveid --new add
where 1=1 -- c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
--and a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}'       
--and b.versionid_ = {EntityVersion}                    
--and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
--and per.isdeleted_ = 'false' 
and per.islatestapprovedscenario 
--and per.isprimary 
-- and a.modelid = 'FA_FIN'
and per.approvalstatus = '2'                         
and per.ApprovedDate is not null  
and per.flag = 'Legacy'
and per.flag_entity = 'non_financials_legacy'
order by entityid,per.cdicode,afm,per.sourcepopulateddate_rating desc)
	
union all

(select distinct on (entityid,per.cdicode,afm)
       per.cdicode as  cdi,
       afm,
       per.ratingscalevalue as grade,
       cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate,
       to_char(cast(cast(per.nextreviewdate as varchar(15)) as date),'dd-MM-yyyy') as nextreviewdate,
       per.modelid as ratingmodelname,
       null as fiscalyear,
       null as statementid , --per.financialcontext,
       null as salesrevenues,
       null as totalassets,
       per.entityid::text as entityid,
       case when f.FinalGrade is not null then 'true' else 'false' end as overrideflag, --new add
       to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate      --new add
       ,per.entityversion, --tbd
       per.versionid_--tbd 
from perimeter_non_financials per
--join olapts.abratingscenario a on cast(per.entityid as int) = cast(a.entityid as int)
--     and a.financialcontext = per.financialcontext and a.approveddate = per.approveddate
--join olapts.abfactentity b on cast(b.entityid as int)  = cast(per.entityid as int)
--        and b.sourcepopulateddate_ < per.sourcepopulateddate
left join olapts.abentityrating f on per.approveid=f.approveid --new add
where  1=1 --c.entityid = '{entityid}' and c.financialid = '{FinancialId}' and c.statementid = '{Convert.ToInt32(maxstatementid)}'
       --and a.financialcontext = '{FinancialContext}' and a.ApprovedDate = '{approveddate}'       
       --and b.versionid_ = {EntityVersion}                    
       --and c.sourcepopulateddate_ <= '{sourcepopulateddate}'  
       --and per.isdeleted_ = 'false' 
       --and per.islatestapprovedscenario::boolean 
       --and per.isprimary::boolean 
       ---- and a.modelid = 'FA_FIN'
       --and per.approvalstatus = '2'                         
       --and per.ApprovedDate is not null  
      and per.flag = 'Ralph'
      and per.flag_entity = 'non_financials_ralph'
order by entityid,per.cdicode,afm,per.sourcepopulateddate_rating desc)
) x
order by entityid,cdi, afm, fiscalyear asc;

--select * from final_table_non_financials --598

---------------------------------------------------------------------------------
--                       Final Table - union
---------------------------------------------------------------------------------

--insert into olapts.panegftest (
--drop table if exists olapts.panegftest;
drop table if exists final_Test1;
create temporary table final_Test1 as
select *
--into olapts.panegftest --1.259
from  (
       --(select distinct on (entityid) 
        --      cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate
              --,entityversion, versionid_
       -- from (
			 (select distinct on (entityid,cdi,case when afm ~ '^[0]+$' then lpad(afm,8,'0') else afm end) --manipulation of same cdis with afms zeros
			          cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate
              from final_table --1.260
			  --where entityid = '120414'
              order by entityid,cdi,case when afm ~ '^[0]+$' then lpad(afm,8,'0') else afm end, fiscalyear desc)
			-- )x
       -- order by entityid, approveddate  desc) --1.260
	
--comment 04/01.2024 - non fafin ektos panegf	
--union all
--	
--       (select distinct on (entityid,cdi,case when afm ~ '^[0]+$' then lpad(afm,8,'0') else afm end) --manipulation of same cdis with afms zeros
--               cdi,afm,grade,approveddate,nextreviewdate,ratingmodelname,fiscalyear,
--		       salesrevenues,totalassets,entityid,overrideflag,creditcommitteedate --,entityversion, versionid_
--       from final_table_non_financials
--	   --where entityid in ('124159','120414')
--       order by entityid,cdi,case when afm ~ '^[0]+$' then lpad(afm,8,'0') else afm end, approveddate  desc )--598 --628
--       
) x order by entityid
--);

--select * from final_Test1 --1.260

--select * from olapts.panegftest--1.858
--select * from approvals_ralph where entityid = '112233'
--select * from ratings_legacy where entityid = '112233'
--select * from ratings_union where entityid = '112233'
--select * from final_rating where entityid = '112233'
--select * from perimeter_financials where entityid = '112233' order by entityid --4353 
--select * from final_table where entityid = '112233' order by approveddate--4350
