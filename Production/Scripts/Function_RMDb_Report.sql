-------------------------------------------------------------------------------------------------------------------------
--                                      Run script in OLAP database 
-- Script creates a function olapts.rmdb_report which takes a date as parameter and populate olapts.rmdb_report table
-------------------------------------------------------------------------------------------------------------------------

-- FUNCTION: olapts.rmdb_report(date)

-- DROP FUNCTION olapts.rmdb_report(date);

CREATE OR REPLACE FUNCTION olapts.rmdb_report(IN ref_date date)
    RETURNS boolean
    LANGUAGE 'plpgsql'
    VOLATILE
    PARALLEL UNSAFE
    COST 100
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
	
	GET DIAGNOSTICS stack = PG_CONTEXT;
	pl_function:= substring(stack from 'function (.*?) line');
	pl_function:= substring(pl_function,1,length(pl_function)-2);
	-- RAISE Notice 'pl_function (%)',pl_function;
	
	pl_targettablename:='rmdb_report';
	
	truncate TABLE olapts.rmdb_report; 
	
-----------------------------------------------------------------------------------------------
--                               RMDb Report Code/ Sign Off 
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

drop table if exists ratings_ralph;
create temporary table ratings_ralph as
select pkid_,
       FinancialContext,
       EntityId,
       ApprovedDate,
	   approveid,
       Updateddate_,
       sourcepopulateddate_ sourcepopulateddate_rating,
	   IsLatestApprovedScenario,
	   nextreviewdate,
	   creditcommitteedate,
	   approvalstatus,
	   modelid,
	   entity_version_match AS entityVersion,
	   versionid_ as versionid_rating,
	   'Ralph' flag
from olapts.abRatingScenario  
where cast(ApprovedDate as date) >= '2021-01-06' 
and ApprovedDate <= cast(ref_date as date) + time '23:59:59'
and isdeleted_ = 'false' 
and IsLatestApprovedScenario::boolean
and IsPrimary::boolean 
and modelid in ('FA_FIN','PdModelCcategory') 
and FinancialContext <> '0' and FinancialContext <> '' and length(FinancialContext) > 16  
and FinancialContext is not null and FinancialContext <> '###' 
and approvalstatus = '2'                         
and ApprovedDate is not null 
order by EntityId,ApprovedDate desc;

--------------------------------------------------------------------------------
-- Find the entity information for this version where the ratings were approved 
--------------------------------------------------------------------------------

-------------------------------------
-- Ratings with financials
-------------------------------------

drop table if exists approvals_ralph_entity_with_financials;
create temporary table approvals_ralph_entity_with_financials as
select distinct a.* ,
	   gc18 as afm,
	   cdicode,
	   'with_financials_ralph' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from ratings_ralph a
left join olapts.abfactentity b on a.entityid=b.entityid and a.entityVersion::int = b.versionid_
	 and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'
where b.sourcepopulateddate_ is not null
order by a.entityid,sourcepopulateddate_entity desc,sourcepopulateddate_Rating desc  ;
	  
----------------------------------------------------------------------------------------
--                                     Legacy 
----------------------------------------------------------------------------------------

----------------------------------------------
-- Find all ratings / Approved & Non approved
----------------------------------------------

drop table if exists ratings_legacy;
create temporary table ratings_legacy as
select pkid_,
       FinancialContext,
       EntityId,
       ApprovedDate,
	   approveid, 
       Updateddate_,
       sourcepopulateddate_ sourcepopulateddate_rating,
	   IsLatestApprovedScenario,	   
	   nextreviewdate,
	   creditcommitteedate,
	   approvalstatus,
	   modelid,
	   cast(SUBSTRING((REGEXP_MATCHES(FinancialContext,';([^;#]*)#'))[1], 1) as int) AS entityVersion,
	   versionid_ versionid_rating,
	   'Legacy' flag
from olapts.factratingscenario 
where sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59' 	 
and cast(sourcepopulateddate_ as date) >='2021-01-06'
and isdeleted_ = 'false' 
and isprimary::boolean 
and modelid in ('FA_FIN','PdModelCcategory')
and FinancialContext <> '0' and FinancialContext <> '' and length(FinancialContext) > 16  
and FinancialContext is not null and FinancialContext <> '###' 
order by EntityId,sourcepopulateddate_ desc; 

--------------------------------------------------------------------------------
-- Find the entity information for this version where the ratings were approved 
--------------------------------------------------------------------------------

-------------------------------------
-- Ratings with financials
-------------------------------------

drop table if exists approvals_legacy_entity_with_financials;
create temporary table approvals_legacy_entity_with_financials as
select distinct a.*,
		gc18 as afm, 
		cdicode,
		'with_financials_legacy' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from ratings_legacy a
left join olapts.factentity b on b.entityid  = a.entityid::int
     and b.versionid_ = a.entityversion 
	 and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59' 	
where b.sourcepopulateddate_ is not null
order by a.entityid, sourcepopulateddate_entity desc, sourcepopulateddate_Rating desc;
	 
--------------------------------------------------------------------------------------------------------------
--
--                                Data Consolidation / Legacy & Ralph
--
--------------------------------------------------------------------------------------------------------------

drop table if exists all_ratings;
create temporary table all_ratings as
select distinct *
from (
select pkid_,financialcontext,entityid::int,approveddate,approveid, sourcepopulateddate_rating,modelid,nextreviewdate,creditcommitteedate,
       islatestapprovedscenario,approvalstatus,entityversion::int,flag,afm,cdicode,flag_entity,versionid_rating, sourcepopulateddate_entity
from approvals_ralph_entity_with_financials
union all
select pkid_,financialcontext,entityid,approveddate,approveid, sourcepopulateddate_rating,modelid,nextreviewdate,creditcommitteedate,
       islatestapprovedscenario,approvalstatus,entityversion,flag,afm,cdicode,flag_entity,versionid_rating,sourcepopulateddate_entity
from approvals_legacy_entity_with_financials
)x;

--------------------------------------------------------------------------------------------------------------
--
--                  Choose Final Rating/ Ranking per entityid/ choose the latest afm,cdi per entityid
--
-- When a rating approved in Legacy take the information from legacy tables. 
-- When a rating began in Legacy and approved in Ralph take the information from Ralph tables.
-- When a rating began in Ralph and approved in Ralph take the information from Ralph tables. 
--------------------------------------------------------------------------------------------------------------

drop table if exists final_rating; 
create temporary table final_rating as 
select *
from (
select row_number() over (partition by entityid order by entityid, sourcepopulateddate_rating desc,sourcepopulateddate_entity desc, flag asc) rn, *
from all_ratings 
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
      -- cast(SUBSTRING((REGEXP_MATCHES(FinancialContext,';([^;#]*)#'))[1], 1) as int) AS entityVersion,
       cast((REGEXP_MATCHES(FinancialContext, '^[^:]*'))[1] as int) AS FinancialId
from (select  * 
	  , (REGEXP_MATCHES(unnest(STRING_TO_ARRAY(REGEXP_REPLACE(FinancialContext, '.*#([^:*]+)', '\1'), ';')), '^(\d+)'))[1] AS statementid
	  ,(REGEXP_MATCHES(unnest(STRING_TO_ARRAY(REGEXP_REPLACE(FinancialContext,'.*#([^:*]+)', '\1'), ';')), ':(\d+)', 'g'))[1]  AS statementid_version  
      from final_rating
	  where modelid in ('FA_FIN','PdModelCcategory') 
	  order by FinancialContext
	 )x;
	 
create index per1 on perimeter_financials(entityid, statementid, financialid);

---------------------------------------------------------------------------------
--                             MACROS - RALPH
---------------------------------------------------------------------------------

---------------------------
--         Ralph
---------------------------
 
drop table if exists macros_ralph;
create temporary table macros_ralph as
select distinct on (per.entityid ,per.financialid,  per.statementid ,accountid )  
       per.entityid ,per.financialid,  per.statementid ,balances.versionid_ versionid_balances ,accountid,
	   balances.sourcepopulateddate_ sourcepopulateddate_balance,sourcepopulateddate_rating,populateddate_ populateddate_balances, originrounding, originbalance
from olapts.abhiststmtbalance balances 
inner join perimeter_financials per
      on balances.statementid = per.statementid 
	  and balances.financialid::int= per.financialid
where 1=1 
      and balances.accountid in ('1100','1520','1521','1522','1523','1640','1641','1642','1643','1646','1650','2680','2685','2686','2687',
			   '2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470',
			   '5950','5960','3400') 
	  and per.flag='Ralph'   	  
order by per.entityid ,per.financialid,  per.statementid,accountid, balances.sourcepopulateddate_ desc;

---------------------------------------------------------------------------------
--                             MACROS - Legacy
---------------------------------------------------------------------------------

drop table if exists balances_legacy;
create temporary table balances_legacy as
select *
from olapts.facthiststmtbalancelatest
where accountid in ('1100','1520','1521','1522','1523','1640','1641','1642','1643','1646','1650','2680','2685','2686','2687',
				   '2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470',
				   '5950','5960','3400');
create index bal1 on balances_legacy(statementid, financialid,accountid,sourcepopulateddate_);

drop table if exists macros_legacy;
create temporary table macros_legacy as
select distinct per.entityid,per.statementid,per.financialid, balance.versionid_ versionid_balances, accountid,balance.sourcepopulateddate_ sourcepopulateddate_balance,
      per.sourcepopulateddate_rating, originrounding, originbalance
FROM balances_legacy balance 
inner join perimeter_financials per 
      on  balance.financialid::int= per.financialid 
	  and balance.statementid=per.statementid::int	
where per.flag='Legacy'
      and cast(per.ApprovedDate  as date) >= '2021-01-06' 
	  and per.ApprovedDate <= cast(ref_date as date) + time '23:59:59';
	 
----------------------------------------------------------------------
--                MACROS - UNION final tables / Legacy & Ralph
----------------------------------------------------------------------

drop table if exists macros_all;
create temporary table macros_all as
select *
from (
select entityid,statementid,financialid,versionid_balances,accountid,sourcepopulateddate_balance,originrounding,originbalance,  'Legacy' flag  from macros_legacy
	union all
select entityid,statementid,financialid,versionid_balances,accountid::int,sourcepopulateddate_balance,originrounding::numeric(19,2),originbalance::numeric(19,2),  'Ralph' flag from macros_ralph
)x;

-----------------------------------FindInventory-----------------------------------

--1520+1521+1522+1523
--Inventories + Finished goods + Work in progress and semi finished products +Raw materials and packing materials 

drop table if exists inventories;
create temporary table inventories as
select entityid, statementid, financialid,sum(inventories) inventories, flag
from (
select distinct entityid,statementid,financialid, accountid,flag 
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as inventories
from macros_all where 1=1
and accountid in ('1520','1521','1522','1523')
	)x
group by entityid, statementid, financialid,flag;

-----------------------------------FindNettradereceivables-------------------------------

--1640 + 1641 + 1642 + 1643 + 1646 - 1650
--Trade Receivables(Gross)+Checques receivable+Bills receivable+Construction contracts+Due from related companies (trade)-Allow for Doubtful Accounts(-)

drop table if exists Nettradereceivables;
create temporary table Nettradereceivables as
select entityid, statementid, financialid,sum(Nettradereceivables) Nettradereceivables, flag
from (
select  distinct entityid,statementid,financialid,accountid, flag
       ,case when accountid ='1650' 
	         then coalesce((case when originrounding = '0' then -originbalance::decimal(19,2)
					             when originrounding = '1' then -originbalance::decimal(19,2)* 1000
						         when originrounding = '2' then -originbalance::decimal(19,2)* 100000 end),0)
	         else coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					             when originrounding = '1' then originbalance::decimal(19,2)* 1000
						         when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) end as Nettradereceivables
from macros_all  
where accountid in ('1640','1641','1642','1643','1646','1650') 
	)x
group by entityid, statementid, financialid, flag;

--------------------------------FindTradespayable--------------------------------		

--2680+2685+2686+2687
--Trade Payables(CP) +Cheques and Bills payable + Construction contracts - obligation + (Due to related companies - trade)

drop table if exists Tradespayable;
create temporary table Tradespayable as
select entityid, statementid, financialid,sum(Tradespayable) Tradespayable, flag
from (
select  distinct entityid,statementid,financialid,accountid, flag
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as Tradespayable					   
from macros_all  
where accountid in ('2680','2685','2686','2687') 
	)x
group by entityid, statementid, financialid, flag;	
	

---------------------------------FindTotalBankingDebts---------------------------------								

--2100+2110+2115+2120+2130+2150+2400+2410+2415+2420+2430+2440+2450+2460+2470
--LTD Bank+LTD other + Syndicated Loans + LTD Converitble + LTD Subordinated + Finance Leases (LTP) 
-- +CPLTD Bank + CPLTD Other + CPLTD Syndicated loans + CPLTD Convertible +CPLTD Subordinated + ST Bank Loans Payable 
-- + ST Other Loans Payable + Finance Leases(CP)+ Overdrafts

drop table if exists TotalBankingDebts;
create temporary table TotalBankingDebts as
select entityid, statementid, financialid,sum(TotalBankingDebts) TotalBankingDebts, flag
from (
select distinct entityid,statementid,financialid,accountid, flag
      ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
				      when originrounding = '1' then originbalance::decimal(19,2)* 1000 
				      when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as TotalBankingDebts	
from macros_all
where accountid in ('2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470')
	)x
group by entityid, statementid, financialid, flag;	
		
----------------------------FindShortTermBankingDebt----------------------------

--2400 + 2410 + 2415 + 2420 + 2430 + 2440 + 2450 + 2460+2470
--CPLTD Bank + CPLTD Other + CPLTD Syndicated loans + CPLTD Convertible +CPLTD Subordinated + ST Bank Loans Payable + ST Other Loans Payable + Finance Leases(CP)+ Overdrafts
	
drop table if exists ShortTermBankingDebt;
create temporary table ShortTermBankingDebt as
select entityid, statementid, financialid,sum(ShortTermBankingDebt) ShortTermBankingDebt, flag
from (
select  distinct entityid,statementid,financialid,accountid, flag
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as ShortTermBankingDebt					   
from macros_all
where accountid in ('2400','2410','2415','2420','2430','2440','2450','2460','2470')
	)x
group by entityid, statementid, financialid, flag;	
	
---------------------------FindLongTermBankingDebt---------------------------

--2100+2110+2115+2120+2130+2150						
--LTD Bank+LTD other + Syndicated Loans + LTD Converitble+LTD Subordinated + Finance Leases (LTP) 						
								
drop table if exists LongTermBankingDebt;
create temporary table LongTermBankingDebt as
select entityid, statementid, financialid,sum(LongTermBankingDebt) LongTermBankingDebt, flag
from (
select  distinct entityid,statementid,financialid,accountid, flag
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as LongTermBankingDebt					   
from macros_all 
where accountid in ('2100','2110','2115','2120','2130','2150')
	)x
group by entityid, statementid, financialid, flag;	

-----------------------------Finddividendspayables-----------------------------

--5950 + 5960						
--(Dividends Paid(Fin)+Dvds Paid(Minority S'holders))
																				
drop table if exists dividendspayables;
create temporary table dividendspayables as
select entityid, statementid, financialid,sum(dividendspayables) dividendspayables, flag
from (
select  distinct entityid,statementid,financialid,accountid, flag
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as dividendspayables					   
from macros_all
where accountid in ('5950','5960')
	)x
group by entityid, statementid, financialid, flag;	

-----------------------------FindInterestExpense (old function returninterestcoverage(!))-----------------------------

--3400
--InterestExpense

drop table if exists InterestExpense;
create temporary table InterestExpense as
select entityid, statementid, financialid,sum(InterestExpense) InterestExpense, flag
from (
select  distinct entityid,statementid,financialid,accountid, flag
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as InterestExpense					   
from macros_all	  
where accountid = '3400' 
	)x
group by entityid, statementid, financialid, flag;	

-----------------------------goodwill-----------------------------

--1100
--goodwill

drop table if exists goodwill;
create temporary table goodwill as
select entityid, statementid, financialid,goodwill, flag
from (
select  distinct entityid,statementid,financialid,accountid, flag
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as goodwill					   
from macros_all	  
where accountid = '1100'
	)x;

-------------------------------commonsharecapital + sharepremium----------------------------------

------------------------------------------
--                 ralph
------------------------------------------

drop table if exists per_ralph;
create temporary table per_ralph as 
select distinct on(a.pkid_)  a.pkid_, per.entityid, per.cdicode,afm,per.financialid,per.statementid,statementyear,statementmonths,
       commonsharecapital, sharepremium, approveddate, approveid,a.sourcepopulateddate_ date_financials, per.sourcepopulateddate_rating date_rating,
	   statementdatekey_
from olapts.abuphiststmtfinancials a 
inner join perimeter_financials per 
     on per.entityid::int = a.entityid::int
	 and a.financialid::int = per.financialid
	 and a.statementid = per.statementid 
	 and per.flag = 'Ralph'
order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc;

--!Old implementation: ordering based on statementyear, pkid_ desc. Due to bug 74487 changed to statementdatekey_

drop table if exists sharecapital_premium_ralph;
create temporary table sharecapital_premium_ralph as
with cte as (
       select entityid, cdicode,afm, financialid,statementid, statementdatekey_, statementyear, statementmonths, commonsharecapital, sharepremium, approveddate, approveid
       from per_ralph 
), cte2 as (select entityid, cdicode,afm,financialid, statementid, statementdatekey_, statementyear, statementmonths,commonsharecapital, sharepremium,
                   lag(commonsharecapital,1) over (partition by entityid,cdicode,afm, financialid order by statementdatekey_) prev_commonsharecapital,
			       lag(sharepremium,1) over (partition by entityid, cdicode,afm, financialid order by statementdatekey_) prev_sharepremium, approveddate, approveid
            from cte 
           ) 
select entityid, cdicode,afm, financialid, statementid, statementdatekey_, statementyear, statementmonths,
       commonsharecapital, prev_commonsharecapital, 
	   commonsharecapital::numeric(19,2) - prev_commonsharecapital::numeric(19,2) as chg_commonsharecapital,
	   sharepremium,prev_sharepremium,
       sharepremium::numeric(19,2) - prev_sharepremium::numeric(19,2)  as chg_sharepremium , approveddate, approveid 
from cte2
order by entityid, cdicode,afm, financialid, statementdatekey_;

------------------------------------------
--                 legacy
------------------------------------------
		
drop table if exists per_legacy;
create temporary table per_legacy as 
select distinct on(a.pkid_) a.pkid_ , per.entityid, per.cdicode,afm,per.financialid,per.statementid,statementyear,statementmonths,
       commonsharecapital, sharepremium,statementdatekey_, a.sourcepopulateddate_ date_financials, per.sourcepopulateddate_rating date_rating, 
	   a.versionid_ as versionid_financials, financialcontext    	   
from olapts.factuphiststmtfinancial a
inner join perimeter_financials per 
     on per.entityid::int = a.entityid::int
	 and a.financialid::int = per.financialid
	 and a.statementid = per.statementid::int
join olapts.factuphiststmtfinancialgift d on a.pkid_ = d.pkid_ and a.versionid_ = d.versionid_ 	 
where 1=1 
     and per.islatestapprovedscenario 
     and per.approvalstatus = '2'                         
     and per.ApprovedDate is not null  
     and per.flag = 'Legacy'
     and per.flag_entity = 'with_financials_legacy'
order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc;

--!Old implementation: ordering based on statementyear, pkid_ desc. Due to bug 74487 changed to statementdatekey_

drop table if exists sharecapital_premium_legacy;
create temporary table sharecapital_premium_legacy as
with cte as (
       select entityid, cdicode,afm, financialid,statementid, statementyear, statementmonths, commonsharecapital, sharepremium,statementdatekey_,
	          date_rating, versionid_financials,pkid_
       from per_legacy 
), cte2 as (select entityid, cdicode,afm,financialid, statementid, statementyear, statementmonths,commonsharecapital, sharepremium,statementdatekey_,
                   lag(commonsharecapital,1) over (partition by entityid,cdicode,afm, financialid order by statementdatekey_  ) prev_commonsharecapital,
			       lag(sharepremium,1) over (partition by entityid, cdicode,afm, financialid order by  statementdatekey_ ) prev_sharepremium,date_rating,			 
			versionid_financials 
            from cte 
           ) 
select entityid,cdicode,afm, financialid, statementid, statementyear, statementmonths,statementdatekey_,
       commonsharecapital, prev_commonsharecapital, 
	   commonsharecapital::numeric(19,2) - prev_commonsharecapital::numeric(19,2) as chg_commonsharecapital,
	   sharepremium,prev_sharepremium,
       sharepremium::numeric(19,2) - prev_sharepremium::numeric(19,2)  as chg_sharepremium,
	   (commonsharecapital::numeric(19,2) - prev_commonsharecapital::numeric(19,2))+(sharepremium::numeric(19,2) - prev_sharepremium::numeric(19,2)) diff,
	   date_rating,versionid_financials
from cte2
order by entityid, cdicode, afm, financialid, statementdatekey_;							

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
select distinct on (entityid,cdi, afm, fnc_year) * 
from (
	select *,
	case when flag='mh eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial desc,ApprovedDate desc ) 
	     when flag='eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial asc,ApprovedDate desc ) 
	end as rn
	from (
select  a.pkid_
	   ,per.cdicode as cdi
	   ,per.afm
	   ,coalesce(d.cashandequivalents::numeric,0.00)::numeric(19,2) as csh
	   ,coalesce(d.ebitda::numeric,0.00)::numeric(19,2) as ebitda
	   ,coalesce(d.totequityreserves::numeric,0.00)::numeric(19,2) as eqty 
	   ,coalesce(goodwill.goodwill::numeric,0.00)::numeric(19,2) as gdwill	 
	   ,coalesce(d.netprofit::numeric,0.00)::numeric(19,2) as nt_incm
	   ,coalesce(d.salesrevenues::numeric,0.00)::numeric(19,2) as sales_revenue
	   ,coalesce(d.netfixedassets::numeric,0.00)::numeric(19,2) as netfixedassets	   
	   ,coalesce(inventories.inventories::numeric,0.00)::numeric(19,2) as inventory	   
	   ,coalesce(Nettradereceivables.Nettradereceivables::numeric,0.00)::numeric(19,2) as nettradereceivables	   
       ,coalesce(d.totalassets::numeric,0.00)::numeric(19,2) as TotalAssets
	   ,coalesce(d.commonsharecapital::numeric,0.00)::numeric(19,2) as CommonShareCapital
	   ,coalesce(Tradespayable.Tradespayable::numeric,0.00)::numeric(19,2) as tradepayable      
	   ,coalesce(TotalBankingDebts.TotalBankingDebts::numeric,0.00)::numeric(19,2) as TotalBankingDebt	      	    
	   ,coalesce(ShortTermBankingDebt.ShortTermBankingDebt::numeric,0.00)::numeric(19,2) as ShortTermBankingDebt	      	    	   
	   ,coalesce(LongTermBankingDebt.LongTermBankingDebt::numeric,0.00)::numeric(19,2) as LongTermBankingDebt	      	    	      
	   ,coalesce(d.totalliabilities::numeric,0.00)::numeric(19,2) as TotalLiabilities
	   ,coalesce(d.grossprofit::numeric,0.00)::numeric(19,2) as GrossProfit
       ,coalesce(d.Ebit::numeric,0.00)::numeric(19,2) as Ebit
	   ,d.profitbeforetax::numeric(19,2) as ProfitBeforeTax
	   ,coalesce(d.workingcapital::numeric,0.00)::numeric(19,2) as WorkingCapital
       ,coalesce(d.dcfcffrmoperact::numeric,0.00)::numeric(19,2) as FlowsOperationalActivity
       ,coalesce(d.dcfcffrominvestact::numeric,0.00)::numeric(19,2) as FlowsInvestmentActivity
	   ,coalesce(d.dcfcffromfinact::numeric,0.00)::numeric(19,2) as FlowsFinancingActivity
	   ,sharecapital_premium.chg_commonsharecapital::numeric(19,2)+sharecapital_premium.chg_sharepremium::numeric(19,2) as ChgCommonShareCapital_ChgSharePremium
	   ,coalesce(dividendspayables.dividendspayables::numeric,0.00)::numeric(19,2) as Balancedividendspayable	      	    	         
	   ,coalesce(d.grossprofitmargin::numeric,0.00)::numeric(19,2) as GrossProfitMargin
       ,coalesce(d.netprofitmargin::numeric,0.00)::numeric(19,2) as NetProfitMargin
	   ,coalesce(d.ebitdamargin::numeric,0.00)::numeric(19,2) as EbitdaMargin
       ,case when d.ebitda::decimal(19,2) = 0.00  then 0.00
             else (TotalBankingDebts.TotalBankingDebts::decimal(19,2)/d.ebitda::decimal(19,2))::decimal(19,2)  
        end as TotalBankingDebttoEbitda 	   
	   ,case when d.ebitda::decimal(19,2) = 0.00 then 0.00
	         else ((coalesce(TotalBankingDebts.TotalBankingDebts::decimal(19,2),0.00) - d.cashandequivalents::decimal(19,2))/d.ebitda::decimal(19,2))::decimal(19,2)
		end as NetBankingDebttoEbitda	
       ,coalesce(d.debttoequity::numeric,0.00)::numeric(19,2) as TotalLiabilitiestoTotalEquity
	   ,coalesce(d.returnonassets::numeric,0.00)::numeric(19,2) as ReturnOnAssets
       ,coalesce(d.returnontoteqres::numeric,0.00)::numeric(19,2) as ReturnonEquity
	   ,case when interestexpense.InterestExpense::decimal(19,2)  = 0.00 or interestexpense.InterestExpense is null then '0.00' 
	         else (ebitda::decimal(19,2) / interestexpense.InterestExpense::decimal(19,2))::decimal(19,2) 
		end as interestcoverage
       ,coalesce(d.currentratio::numeric,0.00)::numeric(19,2) as CurrentRatio
	   ,coalesce(d.quickratio::numeric,0.00)::numeric(19,2) as QuickRatio
	   ,a.statementyear::text as fnc_year
	   ,to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate     	   	   
	   ,to_char(cast(a.statementdatekey_::varchar(15) as date),'yyyymmdd') as publish_date
	   ,cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate
	   ,to_char(cast(ref_date as date),'yyyymmdd') as reference_date ----------------!! external parameter
	   ,concat_ws('|',per.entityid::text,per.versionid_rating::text) as entityid
	   ,per.entityid entityid2	   
	   ,per.Statementid::int as Statementid
	   ,a.versionid_ versionid_financial
	   ,a.sourcepopulateddate_ sourcepopulateddate_financial
	   ,per.sourcepopulateddate_rating sourcepopulateddate_rating
	   ,case when a.sourcepopulateddate_ <= per.sourcepopulateddate_rating then 'mh eteroxronismenh'
	         else 'eteroxronismenh' 
	    end as flag
		,per.approveid
from olapts.factuphiststmtfinancial a
inner join perimeter_financials per 
     on per.entityid::int = a.entityid::int
	 and a.financialid::int = per.financialid
	 and a.statementid = per.statementid::int
join olapts.factuphiststmtfinancialgift d on a.pkid_ = d.pkid_ and a.versionid_ = d.versionid_ 	 	 
left join inventories inventories 
     on per.entityid = inventories.entityid 
	 and per.statementid = inventories.statementid
	 and per.financialid = inventories.financialid
left join Nettradereceivables Nettradereceivables 
     on per.entityid = Nettradereceivables.entityid 
	 and per.statementid = Nettradereceivables.statementid
	 and per.financialid = Nettradereceivables.financialid	
left join Tradespayable Tradespayable 
     on per.entityid = Tradespayable.entityid 
	 and per.statementid = Tradespayable.statementid
	 and per.financialid = Tradespayable.financialid	
left join TotalBankingDebts TotalBankingDebts 
     on per.entityid = TotalBankingDebts.entityid 
	 and per.statementid = TotalBankingDebts.statementid
	 and per.financialid = TotalBankingDebts.financialid	
left join ShortTermBankingDebt ShortTermBankingDebt 
     on per.entityid = ShortTermBankingDebt.entityid 
	 and per.statementid = ShortTermBankingDebt.statementid
	 and per.financialid = ShortTermBankingDebt.financialid	
left join LongTermBankingDebt LongTermBankingDebt 
     on per.entityid = LongTermBankingDebt.entityid 
	 and per.statementid = LongTermBankingDebt.statementid
	 and per.financialid = LongTermBankingDebt.financialid	
left join dividendspayables dividendspayables 
     on per.entityid = dividendspayables.entityid 
	 and per.statementid = dividendspayables.statementid
	 and per.financialid = dividendspayables.financialid		 
left join InterestExpense interestexpense
     on per.entityid = interestexpense.entityid 
	 and per.statementid = interestexpense.statementid
	 and per.financialid = interestexpense.financialid 
left join sharecapital_premium_legacy sharecapital_premium
     on per.entityid = sharecapital_premium.entityid 
	 and per.statementid = sharecapital_premium.statementid
	 and per.financialid = sharecapital_premium.financialid 
left join goodwill goodwill
     on per.entityid = goodwill.entityid 
	 and per.statementid = goodwill.statementid
	 and per.financialid = goodwill.financialid 	 
where a.statementmonths = 12 
and per.IsLatestApprovedScenario
and cast(per.ApprovedDate  as date) >= '2021-01-06' 
and per.ApprovedDate <= cast(ref_date as date) + time '23:59:59'
and per.approvalstatus = '2'                         
and per.ApprovedDate is not null  
and per.flag = 'Legacy'		
order by a.pkid_, a.sourcepopulateddate_ desc,per.ApprovedDate desc
)x 
	)y where rn=1
order by entityid,cdi, afm, fnc_year asc, sourcepopulateddate_financial desc, 
statementid desc, versionid_financial desc 
;

---------------------------
--         Ralph
---------------------------

drop table if exists final_table_ralph;
create temporary table final_table_ralph as
select distinct on (entityid,cdi, afm, fnc_year) * 
from (
	select *,
	      case when flag='mh eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial desc,ApprovedDate desc ) 
	          when flag='eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial asc,ApprovedDate desc ) 
	      end as rn
	from (			
select  a.pkid_
	   ,per.cdicode as cdi
	   ,per.afm
	   ,coalesce(a.cashandequivalents::numeric,0.00)::numeric(19,2) as csh
	   ,coalesce(a.ebitda::numeric,0.00)::numeric(19,2) as ebitda
	   ,coalesce(a.totequityreserves::numeric,0.00)::numeric(19,2) as eqty 
	   ,coalesce(goodwill.goodwill::numeric,0.00)::numeric(19,2) as gdwill	
	   ,coalesce(a.netprofit::numeric,0.00)::numeric(19,2) as nt_incm
	   ,coalesce(a.salesrevenues::numeric,0.00)::numeric(19,2) as sales_revenue
	   ,coalesce(a.netfixedassets::numeric,0.00)::numeric(19,2) as netfixedassets	   
	   ,coalesce(inventories.inventories::numeric,0.00)::numeric(19,2) as inventory	   
	   ,coalesce(Nettradereceivables.Nettradereceivables::numeric,0.00)::numeric(19,2) as nettradereceivables	   
       ,coalesce(a.totalassets::numeric,0.00)::numeric(19,2) as TotalAssets
	   ,coalesce(a.commonsharecapital::numeric,0.00)::numeric(19,2) as CommonShareCapital
	   ,coalesce(Tradespayable.Tradespayable::numeric,0.00)::numeric(19,2) as tradepayable      
	   ,coalesce(TotalBankingDebts.TotalBankingDebts::numeric,0.00)::numeric(19,2) as TotalBankingDebt	      	    
	   ,coalesce(ShortTermBankingDebt.ShortTermBankingDebt::numeric,0.00)::numeric(19,2) as ShortTermBankingDebt	      	    	   
	   ,coalesce(LongTermBankingDebt.LongTermBankingDebt::numeric,0.00)::numeric(19,2) as LongTermBankingDebt	      	    	      
	   ,coalesce(a.totalliabilities::numeric,0.00)::numeric(19,2) as TotalLiabilities
	   ,coalesce(a.grossprofit::numeric,0.00)::numeric(19,2) as GrossProfit
       ,coalesce(a.Ebit::numeric,0.00)::numeric(19,2) as Ebit
	   ,a.profitbeforetax::numeric(19,2) as ProfitBeforeTax
	   ,coalesce(a.workingcapital::numeric,0.00)::numeric(19,2) as WorkingCapital
       ,coalesce(a.dcfcffrmoperact::numeric,0.00)::numeric(19,2) as FlowsOperationalActivity
       ,coalesce(a.dcfcffrominvestact::numeric,0.00)::numeric(19,2) as FlowsInvestmentActivity
	   ,coalesce(a.dcfcffromfinact::numeric,0.00)::numeric(19,2) as FlowsFinancingActivity
	   ,sharecapital_premium.chg_commonsharecapital::numeric(19,2)+sharecapital_premium.chg_sharepremium::numeric(19,2) as ChgCommonShareCapital_ChgSharePremium
	   ,coalesce(dividendspayables.dividendspayables::numeric,0.00)::numeric(19,2) as Balancedividendspayable	      	    	         
	   ,coalesce(a.grossprofitmargin::numeric,0.00)::numeric(19,2) as GrossProfitMargin
       ,coalesce(a.netprofitmargin::numeric,0.00)::numeric(19,2) as NetProfitMargin
	   ,coalesce(a.ebitdamargin::numeric,0.00)::numeric(19,2) as EbitdaMargin
       ,case when a.ebitda::decimal(19,2) = 0.00  then 0.00
             else (TotalBankingDebts.TotalBankingDebts::decimal(19,2)/a.ebitda::decimal(19,2))::decimal(19,2)  
        end as TotalBankingDebttoEbitda 	   
	   ,case when a.ebitda::decimal(19,2) = 0.00 then 0.00
	         else ((coalesce(TotalBankingDebts.TotalBankingDebts::decimal(19,2),0.00) - a.cashandequivalents::decimal(19,2))/a.ebitda::decimal(19,2))::decimal(19,2)
		end as NetBankingDebttoEbitda	
       ,coalesce(a.debttoequity::numeric,0.00)::numeric(19,2) as TotalLiabilitiestoTotalEquity
	   ,coalesce(a.returnonassets::numeric,0.00)::numeric(19,2) as ReturnOnAssets
       ,coalesce(a.returnontoteqres::numeric,0.00)::numeric(19,2) as ReturnonEquity
	   ,case when interestexpense.InterestExpense::decimal(19,2)  = 0.00 or interestexpense.InterestExpense is null then '0.00' 
	         else (ebitda::decimal(19,2) / interestexpense.InterestExpense::decimal(19,2))::decimal(19,2) 
		end as interestcoverage
       ,coalesce(a.currentratio::numeric,0.00)::numeric(19,2) as CurrentRatio
	   ,coalesce(a.quickratio::numeric,0.00)::numeric(19,2) as QuickRatio
	   ,a.statementyear::text as fnc_year
	   ,to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate    	   	   
	   ,to_char(cast(a.statementdatekey_::varchar(15) as date),'yyyymmdd') as publish_date
	   --,to_char(per.approveddate,'yyyymmdd') as approveddate
	   ,cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate  
	   ,to_char(cast(ref_date as date),'yyyymmdd') as reference_date ----------------!! external parameter
	   ,concat_ws('|',per.entityid::text,per.versionid_rating::text) as entityid
	   ,per.entityid entityid2
	   ,per.statementid::int as Statementid
	   ,a.versionid_ versionid_financial
	   ,a.sourcepopulateddate_ sourcepopulateddate_financial
	   ,per.sourcepopulateddate_rating sourcepopulateddate_rating 
       ,per.flag flag_source_table
	   ,case when a.sourcepopulateddate_ <= per.sourcepopulateddate_rating then 'mh eteroxronismenh'
	         else 'eteroxronismenh' 
	    end as flag	
	  ,per.approveid
from olapts.abuphiststmtfinancials a 
inner join perimeter_financials per 
     on per.entityid::int = a.entityid::int
	 and a.financialid::int = per.financialid
	 and a.statementid = per.statementid 
left join inventories inventories 
     on per.entityid = inventories.entityid 
	 and per.statementid = inventories.statementid
	 and per.financialid = inventories.financialid
left join Nettradereceivables Nettradereceivables 
     on per.entityid = Nettradereceivables.entityid 
	 and per.statementid = Nettradereceivables.statementid
	 and per.financialid = Nettradereceivables.financialid	
left join Tradespayable Tradespayable 
     on per.entityid = Tradespayable.entityid 
	 and per.statementid = Tradespayable.statementid
	 and per.financialid = Tradespayable.financialid	
left join TotalBankingDebts TotalBankingDebts 
     on per.entityid = TotalBankingDebts.entityid 
	 and per.statementid = TotalBankingDebts.statementid
	 and per.financialid = TotalBankingDebts.financialid	
left join ShortTermBankingDebt ShortTermBankingDebt 
     on per.entityid = ShortTermBankingDebt.entityid 
	 and per.statementid = ShortTermBankingDebt.statementid
	 and per.financialid = ShortTermBankingDebt.financialid	
left join LongTermBankingDebt LongTermBankingDebt 
     on per.entityid = LongTermBankingDebt.entityid 
	 and per.statementid = LongTermBankingDebt.statementid
	 and per.financialid = LongTermBankingDebt.financialid	
left join dividendspayables dividendspayables 
     on per.entityid = dividendspayables.entityid 
	 and per.statementid = dividendspayables.statementid
	 and per.financialid = dividendspayables.financialid		 
left join InterestExpense interestexpense
     on per.entityid = interestexpense.entityid 
	 and per.statementid = interestexpense.statementid
	 and per.financialid = interestexpense.financialid 
left join sharecapital_premium_ralph sharecapital_premium
     on per.entityid = sharecapital_premium.entityid 
	 and per.statementid = sharecapital_premium.statementid
	 and per.financialid = sharecapital_premium.financialid     
left join goodwill goodwill
     on per.entityid = goodwill.entityid 
	 and per.statementid = goodwill.statementid
	 and per.financialid = goodwill.financialid 	 
where 1=1 
      and a.statementmonths = 12 
	  and cast(per.ApprovedDate as date) >= '2021-01-06' 
	  and per.ApprovedDate <= cast(ref_date as date) + time '23:59:59' 
      and per.flag = 'Ralph'
order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc
)x1
	)y where rn=1
order by entityid,cdi, afm, fnc_year asc,   sourcepopulateddate_financial desc, 
statementid desc , versionid_financial desc  
;

----------------------------------------------------------------------
--                   UNION final tables / Legacy & Ralph
----------------------------------------------------------------------

insert into olapts.rmdb_report (
select distinct on (entityid,cdi, afm, fnc_year) *
from (
select cdi,afm,csh,ebitda,eqty,gdwill,nt_incm,sales_revenue,netfixedassets,inventory,nettradereceivables,totalassets,commonsharecapital,tradepayable,
       totalbankingdebt,shorttermbankingdebt,longtermbankingdebt,totalliabilities,grossprofit,ebit,profitbeforetax,workingcapital,flowsoperationalactivity,
       flowsinvestmentactivity,flowsfinancingactivity,chgcommonsharecapital_chgsharepremium,balancedividendspayable,grossprofitmargin,netprofitmargin,
       ebitdamargin,totalbankingdebttoebitda,netbankingdebttoebitda,totalliabilitiestototalequity,returnonassets,returnonequity,interestcoverage,
       currentratio,quickratio,fnc_year,creditcommitteedate,publish_date,approveddate,reference_date, entityid
from final_table_legacy
union all
select cdi,afm,csh,ebitda,eqty,gdwill,nt_incm,sales_revenue,netfixedassets,inventory,nettradereceivables,totalassets,commonsharecapital,tradepayable,
       totalbankingdebt,shorttermbankingdebt,longtermbankingdebt,totalliabilities,grossprofit,ebit,profitbeforetax,workingcapital,flowsoperationalactivity,
       flowsinvestmentactivity,flowsfinancingactivity,chgcommonsharecapital_chgsharepremium,balancedividendspayable,grossprofitmargin,netprofitmargin,
       ebitdamargin,totalbankingdebttoebitda,netbankingdebttoebitda,totalliabilitiestototalequity,returnonassets,returnonequity,interestcoverage,
       currentratio,quickratio,fnc_year,creditcommitteedate,publish_date,approveddate,reference_date, entityid
from final_table_ralph 
) x
order by entityid,cdi, afm, fnc_year desc
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

ALTER FUNCTION olapts.rmdb_report(date) OWNER TO olap;

-------------------------------------------------------------
--Check if the function was created and table was populated
-------------------------------------------------------------

--select * from olapts.rmdb_report('2023-09-29')  --execution time: 16 mins
--select * from olapts.rmdb_report
