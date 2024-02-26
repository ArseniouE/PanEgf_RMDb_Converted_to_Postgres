CREATE OR REPLACE FUNCTION olapts.rmtest(IN ref_date date)
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

	pl_jobid:='populate_report'||TO_CHAR(now(), 'yyyymmddHH24MI')::varchar;

	GET DIAGNOSTICS stack = PG_CONTEXT;
	pl_function:= substring(stack from 'function (.*?) line');
	pl_function:= substring(pl_function,1,length(pl_function)-2);
	-- RAISE Notice 'pl_function (%)',pl_function;
	
	pl_targettablename:='rmtest';
	
	truncate TABLE olapts.rmtest; 
	
---------------------------------------------RM---------------------------------------------

---------------------------------------------------------------------------------
--                        GatherDataFromRatingScenario
---------------------------------------------------------------------------------

-----------------------------
--          ralph
-----------------------------

drop table if exists ratings_ralph;
create temporary table ratings_ralph as
select --distinct on (EntityId) 
       pkid_,
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
--and ApprovedDate <= '2023-09-29 23:59:59'
and ApprovedDate <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 
and isdeleted_ = 'false' 
and IsLatestApprovedScenario::boolean
and IsPrimary::boolean 
and modelid in ('FA_FIN','PdModelCcategory') 
and FinancialContext <> '0' and FinancialContext <> '' and length(FinancialContext) > 16  
and FinancialContext is not null and FinancialContext <> '###' 
and approvalstatus = '2'                         
and ApprovedDate is not null 
--and entityid = '102853'
order by EntityId,ApprovedDate desc;

--select * from ratings_ralph where entityid = '34504'--3.968

--------------------------------------------------

drop table if exists approvals_ralph_entity_with_financials;
create temporary table approvals_ralph_entity_with_financials as
select distinct --on (a.entityid) --added 04/01/2024 8eloume mono ena zeugos afm, cdi to pio prosfato
       a.* ,
	   gc18 as afm,
	   cdicode,
	   'with_financials_ralph' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from ratings_ralph a
left join olapts.abfactentity b on a.entityid=b.entityid and a.entityVersion::int = b.versionid_
     --and b.sourcepopulateddate_ <= '2023-09-29 23:59:59'
	 and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 	 
where 1=1 
      --and a.entityid = '31970'	 
	  --and modelid in ('FA_FIN','PdModelCcategory') 
	  and b.sourcepopulateddate_ is not null
order by a.entityid,sourcepopulateddate_entity desc,sourcepopulateddate_Rating desc  ; --added 04/01/2024
	  
--select * from approvals_ralph_entity_with_financials where entityid = '34504' --3.968

-----------------------------------------------

-----------------------------
--          legacy
-----------------------------

drop table if exists ratings_legacy;
create temporary table ratings_legacy as
select --distinct on (EntityId) 
       pkid_,
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
where --sourcepopulateddate_ <= '2023-09-29 23:59:59'
sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 	 
and cast(sourcepopulateddate_ as date) >='2021-01-06'
and isdeleted_ = 'false' 
--and islatestapprovedscenario::boolean 
and isprimary::boolean 
and modelid in ('FA_FIN','PdModelCcategory')
and FinancialContext <> '0' and FinancialContext <> '' and length(FinancialContext) > 16  
and FinancialContext is not null and FinancialContext <> '###' 
--and approvalstatus = '2'                         
--and ApprovedDate is not null  
--and EntityId in ('102853') 
order by EntityId,sourcepopulateddate_ desc; --ApprovedDate desc

--select * from ratings_legacy where entityid = '34504' --85.754

--------------------------------------------------

drop table if exists approvals_legacy_entity_with_financials;
create temporary table approvals_legacy_entity_with_financials as
select distinct --on (a.entityid) 
        a.* ,--added 04/01/2024 8eloume mono ena zeugos afm, cdi to pio prosfato
		gc18 as afm, 
		cdicode,
		'with_financials_legacy' flag_entity, b.sourcepopulateddate_ as sourcepopulateddate_entity
from ratings_legacy a
left join olapts.factentity b on b.entityid  = a.entityid::int
     and b.versionid_ = a.entityversion 
	 --and b.sourcepopulateddate_ <= '2023-09-29 23:59:59'  
	 and b.sourcepopulateddate_ <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 	
	 --and modelid = 'FA_FIN'
where 1=1 --and a.entityid = '30688' 
      and b.sourcepopulateddate_ is not null
	  --and cdicode = '5425397'
order by a.entityid, sourcepopulateddate_entity desc, sourcepopulateddate_Rating desc --added 04/01/2024 8eloume mono ena zeugos afm, cdi to pio prosfato
	 ;
	 
--select * from approvals_legacy_entity_with_financials where entityid = '34504' --85.754

-----------------------------------------------

----------------------------------------------------------------------------------
--                   union legacy + ralph financial
----------------------------------------------------------------------------------

--Consolidation των δεδομενων από τις legacy και non legacy δομες

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

--select * from all_ratings where entityid = '34504' --89.722

-------------------choose scenario financials------------------

--Ranking σε επιπεδο entityid,afm,cdicode με προτεραιοποίηση της legacy εναντι της νεας

drop table if exists final_rating; 
create temporary table final_rating as 
select *
from (
select row_number() over (partition by entityid order by entityid, sourcepopulateddate_rating desc,sourcepopulateddate_entity desc, flag asc) rn, *
from all_ratings 
where approveddate is not null and islatestapprovedscenario  --and entityid='103101' 
) x
where rn = 1;

--select * from final_rating where entityid = '30688' --1.265

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
      -- cast(SUBSTRING((REGEXP_MATCHES(FinancialContext,';([^;#]*)#'))[1], 1) as int) AS entityVersion,
       cast((REGEXP_MATCHES(FinancialContext, '^[^:]*'))[1] as int) AS FinancialId
from (select  * 
	  , (REGEXP_MATCHES(unnest(STRING_TO_ARRAY(REGEXP_REPLACE(FinancialContext, '.*#([^:*]+)', '\1'), ';')), '^(\d+)'))[1] AS statementid
	  ,(REGEXP_MATCHES(unnest(STRING_TO_ARRAY(REGEXP_REPLACE(FinancialContext,'.*#([^:*]+)', '\1'), ';')), ':(\d+)', 'g'))[1]  AS statementid_version  
      from final_rating
	  where modelid in ('FA_FIN','PdModelCcategory') --and entityid = '100066'
	  order by FinancialContext
	 )x;
	 
--select * from perimeter_financials where entityid = '34504' --4.365

create index per1 on perimeter_financials(entityid, statementid, financialid);


---------------------------------------------------------------------------------
--                       Final Table -  with financials
---------------------------------------------------------------------------------

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
--                             MACROS - RALPH
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

-----------------------------------
-- Find max versionid_ of balance
-----------------------------------

--drop table if exists max_version;
--create temporary table max_version as
--select distinct on (per.entityid ,per.financialid,  per.statementid )
--       per.entityid ,per.financialid,  per.statementid ,balances.versionid_ max_versionid_ ,accountid
--	   ,balances.sourcepopulateddate_ sourcepopulateddate_balances, per.sourcepopulateddate_rating
--from olapts.abhiststmtbalance balances 
--inner join perimeter_financials per
--      on balances.statementid = per.statementid 
--	  and balances.financialid::int= per.financialid
--	  and balances.sourcepopulateddate_ < per.sourcepopulateddate_rating
--where balances.sourcepopulateddate_ < per.sourcepopulateddate_rating
--     -- and entityid in ('111605') --and per.statementid = '18' --and per.financialid='92813' 
--	  and per.flag='Ralph'
--order by per.entityid ,per.financialid,  per.statementid, balances.sourcepopulateddate_ desc,accountid ;

--select * from max_version where entityid = '100887' 
--execution time: 4 mins

-------------------------
-- Calculate macros 
-------------------------

----OPTIMIZED

--drop table if exists macros_ralph;
--create temporary table macros_ralph as
--select distinct per.entityid,per.statementid,per.financialid, balances.versionid_ versionid_balances, 
--       balances.accountid,balances.sourcepopulateddate_ sourcepopulateddate_balance, per.sourcepopulateddate_rating, originrounding, originbalance
--from olapts.abhiststmtbalance balances 
--inner join perimeter_financials per
--      on balances.statementid = per.statementid 
--	  and balances.financialid::int= per.financialid
--	  and balances.sourcepopulateddate_ < per.sourcepopulateddate_rating
--left join max_version max_v
--	  on max_v.entityid=per.entityid	  
--	  and per.statementid = max_v.statementid 
--where balances.accountid in ('1100','1520','1521','1522','1523','1640','1641','1642','1643','1646','1650','2680','2685','2686','2687',
--				   '2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470',
--				   '5950','5960','3400') 
--      and balances.sourcepopulateddate_ < per.sourcepopulateddate_rating
--      --and per.entityid in ('100887','86564')  --and per.statementid = '18'
--	  and max_v.max_versionid_=balances.versionid_
--	  and per.flag='Ralph'
--	  --and per.entityid = '114748' and per.statementid= '7'
--	  ;
	
--------------------TEST 2024-01-05	--------------------

--ενώ στην νέα λύση είναι σε επίπεδο financialid,statementid (θα χρειαστεί χειρισμός στο sourcepopulateddate/by 
-- καθώς συνεχίζει να αναφέρεται σε επίπεδο accountid (δομικο CL) και μπορεί να δημιουργήσει πολλαπλότητες). 
 
drop table if exists macros_ralph;
create temporary table macros_ralph as
select distinct on (per.entityid ,per.financialid,  per.statementid ,accountid )  --added 05/01/2024 
       per.entityid ,per.financialid,  per.statementid ,balances.versionid_ versionid_balances ,accountid,
	   balances.sourcepopulateddate_ sourcepopulateddate_balance,sourcepopulateddate_rating,populateddate_ populateddate_balances, originrounding, originbalance
from olapts.abhiststmtbalance balances 
inner join perimeter_financials per
      on balances.statementid = per.statementid 
	  and balances.financialid::int= per.financialid
	  --and balances.sourcepopulateddate_ < per.sourcepopulateddate_rating
where 1=1 
    and balances.accountid in ('1100','1520','1521','1522','1523','1640','1641','1642','1643','1646','1650','2680','2685','2686','2687',
			   '2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470',
			   '5950','5960','3400') 
      --and entityid ='31970' --and per.statementid = '18' --and per.financialid='92813' 	  
	  and per.flag='Ralph'   	  --and islatestversion_  
order by per.entityid ,per.financialid,  per.statementid,accountid, balances.sourcepopulateddate_ desc ;


----karakoulis devops
--drop table if exists macros_ralph_test;
--create temporary table macros_ralph_test as
--select distinct on (per.entityid ,per.financialid,  per.statementid ,accountid )  --added 05/01/2024 
--       per.entityid ,per.financialid,  per.statementid ,balances.versionid_ versionid_balances ,accountid,
--	   balances.sourcepopulateddate_ sourcepopulateddate_balance,sourcepopulateddate_rating,balances.populateddate_ populateddate_balances, originrounding, originbalance
--from olapts.abhiststmtbalance balances 
--inner join perimeter_financials per
--      on balances.statementid = per.statementid 
--	  and balances.financialid::int= per.financialid
--	  --and balances.sourcepopulateddate_ < per.sourcepopulateddate_rating
--left join olapts.abhistoricalstatement hist
--     on per.financialid::int = hist.financialid::int
--	 and per.statementid::int= hist.statementid::int
--	 and per.statementid_version::int = hist.versionid_
--where 1=1 
--      and balances.accountid in ('1100','1520','1521','1522','1523','1640','1641','1642','1643','1646','1650','2680','2685','2686','2687',
--			   '2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470',
--			   '5950','5960','3400') 
--      --and entityid = '33622' and per.statementid = '21' and accountid = '1520'	  
--	  and per.flag='Ralph'   	  --and islatestversion_  
--	  and hist.status = 'Reviewed' 
--	  and balances.id_ = hist.dimhistoricalstatementid_
--order by per.entityid ,per.financialid,  per.statementid,accountid, balances.sourcepopulateddate_ desc ;

--select * from  macros_ralph where entityid = '100887'	  --414
--execution time: 7 mins

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
--                             MACROS - Legacy
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

drop table if exists balances_legacy;
create temporary table balances_legacy as
select *
from olapts.facthiststmtbalancelatest
where accountid in ('1100','1520','1521','1522','1523','1640','1641','1642','1643','1646','1650','2680','2685','2686','2687',
				   '2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470',
				   '5950','5960','3400');
				  -- and financialid = '30823'; 				  		 				   
create index bal1 on balances_legacy(statementid, financialid,accountid,sourcepopulateddate_);

drop table if exists macros_legacy;
create temporary table macros_legacy as
select distinct per.entityid,per.statementid,per.financialid, balance.versionid_ versionid_balances, accountid,balance.sourcepopulateddate_ sourcepopulateddate_balance,
      per.sourcepopulateddate_rating, originrounding, originbalance
FROM balances_legacy balance 
inner join perimeter_financials per 
      on  balance.financialid::int= per.financialid 
	  and balance.statementid=per.statementid::int	
	  --and balance.sourcepopulateddate_ < per.sourcepopulateddate_	  
where 1=1 --accountid in ('1520','1521','1522','1523','1640','1641','1642','1643','1646','1650','2680','2685','2686','2687',
	--			   '2100','2110','2115','2120','2130','2150','2400','2410','2415','2420','2430','2440','2450','2460','2470',
	--			   '5950','5960','3400') 
      and per.flag='Legacy'
      --and balance.sourcepopulateddate_ <= per.sourcepopulateddate_	  
      and cast(per.ApprovedDate  as date) >= '2021-01-06' 
	  and per.ApprovedDate <= '2023-09-29 23:59:59'  
	  
	 --and per.entityid = '30823'
	  --and per.ApprovedDate <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 
;
--select  * from macros_legacy where entityid = '91242' and accountid = '1640' --39.918
--execution time: 5mins

---------------------------------------------------------------------------------
---------------------------------------------------------------------------------
--                             MACROS - UNION
---------------------------------------------------------------------------------
---------------------------------------------------------------------------------

drop table if exists macros_all;
create temporary table macros_all as
select *
from (
select entityid,statementid,financialid,versionid_balances,accountid,sourcepopulateddate_balance,originrounding,originbalance,  'Legacy' flag  from macros_legacy
	union all
select entityid,statementid,financialid,versionid_balances,accountid::int,sourcepopulateddate_balance,originrounding::numeric(19,2),originbalance::numeric(19,2),  'Ralph' flag from macros_ralph
)x;

--select * from macros_all where entityid = '100887' and accountid in ('1520','1521','1522','1523') --40.332

-----------------------------------FindInventory-----------------------------------

--1520+1521+1522+1523
--Inventories + Finished goods + Work in progress and semi finished products +Raw materials and packing materials 

drop table if exists inventories;
create temporary table inventories as
select entityid, statementid, financialid,sum(inventories) inventories, flag
from (
select distinct entityid,statementid,financialid, accountid,flag --versionid_
       ,coalesce((case when originrounding = '0' then originbalance::decimal(19,2)
					       when originrounding = '1' then originbalance::decimal(19,2)* 1000 
						   when originrounding = '2' then originbalance::decimal(19,2)* 100000 end),0) as inventories
from macros_all where 1=1
and accountid in ('1520','1521','1522','1523') --and entityid = '100887' and statementid = '20' --and entityid in ('100887','86564') --
	)x
group by entityid, statementid, financialid,flag;

--select * from inventories where entityid = '100887' and statementid = '6'--3.653
--select * from olapts.returninventories('100887', '6') 

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
where accountid in ('1640','1641','1642','1643','1646','1650') --and entityid = '91803'
	)x
group by entityid, statementid, financialid, flag;

--select * from olapts.returnnettradereceivables('91803', '8')
--select * from Nettradereceivables where entityid = '91803' and statementid ='8' --4.257

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
where accountid in ('2680','2685','2686','2687') --and entityid = '111605'
	)x
group by entityid, statementid, financialid, flag;	
	
--select * from olapts.returntradespayable('102338', '18')
--select * from Tradespayable where entityid = '102338' and statementid ='18'  --4.259
	
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
	--and entityid = '114748' and statementid ='7'
	)x
group by entityid, statementid, financialid, flag;	
		
--select * from olapts.returntotalbankingdept('114748', '7')
--select * from TotalBankingDebts where entityid = '93034' and statementid ='36' --3.689	

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

--select * from olapts.returnshorttermbankingdept('93034', '36')
--select * from ShortTermBankingDebt where entityid = '93034' and statementid ='36' --3.560	
		
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

--select * from LongTermBankingDebt where entityid = '94292' and statementid ='4' --3.143
--select * from olapts.returnlongtermbankingdept('94292', '4')

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
where accountid in ('5950','5960') --and entityid = '34504'
	)x
group by entityid, statementid, financialid, flag;	

--select * from dividendspayables where entityid = '118468' and statementid ='10' --697
--select * from olapts.returndividendspayables('118468', '10') 


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

--select entityid,* from InterestExpense where financialid = '125477' --4.276
--select * from olapts.returninterestcoverage('125477', '2') 

-----------------------------goodwill --added 02/01/2024-----------------------------

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
--group by entityid, statementid, financialid, flag;	

--select  entityid, statementid from goodwill group by  entityid, statementid having count(*)>1
--select entityid,* from goodwill where financialid = '112233' --343

-------------------------------commonsharecapital + sharepremium----------------------------------

--ralph

--drop table if exists per_ralph_all;
--create temporary table per_ralph_all as
--select distinct on(a.pkid_) 
--       a.pkid_, per.entityid, per.cdicode,afm,per.financialid,per.statementid,statementyear,statementmonths,commonsharecapital, 
--       sharepremium, approveddate, approveid, a.sourcepopulateddate_ date_financials, per.sourcepopulateddate_rating date_rating  
--from olapts.abuphiststmtfinancials a --680
--inner join perimeter_financials per 
--     on per.entityid::int = a.entityid::int
--	 and a.financialid::int = per.financialid
--	 --and a.statementid = per.statementid 
--	 and a.sourcepopulateddate_ <= per.sourcepopulateddate_rating
--	 and per.flag = 'Ralph'
--	 --and per.entityid = '101474'
--order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc; 

--select * from per_ralph_all

--ralph

drop table if exists per_ralph;
create temporary table per_ralph as 
select distinct on(a.pkid_)  a.pkid_, per.entityid, per.cdicode,afm,per.financialid,per.statementid,statementdatekey_, statementyear,statementmonths,
       commonsharecapital, sharepremium, approveddate, approveid,a.sourcepopulateddate_ date_financials, per.sourcepopulateddate_rating date_rating  
from olapts.abuphiststmtfinancials a --43
inner join perimeter_financials per 
     on per.entityid::int = a.entityid::int
	 and a.financialid::int = per.financialid
	 and a.statementid = per.statementid 
	 --and a.sourcepopulateddate_ <= per.sourcepopulateddate_
	 and per.flag = 'Ralph'
	 --and per.entityid = '101474'	 
order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc;

--select * from per_ralph 43

--drop table if exists min_year_ralph;
--create temporary table min_year_ralph as
--select entityid, cdicode, afm , min(statementyear) min_year from per_ralph group by entityid, cdicode, afm;

--select * from min_year_ralph where entityid = '34324'

--drop table if exists change_field_ralph;
--create temporary table change_field_ralph as
--select *
--from (
--(select distinct on (a.entityid, a.cdicode, a.afm) 
--        a.* , 'exclude_from_rating' flag --, a.statementyear, b.min_year
--from per_ralph_all a
--left join min_year_ralph b on a.entityid = b.entityid and a.cdicode=b.cdicode and a.afm=b.afm 
--where a.statementyear=b.min_year-1
----and a.entityid = '34324'
--and date_financials<=date_rating
--order by a.entityid, a.cdicode, a.afm, a.statementyear desc, date_financials desc)
--union all
--select * , 'include_in_rating' flag from per_ralph
--)x
--order by entityid, cdicode, afm, statementyear;

--select * from change_field

--
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

--select * from sharecapital_premium_ralph where entityid = '101474' order by entityid --43

------------------------------------------

--legacy
		
--drop table if exists per_legacy_all;
--create temporary table per_legacy_all as 
--select distinct on(a.pkid_)  a.pkid_, per.entityid, per.cdicode,afm,per.financialid,per.statementid,statementyear,statementmonths,commonsharecapital, 
--       sharepremium,statementdatekey_ ,a.sourcepopulateddate_ date_financials, per.sourcepopulateddate_rating date_rating , 
--	   a.versionid_ as versionid_financials, financialcontext
--from olapts.factuphiststmtfinancial a
--inner join perimeter_financials per 
--     on per.entityid::int = a.entityid::int
--	 and a.financialid::int = per.financialid
--	 --and a.statementid = per.statementid::int
--	 and a.sourcepopulateddate_ <= per.sourcepopulateddate_rating
--join olapts.factuphiststmtfinancialgift d on a.pkid_ = d.pkid_ and a.versionid_ = d.versionid_ 	 
--where 1=1 
--     and per.islatestapprovedscenario 
--     and per.approvalstatus = '2'                         
--     and per.ApprovedDate is not null  
--     and per.flag = 'Legacy'
--     and per.flag_entity = 'with_financials_legacy'
--    -- and per.entityid = '34324'
--order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc;
--
--select * from per_legacy_all where entityid = '31549' order by statementyear

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
	 --and a.sourcepopulateddate_ <= per.sourcepopulateddate_
join olapts.factuphiststmtfinancialgift d on a.pkid_ = d.pkid_ and a.versionid_ = d.versionid_ 	 
where 1=1 
     and per.islatestapprovedscenario 
     and per.approvalstatus = '2'                         
     and per.ApprovedDate is not null  
     and per.flag = 'Legacy'
     and per.flag_entity = 'with_financials_legacy'
     --and per.entityid = '34324'
order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc;

--select * from per_legacy where entityid = '31549' --4.322

--drop table if exists min_year_legacy;
--create temporary table min_year_legacy as
--select entityid, cdicode, afm , min(statementyear) min_year from per_legacy group by entityid, cdicode, afm;

--select * from min_year_legacy where entityid = '31549'

------

--drop table if exists change_field_legacy;
--create temporary table change_field_legacy as
--select *
--from (
--(select distinct on (a.entityid, a.cdicode, a.afm) 
--        a.* , 'exclude_from_rating' flag --, a.statementyear, b.min_year
--from per_legacy_all a 
--left join min_year_legacy b on a.entityid = b.entityid and a.cdicode=b.cdicode and a.afm=b.afm 
--where a.statementyear=b.min_year-1
----and a.entityid = '31549'
--and date_financials<=date_rating
--order by a.entityid, a.cdicode, a.afm, a.statementyear desc, date_financials desc)
--union all
--select * , 'include_in_rating' flag from per_legacy
--)x
--order by entityid, cdicode, afm, statementyear;

--select * from change_field_legacy where entityid = '31549'

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

--select * from sharecapital_premium_legacy where entityid = '94408' --4322

---------------------------------------------------------------------------------
--                         FINAL TABLES - UNION
---------------------------------------------------------------------------------

--drop table if exists final_table_legacy_old;
--create temporary table final_table_legacy_old as
--select distinct on (entityid,cdi, afm, fnc_year) * 
--from (
--select distinct on(a.pkid_)  a.pkid_
--	    ,per.cdicode as cdi
--	   ,per.afm
--	   ,coalesce(d.cashandequivalents::numeric,0.00)::numeric(19,2) as csh
--	   ,coalesce(d.ebitda::numeric,0.00)::numeric(19,2) as ebitda
--	   ,coalesce(d.totequityreserves::numeric,0.00)::numeric(19,2) as eqty 
--	   --,coalesce(d.goodwill::numeric,0.00)::numeric(19,2) as gdwill_old ---tbd
--	   ,coalesce(goodwill.goodwill::numeric,0.00)::numeric(19,2) as gdwill	 
--	   --,goodwill.goodwill::numeric(19,2) as gdwill		   
--	   ,coalesce(d.netprofit::numeric,0.00)::numeric(19,2) as nt_incm
--	   ,coalesce(d.salesrevenues::numeric,0.00)::numeric(19,2) as sales_revenue
--	   ,coalesce(d.netfixedassets::numeric,0.00)::numeric(19,2) as netfixedassets	   
--	   ,coalesce(inventories.inventories::numeric,0.00)::numeric(19,2) as inventory	   
--	   ,coalesce(Nettradereceivables.Nettradereceivables::numeric,0.00)::numeric(19,2) as nettradereceivables	   
--       ,coalesce(d.totalassets::numeric,0.00)::numeric(19,2) as TotalAssets
--	   ,coalesce(d.commonsharecapital::numeric,0.00)::numeric(19,2) as CommonShareCapital
--	   ,coalesce(Tradespayable.Tradespayable::numeric,0.00)::numeric(19,2) as tradepayable      
--	   ,coalesce(TotalBankingDebts.TotalBankingDebts::numeric,0.00)::numeric(19,2) as TotalBankingDebt	      	    
--	   ,coalesce(ShortTermBankingDebt.ShortTermBankingDebt::numeric,0.00)::numeric(19,2) as ShortTermBankingDebt	      	    	   
--	   ,coalesce(LongTermBankingDebt.LongTermBankingDebt::numeric,0.00)::numeric(19,2) as LongTermBankingDebt	      	    	      
--	   ,coalesce(d.totalliabilities::numeric,0.00)::numeric(19,2) as TotalLiabilities
--	   ,coalesce(d.grossprofit::numeric,0.00)::numeric(19,2) as GrossProfit
--       ,coalesce(d.Ebit::numeric,0.00)::numeric(19,2) as Ebit
--	   ,d.profitbeforetax::numeric(19,2) as ProfitBeforeTax
--	   ,coalesce(d.workingcapital::numeric,0.00)::numeric(19,2) as WorkingCapital
--       ,coalesce(d.dcfcffrmoperact::numeric,0.00)::numeric(19,2) as FlowsOperationalActivity
--       ,coalesce(d.dcfcffrominvestact::numeric,0.00)::numeric(19,2) as FlowsInvestmentActivity
--	   ,coalesce(d.dcfcffromfinact::numeric,0.00)::numeric(19,2) as FlowsFinancingActivity
--	   ,sharecapital_premium.chg_commonsharecapital::numeric(19,2)+sharecapital_premium.chg_sharepremium::numeric(19,2) as ChgCommonShareCapital_ChgSharePremium
--	   ,coalesce(dividendspayables.dividendspayables::numeric,0.00)::numeric(19,2) as Balancedividendspayable	      	    	         
--	   ,coalesce(d.grossprofitmargin::numeric,0.00)::numeric(19,2) as GrossProfitMargin
--       ,coalesce(d.netprofitmargin::numeric,0.00)::numeric(19,2) as NetProfitMargin
--	   ,coalesce(d.ebitdamargin::numeric,0.00)::numeric(19,2) as EbitdaMargin
--       ,case when d.ebitda::decimal(19,2) = 0.00  then 0.00
--             else (TotalBankingDebts.TotalBankingDebts::decimal(19,2)/d.ebitda::decimal(19,2))::decimal(19,2)  
--        end as TotalBankingDebttoEbitda 	   
--	   ,case when d.ebitda::decimal(19,2) = 0.00 then 0.00
--	         else ((coalesce(TotalBankingDebts.TotalBankingDebts::decimal(19,2),0.00) - d.cashandequivalents::decimal(19,2))/d.ebitda::decimal(19,2))::decimal(19,2)
--		end as NetBankingDebttoEbitda	
--       ,coalesce(d.debttoequity::numeric,0.00)::numeric(19,2) as TotalLiabilitiestoTotalEquity
--	   ,coalesce(d.returnonassets::numeric,0.00)::numeric(19,2) as ReturnOnAssets
--       ,coalesce(d.returnontoteqres::numeric,0.00)::numeric(19,2) as ReturnonEquity
--	   ,case when interestexpense.InterestExpense::decimal(19,2)  = 0.00 or interestexpense.InterestExpense is null then '0.00' 
--	         else (ebitda::decimal(19,2) / interestexpense.InterestExpense::decimal(19,2))::decimal(19,2) 
--		end as interestcoverage
--       ,coalesce(d.currentratio::numeric,0.00)::numeric(19,2) as CurrentRatio
--	   ,coalesce(d.quickratio::numeric,0.00)::numeric(19,2) as QuickRatio
--	   ,a.statementyear::text as fnc_year
--	   ,to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate      --new add	   	   
--	   ,to_char(cast(a.statementdatekey_::varchar(15) as date),'yyyymmdd') as publish_date
--	   --,to_char(per.approveddate,'yyyymmdd') as approveddate 	   
--	   ,cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate
--	   ,'20210930' as reference_date ----------------!! external parameter
--	   ,concat_ws('|',per.entityid::text,per.versionid_rating::text) as entityid
--	   --,per.entityid entityid2	   
--	   --,per.FinancialId::int as FinancialId 
--	   ,per.Statementid::int as Statementid
--	   ,a.versionid_ versionid_financial
--	   ,a.sourcepopulateddate_ sourcepopulateddate_financial
--from olapts.factuphiststmtfinancial a
--inner join perimeter_financials per 
--     on per.entityid::int = a.entityid::int
--	 and a.financialid::int = per.financialid
--	 and a.statementid = per.statementid::int
--	 --and a.sourcepopulateddate_ <= per.sourcepopulateddate_ 
--join olapts.factuphiststmtfinancialgift d on a.pkid_ = d.pkid_ and a.versionid_ = d.versionid_ 	 	 
--left join inventories inventories 
--     on per.entityid = inventories.entityid 
--	 and per.statementid = inventories.statementid
--	 and per.financialid = inventories.financialid
--left join Nettradereceivables Nettradereceivables 
--     on per.entityid = Nettradereceivables.entityid 
--	 and per.statementid = Nettradereceivables.statementid
--	 and per.financialid = Nettradereceivables.financialid	
--left join Tradespayable Tradespayable 
--     on per.entityid = Tradespayable.entityid 
--	 and per.statementid = Tradespayable.statementid
--	 and per.financialid = Tradespayable.financialid	
--left join TotalBankingDebts TotalBankingDebts 
--     on per.entityid = TotalBankingDebts.entityid 
--	 and per.statementid = TotalBankingDebts.statementid
--	 and per.financialid = TotalBankingDebts.financialid	
--left join ShortTermBankingDebt ShortTermBankingDebt 
--     on per.entityid = ShortTermBankingDebt.entityid 
--	 and per.statementid = ShortTermBankingDebt.statementid
--	 and per.financialid = ShortTermBankingDebt.financialid	
--left join LongTermBankingDebt LongTermBankingDebt 
--     on per.entityid = LongTermBankingDebt.entityid 
--	 and per.statementid = LongTermBankingDebt.statementid
--	 and per.financialid = LongTermBankingDebt.financialid	
--left join dividendspayables dividendspayables 
--     on per.entityid = dividendspayables.entityid 
--	 and per.statementid = dividendspayables.statementid
--	 and per.financialid = dividendspayables.financialid		 
--left join InterestExpense interestexpense
--     on per.entityid = interestexpense.entityid 
--	 and per.statementid = interestexpense.statementid
--	 and per.financialid = interestexpense.financialid 
--left join sharecapital_premium_legacy sharecapital_premium
--     on per.entityid = sharecapital_premium.entityid 
--	 and per.statementid = sharecapital_premium.statementid
--	 and per.financialid = sharecapital_premium.financialid 
--left join goodwill goodwill
--     on per.entityid = goodwill.entityid 
--	 and per.statementid = goodwill.statementid
--	 and per.financialid = goodwill.financialid 	 
--where 1=1 --a.entityid = '{entityid}' and a.financialid = '{FinancialId}' and a.statementid = '{Convert.ToInt32(firstNumber)}' 
--and a.statementmonths = 12 
----and a.sourcepopulateddate_ <= per.sourcepopulateddat
--and per.IsLatestApprovedScenario
--and cast(per.ApprovedDate  as date) >= '2021-01-06' 
--and per.ApprovedDate <= '2023-09-29 23:59:59'
----and per.ApprovedDate <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 
----and c.modelid in ('FA_FIN','PdModelCcategory')
--and per.approvalstatus = '2'                         
--and per.ApprovedDate is not null  
--and per.flag = 'Legacy'
--and per.entityid ='95673' and a.statementyear = '2019'
--and sharecapital_premium.flag <> 'exclude_from_rating'
--order by a.pkid_, a.sourcepopulateddate_ desc,per.ApprovedDate desc)x
--order by entityid,cdi, afm, fnc_year asc,statementid desc, versionid_financial desc  --edit in 03/01/2024 8eloume to max statementid se periptwseis me 2 idia years
--;

--select fnc_year, eqty,* from final_table_legacy where cdi = '1247484' and fnc_year = '2018'--4.338

drop table if exists final_table_legacy;
create temporary table final_table_legacy as
select distinct on (entityid,cdi, afm, fnc_year) * 
from (
	select *,
	case when flag='mh eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial desc,ApprovedDate desc ) 
	     when flag='eteroxronismenh' then row_number() over (partition by pkid_ order by flag desc,pkid_, sourcepopulateddate_financial asc,ApprovedDate desc ) 
	end as rn
	from (
select --distinct on(a.pkid_) 
	    a.pkid_
	   ,per.cdicode as cdi
	   ,per.afm
	   ,coalesce(d.cashandequivalents::numeric,0.00)::numeric(19,2) as csh
	   ,coalesce(d.ebitda::numeric,0.00)::numeric(19,2) as ebitda
	   ,coalesce(d.totequityreserves::numeric,0.00)::numeric(19,2) as eqty 
	   --,coalesce(d.goodwill::numeric,0.00)::numeric(19,2) as gdwill_old ---tbd
	   ,coalesce(goodwill.goodwill::numeric,0.00)::numeric(19,2) as gdwill	 
	   --,goodwill.goodwill::numeric(19,2) as gdwill		   
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
	   ,to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate      --new add	   	   
	   ,to_char(cast(a.statementdatekey_::varchar(15) as date),'yyyymmdd') as publish_date
	   --,to_char(per.approveddate,'yyyymmdd') as approveddate 	   
	   ,cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate
	   ,to_char(cast(ref_date as date),'yyyymmdd') as reference_date ----------------!! external parameter
	   ,concat_ws('|',per.entityid::text,per.versionid_rating::text) as entityid
	   ,per.entityid entityid2	   
	   --,per.FinancialId::int as FinancialId 
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
	 --and a.sourcepopulateddate_ <= per.sourcepopulateddate_ 
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
where 1=1 --a.entityid = '{entityid}' and a.financialid = '{FinancialId}' and a.statementid = '{Convert.ToInt32(firstNumber)}' 
and a.statementmonths = 12 
--and a.sourcepopulateddate_ <= per.sourcepopulateddat
and per.IsLatestApprovedScenario
and cast(per.ApprovedDate  as date) >= '2021-01-06' 
and per.ApprovedDate <= '2023-09-29 23:59:59'
--and per.ApprovedDate <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 
--and c.modelid in ('FA_FIN','PdModelCcategory')
and per.approvalstatus = '2'                         
and per.ApprovedDate is not null  
and per.flag = 'Legacy'		
--and a.statementyear = '2019' and per.cdicode = '1026401' --and per.entityid ='34504' 		
order by a.pkid_, a.sourcepopulateddate_ desc,per.ApprovedDate desc
)x 
	)y where rn=1
order by entityid,cdi, afm, fnc_year asc, sourcepopulateddate_financial desc, --added 09/01/2024
statementid desc, versionid_financial desc  --edit in 03/01/2024 8eloume to max statementid se periptwseis me 2 idia years
;

--select * from final_table_legacy --4.301

--select flag,fnc_year,* from final_table_legacy order by entityid, cdi, afm, fnc_year
--select * from final_table_legacy where entityid like '101014%' and fnc_year = '2021'

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
select --distinct on(a.pkid_) 
		a.pkid_
	   ,per.cdicode as cdi
	   ,per.afm
	   ,coalesce(a.cashandequivalents::numeric,0.00)::numeric(19,2) as csh
	   ,coalesce(a.ebitda::numeric,0.00)::numeric(19,2) as ebitda
	   ,coalesce(a.totequityreserves::numeric,0.00)::numeric(19,2) as eqty 
	   --,coalesce(a.goodwill::numeric,0.00)::numeric(19,2) as gdwill_old
	   ,coalesce(goodwill.goodwill::numeric,0.00)::numeric(19,2) as gdwill	
	   --,goodwill.goodwill::numeric(19,2) as gdwill	
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
	   ,to_char(per.creditcommitteedate,'dd-MM-yyyy')as creditcommitteedate      --new add	   	   
	   ,to_char(cast(a.statementdatekey_::varchar(15) as date),'yyyymmdd') as publish_date
	   --,to_char(per.approveddate,'yyyymmdd') as approveddate
	   ,cast(cast(per.approveddate as timestamptz) as varchar(30)) as approveddate  
	   ,to_char(cast(ref_date as date),'yyyymmdd') as reference_date ----------------!! external parameter
	   ,concat_ws('|',per.entityid::text,per.versionid_rating::text) as entityid
	   ,per.entityid entityid2  -----------------tbd
	   --,per.FinancialId::int as FinancialId -----------------tbd
	   --,per.Statementid::int as Statementid -----------------tbd	 
	   ,per.statementid::int as Statementid
	   ,a.versionid_ versionid_financial
	   ,a.sourcepopulateddate_ sourcepopulateddate_financial
	   ,per.sourcepopulateddate_rating sourcepopulateddate_rating 
       ,per.flag flag_source_table
	   ,case when a.sourcepopulateddate_ <= per.sourcepopulateddate_rating then 'mh eteroxronismenh'
	         else 'eteroxronismenh' 
	    end as flag	
	  ,per.approveid
from olapts.abuphiststmtfinancials a --3845
inner join perimeter_financials per 
     on per.entityid::int = a.entityid::int
	 and a.financialid::int = per.financialid
	 and a.statementid = per.statementid 
----and a.sourcepopulateddate_ <= per.sourcepopulateddate_rating  --03/01/2024
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
	  and per.ApprovedDate <= '2023-09-29 23:59:59'
	  --and per.ApprovedDate <= cast(ref_date as date) + time '23:59:59'--'2023-09-29 23:59:59' 
      and per.flag = 'Ralph'
	 --and a.entityid = '101474'
order by a.pkid_, a.sourcepopulateddate_ desc, per.ApprovedDate desc
)x1
	)y where rn=1
order by entityid,cdi, afm, fnc_year asc,   sourcepopulateddate_financial desc, --added 09/01/2024
statementid desc , versionid_financial desc  --edit in 03/01/2024 8eloume to max statementid se periptwseis me 2 idia years
;

--select ChgCommonShareCapital_ChgSharePremium,fnc_year,* from final_table_ralph --43


----------------------------------------------------------------------
--                        UNION final_table
----------------------------------------------------------------------

--drop table olapts.rmtest
insert into olapts.rmtest (
--drop table if exists final_table;
--create temporary table final_table as
select distinct on (entityid,cdi, afm, fnc_year) *
--into olapts.rmtest
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
order by entityid,cdi, afm, fnc_year desc--;
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

--select * from olapts.rmtest('2023-09-29')  --execution time: 16 mins
--select * from olapts.rmtest_view

--select * from olapts.rmtest_2 where cdi = '015565494'


select cdi,afm,csh,ebitda,eqty,gdwill,nt_incm,sales_revenue,netfixedassets,inventory,nettradereceivables,totalassets,commonsharecapital,tradepayable,
       totalbankingdebt,shorttermbankingdebt,longtermbankingdebt,totalliabilities,grossprofit,ebit,profitbeforetax,workingcapital,flowsoperationalactivity,
       flowsinvestmentactivity,flowsfinancingactivity,chgcommonsharecapital_chgsharepremium,balancedividendspayable,grossprofitmargin,netprofitmargin,
       ebitdamargin,totalbankingdebttoebitda,netbankingdebttoebitda,totalliabilitiestototalequity,returnonassets,returnonequity,interestcoverage,
       currentratio,quickratio,fnc_year,creditcommitteedate,publish_date,approveddate, reference_date, entityid from olapts.rmtest
except
select cdi,afm,csh,ebitda,eqty,gdwill,nt_incm,sales_revenue,netfixedassets,inventory,nettradereceivables,totalassets,commonsharecapital,tradepayable,
       totalbankingdebt,shorttermbankingdebt,longtermbankingdebt,totalliabilities,grossprofit,ebit,profitbeforetax,workingcapital,flowsoperationalactivity,
       flowsinvestmentactivity,flowsfinancingactivity,chgcommonsharecapital_chgsharepremium,balancedividendspayable,grossprofitmargin,netprofitmargin,
       ebitdamargin,totalbankingdebttoebitda,netbankingdebttoebitda,totalliabilitiestototalequity,returnonassets,returnonequity,interestcoverage,
       currentratio,quickratio,fnc_year,creditcommitteedate,publish_date,approveddate, reference_date, entityid from olapts.rmtest_2
