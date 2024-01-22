----------------------------------------------------------------------------------
--                         Run script in OLAP database 
----------------------------------------------------------------------------------

--------------------------------------------------------------
--                 VIEW: olapts.panegf_view
--------------------------------------------------------------

DROP VIEW IF EXISTS olapts.panegf_view;

CREATE OR REPLACE VIEW olapts.panegf_view AS
 SELECT * FROM olapts.panegf_report;

ALTER TABLE olapts.panegf_view OWNER TO olap;

--------------------------------------------------------------
--         VIEW: olapts.rmdb_view
--------------------------------------------------------------

DROP VIEW IF EXISTS olapts.rmdb_view;

CREATE OR REPLACE VIEW olapts.rmdb_view AS
 SELECT * FROM olapts.rmdb_report;

ALTER TABLE olapts.rmdb_view OWNER TO olap;

-----------------------------------
--Check if the views were created
-----------------------------------

--select * from olapts.panegf_view;
--select * from olapts.rmdb_view;


