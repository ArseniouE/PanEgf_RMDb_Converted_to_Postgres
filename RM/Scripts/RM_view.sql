--------------------------view--------------------------

-- DROP VIEW olapts.rmtest_view;

CREATE OR REPLACE VIEW olapts.rmtest_view
 AS
   SELECT *
   FROM olapts.rmtest;

ALTER TABLE olapts.rmtest_view
    OWNER TO uniadmintrn;

--select * from olapts.rmtest_view