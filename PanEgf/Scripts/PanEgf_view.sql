--------------------------view--------------------------

-- DROP VIEW olapts.panegftest_view;

CREATE OR REPLACE VIEW olapts.panegftest_view
 AS
   SELECT *
   FROM olapts.panegftest;

ALTER TABLE olapts.panegftest_view
    OWNER TO uniadmintrn;

select * from olapts.panegftest_view