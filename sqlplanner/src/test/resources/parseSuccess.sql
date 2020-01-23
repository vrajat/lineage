-- ParserTest:sanityTest
select 1 from tbl
-- ParserTest:select all
select * from volt_tt_576f619a83feb
-- ParserTest: escaped new line
select\n* from x
-- ParserTest: three line
select x
  from y
  where z > 10
-- ParserTest: two line
select x
  from y
-- ParserTest: semi-colon
select x
  from y;
