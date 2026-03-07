-- Reference
--   - https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply

-- ## Apply a row filter

-- Define the row filter function
CREATE FUNCTION us_filter(region STRING)
RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, region='US');
-- Apply the function to a table as a row filter
CREATE TABLE sales(region STRING, id INT);
ALTER TABLE sales SET ROW FILTER us_filter ON (region);
-- Disable the row filter
ALTER TABLE sales DROP ROW FILTER:

-- ## Apply a column mask

-- Define the column mask function
CREATE FUNCTION ssn_mask(ssn STRING)
RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('HumanResourceDept')
    THEN ssn
    ELSE '***-**-****'
END;
-- Apply the function to a table as a column filter at the creation
CREATE TABLE users (name STRING, ssn STRING MASK ssn_mask);
-- Apply the function to a table as a column filter after the creation
CREATE TABLE users (name STRINGm ssn STRING);
ALTER TABLE users ALTER COLUMN ssn SET MASK ssn_mask;

-- ## Column mask with Python UDF

-- Create the Python UDF with masking logic
CREATE OR REPLACE FUNCTION email_mask_python(email STRING)
RETURN STRING
LANGUAGE PYTHON
AS $$
import re
return re.sub(r'^[^@]+', lambda m: '*' * len(m.group()), email)
$$;
-- Create a SQL wrapper function that calls the Python UDF
CREATE OR REPLACE FUNCTION email_mask_sql(email STRING)
RETURN email_mask_python(email);
-- Create a table and apply the SQL wrapper function as the colum mask
CREATE TABLE contacts (
    name STRING,
    email STRING MASK email_mask_sql
);

