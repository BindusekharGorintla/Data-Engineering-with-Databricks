----------------------------------------------------
-- SOLUTION: SPARK DECLARATIVE PIPELINE
----------------------------------------------------


--------------------
-- CSV -> BRONZE
--------------------
CREATE OR REFRESH STREAMING TABLE lab_1_bronze_db.employees_bronze_lab4_solution -- Modified to ST
AS
SELECT 
  *,
  current_timestamp() AS ingestion_time,
  _metadata.file_name as raw_file_name
FROM STREAM read_files(            -- Add STREAM         
  '${source}',                     -- Configuration parameter to data source files
  format => 'CSV',
  header => 'true'
);


--------------------
-- BRONZE -> SILVER
--------------------
CREATE OR REFRESH STREAMING TABLE lab_2_silver_db.employees_silver_lab4_solution -- Modified to ST
(
  CONSTRAINT check_country EXPECT (Country IN ('US','GR')),
  CONSTRAINT check_salary EXPECT (Salary > 0),
  CONSTRAINT check_null_id EXPECT (EmployeeID IS NOT NULL) ON VIOLATION DROP ROW
)
AS
SELECT
  EmployeeID,
  FirstName,
  upper(Country) AS Country,
  Department,
  Salary,
  HireDate,
  date_format(HireDate, 'MMMM') AS HireMonthName,
  year(HireDate) AS HireYear, 
  Operation
FROM STREAM lab_1_bronze_db.employees_bronze_lab4_solution;    -- Add STREAM keyword



--------------------
-- SILVER -> GOLD
--------------------

-- MV 1 
CREATE OR REFRESH MATERIALIZED VIEW lab_3_gold_db.employees_by_country_gold_lab4_solution -- Modified to MV
AS
SELECT 
  Country,
  count(*) AS TotalEmployees,
  sum(Salary) AS TotalSalary
FROM lab_2_silver_db.employees_silver_lab4_solution
GROUP BY Country;

-- MV 2
CREATE OR REFRESH MATERIALIZED VIEW lab_3_gold_db.salary_by_department_gold_lab4_solution  -- Modified to MV
AS
SELECT
  Department,
  sum(Salary) AS TotalSalary
FROM lab_2_silver_db.employees_silver_lab4_solution
GROUP BY Department;