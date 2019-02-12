-- Table to hold data from API call
CREATE TABLE CountryIncome (
  CountryId VARCHAR(3) NOT NULL,
  CountryISOCode VARCHAR(2) NULL,
  CountryName VARCHAR(100) NULL,
  CapitalCity VARCHAR(200) NULL,
  RegionId VARCHAR(10) NULL,
  RegionName VARCHAR(50) NULL,
  IncomeLevelId VARCHAR (10) NULL,
  IncomeLevel VARCHAR(50) NULL
);

-- Table to hold the data from GDP CSV
CREATE TABLE GEPData (
  SeriesName VARCHAR(100) NOT NULL,
  SeriesCode VARCHAR(100) NOT NULL,
  CountryName VARCHAR(100) NOT NULL,
  CountryCode	VARCHAR(3) NOT NULL,
  "2000"	VARCHAR(50) NULL,
  "2001"	VARCHAR(50) NULL,
  "2002"	VARCHAR(50) NULL,
  "2003"	VARCHAR(50) NULL,
  "2004"	VARCHAR(50) NULL,
  "2005"	VARCHAR(50) NULL,
  "2006"	VARCHAR(50) NULL,
  "2007"	VARCHAR(50) NULL,
  "2008"	VARCHAR(50) NULL,
  "2009"	VARCHAR(50) NULL,
  "2010"	VARCHAR(50) NULL,
  "2011"	VARCHAR(50) NULL,
  "2012"	VARCHAR(50) NULL,
  "2013"	VARCHAR(50) NULL,
  "2014"	VARCHAR(50) NULL,
  "2015"	VARCHAR(50) NULL
);

-- Modelled Data Table
CREATE TABLE GDP (
    CountryCode VARCHAR(3) NOT NULL,
    Year INT NOT NULL,
    GDPAmount NUMERIC(20,5) NULL
);

-- Take the loaded CSV table and convert from columns to rows on years for easier modelling
INSERT INTO GDP
  (CountryCode, Year, GDPAmount)
SELECT
  CountryCode,
  Year,
  CASE WHEN GDPAmount = '..' THEN '0' ELSE GDPAmount END::NUMERIC(20,5) as GDPAmount -- Replace the '..' with Zeros
FROM (
       SELECT CountryCode,
              unnest(
                  array ['2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011','2012','2013','2014','2015']):: INT as Year,
              unnest(
                  array ["2000","2001","2002","2003","2004","2005","2006","2007","2008","2009","2010","2011","2012","2013","2014","2015"])       as GDPAmount
       FROM GEPData
       WHERE SeriesName = 'GDP (current US$)'
) as unpvt;

/*
Answers to analysis
*/

-- Q1
SELECT
  CountryId,
  CountryName,
  IncomeLevel
FROM CountryIncome
WHERE IncomeLevel = 'Upper middle income';

--Q2
SELECT
  RegionName,
  CountryId,
  CountryName,
  IncomeLevel
FROM CountryIncome
WHERE IncomeLevel = 'Low income'
ORDER BY RegionName, CountryName;

-- Q3
SELECT RegionName,
  COUNT(*)::NUMERIC(7,2) AS TotalCountries,
  COUNT(*) FILTER (WHERE IncomeLevel = 'High income')::NUMERIC(7,2) AS HighIncomeCountries,
  COUNT(*) FILTER (WHERE IncomeLevel = 'High income')::NUMERIC(7,2) / COUNT(*)::NUMERIC(7,2) AS HighIncomePct
FROM CountryIncome
GROUP BY RegionName
ORDER BY HighIncomePct DESC
LIMIT 1;

-- Q4
SELECT
  RegionName,
  CountryName,
  GDPAmount,
  SUM(GDPAmount) OVER (PARTITION BY RegionName ORDER BY  GDPAmount ASC, CountryName ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM CountryIncome
INNER JOIN GDP
  ON CountryCode = CountryId
WHERE Year = 2015;

-- Q5
SELECT
  RegionName,
  CountryName,
  Year,
  GDPAmount,
  CASE WHEN LAG(GDPAmount, 1,0.0) OVER (PARTITION BY CountryName ORDER BY Year ASC) = 0.00 THEN 0.0
       ELSE (GDPAmount - LAG(GDPAmount, 1,0.0) OVER (PARTITION BY CountryName ORDER BY Year ASC)) / LAG(GDPAmount, 1,0.0) OVER (PARTITION BY CountryName ORDER BY Year ASC)
  END GrowthPercentage
FROM CountryIncome
INNER JOIN GDP
  ON CountryCode = CountryId;

-- Q6
SELECT
  RegionName,
  CountryName,
  GDPAmount,
  GDPRank
FROM (
       SELECT RegionName,
              CountryName,
              GDPAmount,
              ROW_NUMBER() OVER (PARTITION BY RegionName ORDER BY GDPAmount ASC) AS GDPRank
       FROM CountryIncome
              INNER JOIN GDP
                         ON CountryCode = CountryId
       WHERE Year = '2015'
) as b
WHERE b.GDPRank <=3