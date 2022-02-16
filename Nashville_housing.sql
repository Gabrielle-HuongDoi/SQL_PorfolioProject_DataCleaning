/* 
CLEANING DATA - NASHVILLE HOUSING DATA
*/

############################################################
-- IMPORT DATA
############################################################

CREATE DATABASE Nashville;
USE Nashville;

-- Create table
DROP TABLE IF EXISTS nashville;
CREATE TABLE nashville (
unique_ID VARCHAR(10),
parcel_ID VARCHAR(20),
land_use VARCHAR(50),
address VARCHAR(50),
sale_date VARCHAR(20),
sale_price INT,
legal_reference VARCHAR(20),
sold_as_vacant VARCHAR(3),
owner_name VARCHAR(100),
owner_address VARCHAR(50),
acreage FLOAT,
tax_district VARCHAR(50),
land_value INT,
building_value INT,
total_value INT,
year_built SMALLINT,
bedrooms TINYINT,
full_bath TINYINT,
half_bath TINYINT
);

-- Import data. 
SET GLOBAL local_infile = 1;
LOAD DATA LOCAL INFILE "E:/material/Project/SQL/Cleaning_data/Nashville Housing Data for Data Cleaning.csv" 
INTO TABLE nashville
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(unique_ID, parcel_ID, land_use, @address, sale_date, sale_price, legal_reference, sold_as_vacant, @owner_name, @owner_address, @acreage,
@tax_district, @land_value, @building_value, @total_value, @year_built, @bedrooms, @full_bath, @half_bath)
SET address = NULLIF(@address, ''),
	owner_name = NULLIF(@owner_name, ''),
	owner_address = NULLIF(@owner_address, ''),
	acreage = NULLIF(@acreage, ''),
    tax_district = NULLIF(@tax_district, ''),
    land_value = NULLIF(@land_value, ''),
    building_value = NULLIF(@building_value, ''),
    total_value = NULLIF(@total_value, ''),
    year_built = NULLIF(@year_built, ''),
    bedrooms = NULLIF(@bedrooms, ''),
    full_bath = NULLIF(@full_bath, ''),
    half_bath = NULLIF(@full_bath, '');
    


############################################################
-- CLEANING DATA
############################################################

# 1. CONVERT DATE FORMAT
ALTER TABLE nashville
ADD date_convert DATE;

UPDATE nashville
SET date_convert = STR_TO_DATE(sale_date, '%M %d, %Y');

#------------------------------------------------------------------


# 2. POPULATE PROPERTY ADDRESS

-- There are some properties that miss the address information
SELECT *
FROM nashville
WHERE address IS NULL
ORDER BY parcel_ID;

-- If the address is missing, check other properties which have the same parcel_ID to get the equivalent address
SELECT a.unique_ID AS aUniqueID, a.parcel_ID AS aParcelID, a.address AS aAddress, 
		b.unique_ID AS bUniqueID, b.parcel_ID AS bParcelID, b.address AS bAddress, IFNULL(a.address, b.address) AS address_filling
FROM nashville a JOIN nashville b 
ON a.parcel_ID = b.parcel_ID AND a.unique_ID <> b.unique_ID
WHERE a.address IS NULL;

-- Based on previous results, update missing address into Nashville table
UPDATE nashville a
INNER JOIN nashville b ON (a.parcel_ID = b.parcel_ID AND a.unique_ID <> b.unique_ID)
SET a.address = IFNULL(a.address, b.address)
WHERE a.address IS NULL;

#----------------------------------------------------------------------------


# 3. BREAK DOWN ADDRESS INTO INDIVIDUAL COLUMNS (ADDRESS, CITY, STATE)

-- Note: the address column now is represented as, for example, '1808  FOX CHASE DR, GOODLETTSVILLE'
-- Or for the owner address, it includes the state as '1808  FOX CHASE DR, GOODLETTSVILLE, TN'
SELECT address, owner_address
FROM nashville
LIMIT 10;

-- Seperate the property address
SELECT address, SUBSTRING(address, 1, LOCATE(',', address) - 1) AS Address, 
		SUBSTRING(address, LOCATE(',', address) + 2, LENGTH(address)) AS City
FROM nashville
LIMIT 10;

-- Update seperated property address into Nashville table
ALTER TABLE nashville
ADD Address_format VARCHAR(50) AFTER address,
ADD City VARCHAR(50) AFTER Address_format;

UPDATE nashville
SET Address_format = SUBSTRING(address, 1, LOCATE(',', address) - 1),
	City = SUBSTRING(address, LOCATE(',', address) + 2, LENGTH(address));
    

-- Similarly, seperate owner address
SELECT owner_address, LEFT(owner_address, LOCATE(',', owner_address) - 1) AS owner_add,
		SUBSTRING_INDEX(SUBSTRING_INDEX(owner_address, ',', 2), ',', -1 ) AS owner_city,
        SUBSTRING_INDEX(owner_address, ',', -1) AS owner_state
FROM nashville
LIMIT 100, 150;

-- Update owner address into Nashville table
ALTER TABLE nashville
ADD owner_add VARCHAR(50),
ADD owner_city VARCHAR(50),
ADD owner_state VARCHAR(10);

UPDATE nashville
SET owner_add = LEFT(owner_address, LOCATE(',', owner_address) - 1),
	owner_city = SUBSTRING_INDEX(SUBSTRING_INDEX(owner_address, ',', 2), ',', -1 ),
    owner_state = SUBSTRING_INDEX(owner_address, ',', -1);

#----------------------------------------------------------------------------------------



# 3. REMOVE DUPLICATES

-- Duplicate entries
WITH duplicates
AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY parcel_ID, address, sale_price, sale_date, legal_reference 
							ORDER BY unique_ID) row_num
	FROM nashville
    )
SELECT *
FROM duplicates
WHERE row_num > 1;

-- Delete duplicated rows
DELETE FROM nashville
WHERE unique_ID IN (WITH duplicates
					AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY parcel_ID, address, sale_price, sale_date, legal_reference 
														ORDER BY unique_ID) row_num
						FROM nashville)
					SELECT unique_ID
					FROM duplicates
					WHERE row_num > 1
					);
                    
#-------------------------------------------------------------------------------



# 4. REMOVE UNNECESSARY COLUMNS AND TRIM WHITE SPACES IN ADDRESS RELATED INFORMATION

-- Drop unused columns
ALTER TABLE nashville
DROP COLUMN address, 
DROP COLUMN owner_address, 
DROP COLUMN tax_district, 
DROP COLUMN sale_date;

SHOW COLUMNS IN nashville;

-- Trim white spaces
UPDATE nashville
SET Address_format = TRIM(Address_format),
	City = TRIM(City);

UPDATE nashville
SET owner_add = TRIM(owner_add),
	owner_city = TRIM(owner_city),
    owner_state = TRIM(owner_state);

#----------------------------------------------------------------------------------------------------------------------