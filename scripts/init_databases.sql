/*
=============================================================
Create Databases for Medallion Architecture
=============================================================
Script Purpose:
    This script drops and recreates three databases named 'Bronze', 'Silver', and 'Gold'. 
    These databases represent the layers in a Medallion architecture:
        - Bronze: Raw/ingested data
        - Silver: Cleaned/transformed data
        - Gold: Aggregated/business-ready data

WARNING:
    Running this script will drop the existing databases if they exist. 
    All data in these databases will be permanently deleted. Proceed with caution.
*/

-- Drop and recreate Bronze database
DROP DATABASE IF EXISTS Bronze;
CREATE DATABASE Bronze;

-- Drop and recreate Silver database
DROP DATABASE IF EXISTS Silver;
CREATE DATABASE Silver;

-- Drop and recreate Gold database
DROP DATABASE IF EXISTS Gold;
CREATE DATABASE Gold;


-- USE Bronze;
-- USE Silver;
-- USE Gold;
