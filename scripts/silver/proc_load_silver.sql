DELIMITER $$

CREATE PROCEDURE LoadSilverTables()
BEGIN
    -- ============================================
    -- Truncate and Load Data into crm_cust_info
    -- ============================================
    -- Truncating the target table to ensure a clean load
    TRUNCATE TABLE Silver.crm_cust_info;

    -- Inserting cleaned data from Bronze to Silver
    INSERT INTO Silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'N/A'
        END AS cst_marital_status,
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            ELSE 'N/A'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
        FROM crm_cust_info
    ) AS ranked
    WHERE rn = 1;

    -- ============================================
    -- Truncate and Load Data into crm_prd_info
    -- ============================================
    -- Truncating the target table to ensure a clean load
    TRUNCATE TABLE Silver.crm_prd_info;

    -- Inserting cleaned data from Bronze to Silver
    INSERT INTO Silver.crm_prd_info (
        prd_id,
        cat_id,            
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,        
        dwh_create_date     
    )
    SELECT  
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,  
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        IFNULL(prd_cost, 0) AS prd_cost,  
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,  
        DATE_SUB(
            CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE),
            INTERVAL 1 DAY
        ) AS prd_end_dt,  -- Subtracting 1 day from the next start date
        NOW() AS dwh_create_date  
    FROM crm_prd_info;

    -- ============================================
    -- Truncate and Load Data into crm_sales_details
    -- ============================================
    -- Truncating the target table to ensure a clean load
    TRUNCATE TABLE Silver.crm_sales_details;

    -- Inserting cleaned data from Bronze to Silver
    INSERT INTO Silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        dwh_create_date
    )
    SELECT  
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        
        -- Handling sls_order_dt transformation
        CASE 
            WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS CHAR)) != 8 THEN NULL 
            ELSE CAST(CAST(sls_order_dt AS CHAR) AS DATE)
        END AS sls_order_dt,
        
        -- Handling sls_ship_dt transformation
        CASE 
            WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS CHAR)) != 8 THEN NULL 
            ELSE CAST(CAST(sls_ship_dt AS CHAR) AS DATE)
        END AS sls_ship_dt,
        
        -- Handling sls_due_dt transformation
        CASE 
            WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS CHAR)) != 8 THEN NULL 
            ELSE CAST(CAST(sls_due_dt AS CHAR) AS DATE)
        END AS sls_due_dt,
        
        -- Handling sls_sales calculation
        CASE 
            WHEN sls_sales IS NULL 
                OR sls_sales <= 0 
                OR sls_sales != ABS(sls_quantity * sls_price) 
            THEN sls_quantity * ABS(sls_price)  
            ELSE sls_sales  
        END AS sls_sales,
        
        -- Handling sls_price calculation
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
            THEN sls_sales / NULLIF(sls_quantity, 0)  
            ELSE sls_price  
        END AS sls_price,
        
        sls_quantity,
        
        -- Add current timestamp for dwh_create_date
        NOW() AS dwh_create_date  
    FROM crm_sales_details;

    -- ============================================
    -- Truncate and Load Data into erp_cust_az12
    -- ============================================
    -- Truncating the target table to ensure a clean load
    TRUNCATE TABLE Silver.erp_cust_az12;

    -- Inserting cleaned data from Bronze to Silver
    INSERT INTO Silver.erp_cust_az12 (cid, bdate, gen)
    SELECT 
        CASE 
            WHEN LEFT(cid, 3) = 'NAS' THEN SUBSTRING(cid, 4, LENGTH(cid) - 3)
            ELSE cid
        END AS cid,
        CASE 
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(gen) = 'F' OR UPPER(gen) = 'FEMALE' THEN 'Female'
            WHEN UPPER(gen) = 'M' OR UPPER(gen) = 'MALE' THEN 'Male'
            ELSE 'N/A'
        END AS gen
    FROM Bronze.erp_cust_az12;

    -- ============================================
    -- Truncate and Load Data into erp_loc_a101
    -- ============================================
    -- Truncating the target table to ensure a clean load
    TRUNCATE TABLE Silver.erp_loc_a101;

    -- Inserting cleaned data from Bronze to Silver
    INSERT INTO Silver.erp_loc_a101 (cid, cntry)
    SELECT 
        REPLACE(cid, '-', '') AS cid,  -- Remove any dashes from cid
        CASE
            WHEN TRIM(cntry) IN ('', ' ', '   ') THEN 'N/A' 
            WHEN UPPER(TRIM(cntry)) IN ('USA', 'US') THEN 'United States'  
            WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'  
            WHEN UPPER(TRIM(cntry)) = 'GB' THEN 'United Kingdom' 
            WHEN UPPER(TRIM(cntry)) = 'FR' THEN 'France'  
            WHEN UPPER(TRIM(cntry)) = 'AU' THEN 'Australia'
            ELSE TRIM(cntry)  
        END AS normalized_cntry
    FROM Bronze.erp_loc_a101;

    -- ============================================
    -- Truncate and Load Data into erp_px_cat_g1v2
    -- ============================================
    -- Truncating the target table to ensure a clean load
    TRUNCATE TABLE Silver.erp_px_cat_g1v2;

    -- Inserting cleaned data from Bronze to Silver
    INSERT INTO Silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance
    FROM Bronze.erp_px_cat_g1v2;

END$$

DELIMITER ;
