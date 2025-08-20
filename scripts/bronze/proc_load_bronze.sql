-- Log table creation
CREATE TABLE IF NOT EXISTS bronze_load_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    run_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20),
    message TEXT
);

DELIMITER $$

CREATE PROCEDURE bronze.load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;
    
    -- Declare a custom handler for error logging
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        INSERT INTO bronze_load_log (status, message)
        VALUES ('FAILED', CONCAT('Error occurred during CSV load: ', ERROR_MESSAGE()));
    END;

    -- Start the transaction
    START TRANSACTION;
    SET batch_start_time = NOW();
    
    -- General logging function
    CREATE TEMPORARY PROCEDURE log_message(IN msg VARCHAR(255))
    BEGIN
        INSERT INTO bronze_load_log (status, message) VALUES ('INFO', msg);
    END;

    -- Begin loading process
    CALL log_message('================================================');
    CALL log_message('Loading Bronze Layer');
    CALL log_message('================================================');

    -- Table information
    DECLARE table_list CURSOR FOR 
        SELECT 'bronze.crm_cust_info', '/Users/Mac/Desktop/source_crm/cust_info.csv', 
               'cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date'
        UNION ALL
        SELECT 'bronze.crm_prd_info', '/Users/Mac/Desktop/source_crm/prd_info.csv',
               'prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt'
        UNION ALL
        SELECT 'bronze.crm_sales_details', '/Users/Mac/Desktop/source_crm/sales_details.csv',
               'sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price'
        UNION ALL
        SELECT 'bronze.erp_loc_a101', '/Users/Mac/Desktop/source_erp/loc_a101.csv',
               'cid, cntry'
        UNION ALL
        SELECT 'bronze.erp_cust_az12', '/Users/Mac/Desktop/source_erp/cust_az12.csv',
               'cid, bdate, gen'
        UNION ALL
        SELECT 'bronze.erp_px_cat_g1v2', '/Users/Mac/Desktop/source_erp/px_cat_g1v2.csv',
               'id, cat, subcat, maintenance';
    
    -- Loop through tables and load data
    DECLARE done INT DEFAULT 0;
    DECLARE table_name VARCHAR(255);
    DECLARE file_path VARCHAR(255);
    DECLARE columns VARCHAR(255);

    OPEN table_list;
    read_loop: LOOP
        FETCH table_list INTO table_name, file_path, columns;
        IF done THEN
            LEAVE read_loop;
        END IF;

        -- Truncate table
        SET start_time = NOW();
        CALL log_message(CONCAT('>> Truncating Table: ', table_name));
        SET @truncate_sql = CONCAT('TRUNCATE TABLE ', table_name);
        PREPARE stmt FROM @truncate_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        -- Insert Data
        CALL log_message(CONCAT('>> Inserting Data Into: ', table_name));
        SET @load_sql = CONCAT('LOAD DATA LOCAL INFILE "', file_path, '" INTO TABLE ', table_name, 
            ' FIELDS TERMINATED BY "," ENCLOSED BY "\\" LINES TERMINATED BY "\\n" IGNORE 1 ROWS (', columns, ')');
        PREPARE stmt FROM @load_sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        SET end_time = NOW();
        CALL log_message(CONCAT('>> Load Duration: ', TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds'));
        CALL log_message('>> -------------');
    END LOOP;
    
    CLOSE table_list;

    -- Commit if everything succeeds
    COMMIT;

    SET batch_end_time = NOW();
    CALL log_message('==========================================');
    CALL log_message('Loading Bronze Layer is Completed');
    CALL log_message(CONCAT('Total Load Duration: ', TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds'));
    CALL log_message('==========================================');
    
END$$

DELIMITER ;
