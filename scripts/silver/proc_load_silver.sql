Create or alter procedure silver.load_silver  as
begin 
	truncate table silver.crm_cust_info; 
	insert into silver.crm_cust_info (
		   [cst_id]
		  ,[cst_key]
		  ,[cst_firstname]
		  ,[cst_lastname]
		  ,[cst_marital_status]
		  ,[cst_gndr]
		  ,[cst_create_date]
		  )

	select 
	cst_id, 
	cst_key, 
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	case when upper(cst_marital_status) = 'S' then 'Single'
 		 when UPPER(cst_marital_status) ='M' then 'Married' 
		 else 'n/a' end 
		 cst_marital_status,
	case when upper(cst_gndr) = 'F' then 'Female'
 		 when UPPER(cst_gndr) ='M' then 'Male' 
		 else 'n/a' end cst_gndr,
	cst_create_date
	from (
		select
			* ,
			ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info  
			where cst_id is not null
		) t where flag_last=1;

 -------------Crm_prd_info-----------------------
	truncate table silver.crm_prd_info; 
	insert into silver.[crm_prd_info](
		prd_id,cat_id,prd_key, prd_nm, prd_cost,prd_line,prd_start_dt,prd_end_dt
	)
	SELECT 
		   [prd_id]
		  ,REPLACE(substring(prd_key,1,5), '-','_') as cat_id
		  ,SUBSTRING(prd_key,7,len(prd_key))as prd_key
		  ,[prd_nm]
		  ,isnull([prd_cost],0) as [prd_cost]
		  ,case upper(trim(prd_line))
				when  'M' then 'Mountain'
				when  'R' then 'Road'
				when  'S' then 'Other Sales'
				when  'T' then 'Touring'
			else 'n/a' end prd_line
		  ,[prd_start_dt]
		  ,DATEADD(day, -1, lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt
		 -- ,[dwh_create_date]
	  FROM [DataWarehouse].bronze.[crm_prd_info]

	  ---------crm_sales_details----------
		  truncate table silver.crm_sales_details;
	insert into silver.crm_sales_details (
		   [sls_ord_num]
		  ,[sls_prd_key]
		  ,[sls_cust_id]
		  ,[sls_order_dt]
		  ,[sls_ship_dt]
		  ,[sls_due_dt]
		 , [sls_sales] 	
		  ,[sls_quantity]
		  ,sls_price
	)
	SELECT [sls_ord_num]
		  ,[sls_prd_key]
		  ,[sls_cust_id]
		  , case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null 
				else cast(cast(sls_order_dt as varchar) as date) end [sls_order_dt]
		  ,case when [sls_ship_dt] = 0 or len([sls_ship_dt]) !=8 then null 
				else cast(cast([sls_ship_dt] as varchar) as date) end [sls_ship_dt]
		  ,case when [sls_due_dt] = 0 or len([sls_due_dt]) !=8 then null 
				else cast(cast([sls_due_dt] as varchar) as date) end [sls_due_dt]
		  ,
		  case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
					then sls_quantity * abs(sls_price) else sls_sales end [sls_sales] 	
		  ,[sls_quantity]
		  ,case when sls_price is null or sls_price <=0 then sls_sales / nullif(sls_quantity,0) 
					else sls_price end sls_price
	  FROM [DataWarehouse].[bronze].[crm_sales_details]

	------------erp_cust_az12----------------------
	truncate table silver.erp_cust_az12
	insert into silver.erp_cust_az12 (cid,bdate,gen)
	select
	case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
			else cid 
			end cid ,
			case when bdate > GETDATE() then null 
			   else bdate end 
	bdate,
	CASE  
		WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
		when upper(trim(gen)) in ('M', 'MALE') THEN 'Male'
		else 'n/a' end gen 
	from bronze.erp_cust_az12
	where bdate <'1924-01-01' or bdate > GETDATE()

	--------erp_px_cat_g1v2---------------
	truncate table silver.erp_px_cat_g1v2;

	insert into silver.erp_px_cat_g1v2(
	id, 
	cat,
	subcat,
	mainteance)
	select 
	id, 
	cat,
	subcat,
	mainteance
	from bronze.erp_px_cat_g1v2;
	-----------erp_loc_a101-------------------------
	truncate table silver.erp_loc_a101;
	insert into silver.erp_loc_a101(cid, cntry)
	SELECT 
	REPLACE (cid,'-','')  cid  ,
	case when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US','USA') THEN 'United States'
		when trim (cntry)='' OR cntry is null then 'n/a'
		else trim (cntry) end 
	[cntry]
	FROM [DataWarehouse].bronze.[erp_loc_a101]

end 


