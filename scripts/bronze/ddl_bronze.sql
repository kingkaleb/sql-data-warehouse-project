create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE();
			print '=========================='
			print 'Loading Bronze layer'
			print '=========================='
			truncate table bronze.crm_cust_info; 

			print '---------------------------------'
			print 'Loading CRM Tables '
			print '---------------------------------'

			set @start_time = GETDATE();
			truncate table bronze.crm_cust_info; 
			bulk insert bronze.crm_cust_info
			from 'C:\DW_Project\Sourcces\datasets\source_crm\cust_info.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time = GETDATE();
			print 'CRM_CUST_INFO >> Load Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

			set @start_time = GETDATE();
			truncate table bronze.crm_prd_info; 
			bulk insert bronze.crm_prd_info
			from 'C:\DW_Project\Sourcces\datasets\source_crm\prd_info.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time= GETDATE();
			print 'CRM_PRD_INFO >> Load Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
 
			set @start_time=GETDATE();
			truncate table bronze.crm_sales_details; 
			bulk insert bronze.crm_sales_details
			from 'C:\DW_Project\Sourcces\datasets\source_crm\sales_details.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time = GETDATE()
			print 'CRM_SALES_INFO >> Load Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

 
			print '---------------------------------'
			print 'Loading ERP Tables '
			print '---------------------------------'

			set @start_time = GETDATE();
			truncate table bronze.erp_cust_az12; 
			bulk insert bronze.erp_cust_az12
			from 'C:\DW_Project\Sourcces\datasets\source_erp\CUST_AZ12.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time = GETDATE();
			print 'ERP_CUST_AZ12_INFO >> Load Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

			set @start_time = GETDATE();
			truncate table bronze.erp_LOC_A101; 

			bulk insert bronze.erp_LOC_A101
			from 'C:\DW_Project\Sourcces\datasets\source_erp\LOC_A101.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time = GETDATE();
 			print 'ERP_LOC_A101_INFO >> Load Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';

			set @start_time = GETDATE();
			truncate table bronze.erp_PX_CAT_G1V2; 
			bulk insert bronze.erp_PX_CAT_G1V2
			from 'C:\DW_Project\Sourcces\datasets\source_erp\PX_CAT_G1V2.csv'
			with (
				firstrow=2,
				fieldterminator=',',
				tablock
			);
			set @end_time = GETDATE();
			print 'ERP_PX_CAT_G1V2_INFO >> Load Duration : ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds';
			print '=============================='
			print 'Loading Bronze layer Finished'
			print '=============================='
	set @batch_end_time = GETDATE();
	print 'Total Load Duration : ' + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + ' seconds';

	End try
	begin catch
			print '=============================='
			print 'Error has occuard wile loading'
			print 'Error message' + Error_message();
			print '=============================='
	end catch
	
end;
