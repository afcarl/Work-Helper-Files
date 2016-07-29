from transfer_classes import Hive_to_MySQL
from transfer_classes import Salesforce_to_MySQL


query = '''	        select b.customer_name as customer, a.cluster_uid, c.cluster_name, a.cluster_version
	        from
	          (
	        select *
	        from aiq_prod.cluster_versions
	        where ds ='2016-07-10'
	         ) a
	         left outer join
	         (
	           select customer_uid,customer_name
	           from aiq_prod.customers
	           where ds = '2016-07-10'
	         )b on a.customer_uid = b.customer_uid
	         left outer join
	         (
	           select cluster_uid, cluster_name
	           from aiq_prod.clusters
	           where ds =  '2016-07-10'
	          ) c on a.cluster_uid = c.cluster_uid'''
table_name = "TEST3"

i = Hive_to_MySQL()
# drivewearmodel_data = i.transfer_data(query, table_name)


# i = Drivewearmodel()
# model data method creates panadas dataframe of the query
drivewearmodel_data = i.modeldata(query , table_name)


# sf_instance = Salesforce_to_MySQL()
# 	# Pull Data In To Variable all_data
# # sf_instance.sf_dictionary = build_sf_dict()
# 	# Save SF Dictionary
# sf_instance.save_SF_mysql("sales_force_data_index")


