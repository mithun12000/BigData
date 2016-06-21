medicare_records = LOAD '/user/kgarner/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt' USING PigStorage('\t')
	AS (npi, nppes_provider_last_org_name, nppes_provider_first_name,
	nppes_provider_mi, nppes_credentials, nppes_provider_gender,
	nppes_entity_code, nppes_provider_street1, nppes_provider_street2,
	nppes_provider_city, nppes_provider_zip, nppes_provider_state,
	nppes_provider_country, provider_type, medicare_participation_indicator,
	place_of_service, hcpcs_code, hcpcs_description, line_srvc_cnt,
	bene_unique_cnt, bene_day_srvc_cnt, average_medicare_allowed_amt, stdev_medicare_allowed_amt,
	average_submitted_chrg_amt, stdev_submitted_chrg_amt, average_medicare_payment_amt,
	stdev_medicare_payment_amt);
	
physician_records = LOAD '/user/kgarner/OPPR_ALL_DTL_GNRL_12192014.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage()
	AS (general_transaction_id, Program_Year, payment_publication_date,
	submitting_applicable_manufacturer_or_applicable_gpo_name, covered_recipient_type, teaching_hospital_id,
	teaching_hospital_name, physician_profile_id, physician_first_name,
	physician_middle_name, physician_last_name, physician_name_suffix,
	recipient_primary_business_street_address_line1, recipient_primary_business_street_address_line2, recipient_city,
	recipient_state, recipient_zip_code, recipient_country, recipient_province,
	recipient_postal_code, physician_primary_type, physician_specialty, physician_license_state_code1,
	physician_license_state_code2, physician_license_state_code3, physician_license_state_code4,
	physician_license_state_code5, product_indicator, name_of_associated_covered_drug_or_biological1,
	name_of_associated_covered_drug_or_biological2, name_of_associated_covered_drug_or_biological3,
	name_of_associated_covered_drug_or_biological4, name_of_associated_covered_drug_or_biological5,
	ndc_of_associated_covered_drug_or_biological1, ndc_of_associated_covered_drug_or_biological2,
	ndc_of_associated_covered_drug_or_biological3, ndc_of_associated_covered_drug_or_biological4,
	ndc_of_associated_covered_drug_or_biological5, name_of_associated_covered_device_or_medical_supply1,
	name_of_associated_covered_device_or_medical_supply2, name_of_associated_covered_device_or_medical_supply3,
	name_of_associated_covered_device_or_medical_supply4, name_of_associated_covered_device_or_medical_supply5,
	applicable_manufacturer_or_applicable_gpo_making_payment_name, applicable_manufacturer_or_applicable_gpo_making_payment_id,
	applicable_manufacturer_or_applicable_gpo_making_payment_state, applicable_manufacturer_or_applicable_gpo_making_payment_country,
	dispute_status_for_publication, total_amount_of_payment_usdollars, date_of_payment, number_of_payments_included_in_total_amount,
	form_of_payment_or_transfer_of_value, nature_of_payment_or_transfer_of_value, city_of_travel, state_of_travel, country_of_travel,
	physician_ownership_indicator, third_party_payment_recipient_indicator, name_of_third_party_entity_receiving_payment_or_transfer_of_value,
	charity_indicator, third_party_equals_covered_recipient_indicator, contextual_information, delay_in_publication_of_general_payment_indicator);

						-- Medicare records 
filtered_medicare = FOREACH(FILTER medicare_records BY hcpcs_code == '93015')
					GENERATE nppes_provider_street1, nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;		

medicare_hash = FOREACH filtered_medicare
				 GENERATE REPLACE(nppes_provider_street1, '#', '') as nppes_provider_street1, REPLACE(nppes_provider_street2, '#', '') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_periods = FOREACH 	medicare_hash GENERATE REPLACE(nppes_provider_street1,'\\.','') as nppes_provider_street1, REPLACE(nppes_provider_street2,'\\.','') as nppes_provider_street2, REPLACE(nppes_provider_city, '\\.', '') as nppes_provider_city, REPLACE(nppes_provider_state, '\\.', '') as nppes_provider_state, average_submitted_chrg_amt;		

medicare_space = FOREACH medicare_periods
				 GENERATE REPLACE(nppes_provider_street1, '\\s+', ' ') as nppes_provider_street1, REPLACE(nppes_provider_street2, '\\s+', ' ') as nppes_provider_street2, REPLACE(nppes_provider_city, '\\s+', ' ') as nppes_provider_city, REPLACE(nppes_provider_state, '\\s+', ' ') as nppes_provider_state, average_submitted_chrg_amt;
					
medicare_trim_spaces = FOREACH medicare_space
					GENERATE TRIM(nppes_provider_street1) as nppes_provider_street1, TRIM(nppes_provider_street2) as nppes_provider_street2, TRIM(nppes_provider_city) as nppes_provider_city, TRIM(nppes_provider_state) as nppes_provider_state, average_submitted_chrg_amt;

			
medicare_upper = FOREACH medicare_trim_spaces
					GENERATE UPPER(nppes_provider_street1) as nppes_provider_street1, UPPER(nppes_provider_street2) as nppes_provider_street2, UPPER(nppes_provider_city) as nppes_provider_city, UPPER(nppes_provider_state) as nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_ste = FOREACH medicare_upper GENERATE REPLACE(nppes_provider_street1,'SUITE','STE') as nppes_provider_street1, REPLACE(nppes_provider_street2,'SUITE','STE') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;
					
medicare_abb_st = FOREACH 	medicare_abb_ste GENERATE REPLACE(nppes_provider_street1,'STREET','ST') as nppes_provider_street1, REPLACE(nppes_provider_street2,'STREET','ST') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_blvd = FOREACH medicare_abb_st GENERATE REPLACE(nppes_provider_street1,'BOULEVARD','BLVD') as nppes_provider_street1, REPLACE(nppes_provider_street2,'BOULEVARD','BLVD') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_rd = FOREACH 	medicare_abb_blvd GENERATE REPLACE(nppes_provider_street1,'ROAD','RD') as nppes_provider_street1, REPLACE(nppes_provider_street2,'ROAD','RD') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_apt = FOREACH 	medicare_abb_rd GENERATE REPLACE(nppes_provider_street1,'APARTMENT','APT') as nppes_provider_street1, REPLACE(nppes_provider_street2,'APARTMENT','APT') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_ave = FOREACH 	medicare_abb_apt GENERATE REPLACE(nppes_provider_street1,'AVENUE','AVE') as nppes_provider_street1, REPLACE(nppes_provider_street2,'AVENUE','AVE') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_bldg = FOREACH medicare_abb_ave GENERATE REPLACE(nppes_provider_street1,'BUILDING','BLDG') as nppes_provider_street1, REPLACE(nppes_provider_street2,'BUILDING','BLDG') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_dept = FOREACH medicare_abb_bldg GENERATE REPLACE(nppes_provider_street1,'DEPARTMENT','DEPT') as nppes_provider_street1, REPLACE(nppes_provider_street2,'DEPARTMENT','DEPT') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_ln = FOREACH medicare_abb_dept GENERATE REPLACE(nppes_provider_street1,'LANE','LN') as nppes_provider_street1, REPLACE(nppes_provider_street2,'LANE','LN') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_plz = FOREACH medicare_abb_ln GENERATE REPLACE(nppes_provider_street1,'PLAZA','PLZ') as nppes_provider_street1, REPLACE(nppes_provider_street2,'PLAZA','PLZ') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_rdg = FOREACH medicare_abb_plz GENERATE REPLACE(nppes_provider_street1,'RIDGE','RDG') as nppes_provider_street1, REPLACE(nppes_provider_street2,'RIDGE','RDG') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_dr = FOREACH medicare_abb_rdg GENERATE REPLACE(nppes_provider_street1,'DRIVE','DR') as nppes_provider_street1, REPLACE(nppes_provider_street2,'DRIVE','DR') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_pkwy = FOREACH medicare_abb_dr GENERATE REPLACE(nppes_provider_street1,'PARKWAY','PKWY') as nppes_provider_street1, REPLACE(nppes_provider_street2,'PARKWAY','PKWY') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_vly = FOREACH medicare_abb_pkwy GENERATE REPLACE(nppes_provider_street1,'VALLY','VLY') as nppes_provider_street1, REPLACE(nppes_provider_street2,'VALLEY','VLY') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

medicare_abb_pl = FOREACH medicare_abb_vly GENERATE REPLACE(nppes_provider_street1,'PLACE','PL') as nppes_provider_street1, REPLACE(nppes_provider_street2,'PLACE','PL') as nppes_provider_street2, nppes_provider_city, nppes_provider_state, average_submitted_chrg_amt;

grouped_filtered_medicare = FOREACH(GROUP medicare_abb_pl BY (nppes_provider_street1, nppes_provider_street2, nppes_provider_city, nppes_provider_state) PARALLEL 1)
					GENERATE FLATTEN(group), AVG(medicare_abb_pl.average_submitted_chrg_amt) AS medicare_billings;


					-- Pharmaceutical records
physician_hash = FOREACH physician_records
				 GENERATE REPLACE(recipient_primary_business_street_address_line1, '#', '') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2, '#', '') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;
					
physician_periods = FOREACH physician_hash GENERATE REPLACE(recipient_primary_business_street_address_line1, '\\.', '') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2, '\\.', '') as recipient_primary_business_street_address_line2, REPLACE(recipient_city, '\\.', '') as recipient_city, REPLACE(recipient_state, '\\.', '') as recipient_state, total_amount_of_payment_usdollars;													
					
physician_space = FOREACH physician_periods
					GENERATE REPLACE(recipient_primary_business_street_address_line1, '\\s+', ' ') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2, '\\s+', ' ') as recipient_primary_business_street_address_line2, REPLACE(recipient_city, '\\s+', ' ') as recipient_city, REPLACE(recipient_state, '\\s+', ' ') as recipient_state, total_amount_of_payment_usdollars;			

physician_trim = FOREACH physician_space
					GENERATE TRIM(recipient_primary_business_street_address_line1) as recipient_primary_business_street_address_line1, TRIM(recipient_primary_business_street_address_line2) as recipient_primary_business_street_address_line2, TRIM(recipient_city) as recipient_city, TRIM(recipient_state) as recipient_state, total_amount_of_payment_usdollars;

physician_upper = FOREACH physician_trim
					GENERATE UPPER(recipient_primary_business_street_address_line1) as recipient_primary_business_street_address_line1, UPPER(recipient_primary_business_street_address_line2) as recipient_primary_business_street_address_line2, UPPER(recipient_city) as recipient_city, UPPER(recipient_state) as recipient_state, total_amount_of_payment_usdollars;

physician_abb_ste = FOREACH physician_upper GENERATE REPLACE(recipient_primary_business_street_address_line1,'SUITE','STE') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'SUITE','STE') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;
					
physician_abb_st = FOREACH 	physician_abb_ste GENERATE REPLACE(recipient_primary_business_street_address_line1,'STREET','ST') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'STREET','ST') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_blvd = FOREACH physician_abb_st GENERATE REPLACE(recipient_primary_business_street_address_line1,'BOULEVARD','BLVD') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'BOULEVARD','BLVD') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;
		
physician_abb_rd = FOREACH 	physician_abb_blvd GENERATE REPLACE(recipient_primary_business_street_address_line1,'ROAD','RD') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'ROAD','RD') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_apt = FOREACH 	physician_abb_rd GENERATE REPLACE(recipient_primary_business_street_address_line1,'APARTMENT','APT') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'APARTMENT','APT') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_ave = FOREACH 	physician_abb_apt GENERATE REPLACE(recipient_primary_business_street_address_line1,'AVENUE','AVE') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'AVENUE','AVE') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_bldg = FOREACH physician_abb_ave GENERATE REPLACE(recipient_primary_business_street_address_line1,'BUILDING','BLDG') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'BUILDING','BLDG') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_dept = FOREACH physician_abb_bldg GENERATE REPLACE(recipient_primary_business_street_address_line1,'DEPARTMENT','DEPT') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'DEPARTMENT','DEPT') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_ln = FOREACH physician_abb_dept GENERATE REPLACE(recipient_primary_business_street_address_line1,'LANE','LN') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'LANE','LN') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_plz = FOREACH physician_abb_ln GENERATE REPLACE(recipient_primary_business_street_address_line1,'PLAZA','PLZ') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'PLAZA','PLZ') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_pl = FOREACH physician_abb_plz GENERATE REPLACE(recipient_primary_business_street_address_line1,'PLACE','PL') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'PLACE','PL') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_rdg = FOREACH physician_abb_pl GENERATE REPLACE(recipient_primary_business_street_address_line1,'RIDGE','RDG') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'RIDGE','RDG') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_dr = FOREACH physician_abb_rdg GENERATE REPLACE(recipient_primary_business_street_address_line1,'DRIVE','DR') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'DRIVE','DR') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_pkwy = FOREACH physician_abb_dr GENERATE REPLACE(recipient_primary_business_street_address_line1,'PARKWAY','PKWY') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'PARKWAY','PKWY') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;

physician_abb_vly = FOREACH physician_abb_pkwy GENERATE REPLACE(recipient_primary_business_street_address_line1,'VALLY','VLY') as recipient_primary_business_street_address_line1, REPLACE(recipient_primary_business_street_address_line2,'VALLY','VLY') as recipient_primary_business_street_address_line2, recipient_city, recipient_state, total_amount_of_payment_usdollars;
		
grouped_physician = FOREACH(GROUP physician_abb_vly BY (recipient_primary_business_street_address_line1, recipient_primary_business_street_address_line2, recipient_city, recipient_state) PARALLEL 1)
					GENERATE FLATTEN(group), SUM(physician_abb_vly.total_amount_of_payment_usdollars) AS pharmaceutical_payments;
				
				
joined_filtered_records = JOIN grouped_filtered_medicare BY (nppes_provider_street1, nppes_provider_street2, nppes_provider_city, nppes_provider_state),
								grouped_physician BY (recipient_primary_business_street_address_line1, recipient_primary_business_street_address_line2, recipient_city, recipient_state);
final_records = FOREACH joined_filtered_records GENERATE recipient_primary_business_street_address_line1, recipient_primary_business_street_address_line2, recipient_city, recipient_state, grouped_filtered_medicare::medicare_billings, grouped_physician::pharmaceutical_payments; 							
--final_records = FOREACH joined_filtered_records GENERATE grouped_filtered_medicare::medicare_billings, grouped_physician::pharmaceutical_payments; 

STORE final_records INTO '/user/shavanur/output' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','WINDOWS');



