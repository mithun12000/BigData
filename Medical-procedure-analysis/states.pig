lines = LOAD '/user/shavanur/Plays/Medicare-Physician-and-Other-Supplier-PUF-CY2012.txt' USING PigStorage('\t') 
as (npi,nppes_provider_last_org_name,nppes_provider_first_name,nppes_provider_mi,nppes_credentials,nppes_provider_gender,
nppes_entity_code,nppes_provider_street1,nppes_provider_street2,nppes_provider_city,nppes_provider_zip,nppes_provider_state,nppes_provider_country,provider_type,medicare_participation_indicator,
place_of_service,hcpcs_code,hcpcs_description,line_srvc_cnt,bene_unique_cnt,bene_day_srvc_cnt,average_Medicare_allowed_amt,stdev_Medicare_allowed_amt,average_submitted_chrg_amt,stdev_submitted_chrg_amt,
average_Medicare_payment_amt,stdev_Medicare_payment_amt);
lines_getdetails = foreach lines GENERATE average_submitted_chrg_amt as Cost, nppes_provider_state as state, hcpcs_code,nppes_provider_country;
lines_getdetails_93015 = FILTER lines_getdetails BY hcpcs_code matches '93015';
lines_getdetails_US = FILTER lines_getdetails_93015 BY nppes_provider_country matches 'US';
grouped = GROUP lines_getdetails_US by state PARALLEL 1;
lines_total = FOREACH grouped GENERATE group, AVG(lines_getdetails_US.Cost);
STORE lines_total INTO '/user/shavanur/output' USING PigStorage('\t');

