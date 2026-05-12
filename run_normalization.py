from normalization import normalize_csv_security_names

normalize_csv_security_names(
    input_csv="pflt_security_mapping_unique.csv",
    output_csv="pflt_security_mapping_unique_normalized.csv",
    security_name_header="master_comp_security_name", 
    soi_name_header="soi_name"             
)