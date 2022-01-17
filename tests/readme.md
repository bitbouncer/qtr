#create testdata for parquet
sed -n '1,5p' 2020-01-01-OPTIONS-options_chain.csv > test1.csv
gzip test1.csv


