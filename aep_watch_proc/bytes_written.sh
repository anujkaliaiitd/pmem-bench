cols=`seq -s ',' 13 13 156`
cat output.csv | tail -n +6 | cut -d ';' -f $cols
