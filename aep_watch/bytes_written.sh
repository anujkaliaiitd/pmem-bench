cols=`seq -s ',' 4 13 147`
cat output.csv | tail -n +6 | cut -d ';' -f $cols
