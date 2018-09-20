pmempool rm --verbose /dev/dax0.0
pmempool create --layout pmemkv obj /dev/dax0.0
./bench
