rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-128-1024-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 1 -n 1000
rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-128-1024-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 2 -n 500
rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-128-1024-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 4 -n 250
rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-128-1024-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 8 -n 125
