rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-16-32-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 1 -n 10000
rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-16-32-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 2 -n 5000
rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-16-32-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 4 -n 2500
rm -f /dev/shm/nvdimm_midas && ./midas/bin/scaling -d data/dat-16-32-65536.csv -p mixed-sap-oltp.cfg -p read-only.cfg -a 64 -t 8 -n 1250
