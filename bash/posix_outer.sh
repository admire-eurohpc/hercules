#!/bin/bash

#Individual test over NFS.
./posix_individual_write.sh /home/mandres/IMSS/build/posix_inv ../tst/posix_individual_network.csv

#Individual test over local storage.
./posix_individual_write.sh /tmp/posix_inv ../tst/posix_individual_local.csv
