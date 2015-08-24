iquery -aq "multisplit('paths=/development/gitdev/multisplit/src/testfolder','instances=3')"


iquery -aq "multisplit('paths=/development/gitdev/multisplit/src/testfile2','instances=3')"


iquery -aq "multisplit('paths=/development/gitdev/multisplit/src/testfolder','instances=-1')"


iquery -aq "multisplit('paths=/development/gitdev/multisplit/src/testfile1;/development/gitdev/multisplit/src/testfile2','instances=3;0')"


iquery -aq "multisplit('paths=/development/gitdev/multisplit/src/testfile1;/development/gitdev/multisplit/src/testfile2','instances=3;-1')"

 iquery -aq "multisplit('paths=/development/gitdev/multisplit/src/testfolder','instances=4')"


iquery -aq "multisplit('paths=/development/gitdev/multisplit/src/testfolder','instances=-5')"


