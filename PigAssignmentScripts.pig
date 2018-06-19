billsByName = LOAD 'bankdata' using TextLoader AS (line: chararray);
dump billsByName;
describe billsByName;                                                 
tupleToBag = FOREACH billsByName GENERATE flatten(myudfs.TupleToBag(line));   
y = limit tupleToBag 10;
dump y;
tellerBillGrp = group tupleToBag  by tellerName;
z = limit tellerBillGrp 10;
dump z;
tots = foreach tellerBillGrp generate group, SUM(tupleToBag.bill) as bill;                         ;    
dump tots;
ordgrp = ORDER tots  BY bill DESC;                                                        
dump ordgrp; 
filterSaeed = filter tots  by group == 'Saeed';   
dump filterSaeed;
filteredTeller = filter tots  by bill == filterSaeed.bill and group != 'Saeed';
teller = foreach filteredTeller generate group;                                
dump teller;