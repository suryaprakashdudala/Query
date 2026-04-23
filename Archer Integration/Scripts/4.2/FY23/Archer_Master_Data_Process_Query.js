db = db.getSiblingDB('isqc');

db.masterDataProcess.drop()

db.globaldocumentation.aggregate( [
    {
        $match: {
            type: 'Process',
            fiscalYear : 2023
                }
    },
    {
             $project:{
             UniqueId:{ "$toString": "$_id" },
             ProcessName:"$name"
                    
                      }
    },
    {
             $out: "masterDataProcess"
     }
  ]);
  
  
db.masterDataProcess.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY23"}})