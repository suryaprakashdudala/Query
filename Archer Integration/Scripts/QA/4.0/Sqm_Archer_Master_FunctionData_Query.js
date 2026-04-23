db = db.getSiblingDB('isqc');

//Drop existing collection
db.masterfunctiondetails.drop()

db.globaldocumentation.aggregate( [
    {
        $match: {
            type: 'Function'
                }
    },
    {
             $project:{
             FunctionId:{ "$toString": "$_id" },
             FunctionName:"$name",
             FiscalYear:"$fiscalYear"
                    
                      }
    },
    {
             $out: "masterfunctiondetails"
     }
  ]);