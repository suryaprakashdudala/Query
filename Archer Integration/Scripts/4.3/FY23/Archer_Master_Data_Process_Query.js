try{
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
}catch (error) {
    print("SYSTEM:Archer Error :: Error at Master Data Process Query ",error);
    throw(error);
}