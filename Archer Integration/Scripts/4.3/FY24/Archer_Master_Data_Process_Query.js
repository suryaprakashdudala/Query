 try{ 
    db = db.getSiblingDB('isqc');

    db.masterDataProcess.drop()

    db.globaldocumentation.aggregate( [
        {
            $match: {
                type: 'Process',
                fiscalYear : 2024
                    }
        },
        {
                $project:{
                _id:{ "$toString": "$_id" },
                UniqueId:"$uniqueId",
                ProcessName:"$name",
                ProcessDescription: "$description"
                        
                        }
        },
        {
        $addFields: { "FiscalYear": "FY24" }
        },
        {
        $addFields: { 'ArcherPublishedOn' : new Date().toISOString()}
        },
        {
                $out: "masterDataProcess"
        }
    ]);
    
    db.masterDataProcess.find().forEach(function (pro){
        var existingProcess = db.masterDataProcessCopy.findOne({UniqueId: pro.UniqueId});
        var isProcessUpdate = existingProcess && db.event.findOne({actor: pro.UniqueId, message: 'ActionType_Update',modifiedOn : {$gt:existingProcess.ArcherPublishedOn}});
            if (!(existingProcess)) {
                db.masterDataProcess.updateOne({_id:pro._id},{$set:{EventType:'New',EventAction : 'Update', RecordStatus : 'Active',PublishedDate:new Date().toISOString()}});
            }
            else if (isProcessUpdate) {
                db.masterDataProcess.updateOne({_id:pro._id},{$set:{EventType:'Updated',EventAction : 'Update', RecordStatus : 'Active',PublishedDate:new Date().toISOString()}});
            }
        else
            {
                db.masterDataProcess.updateOne({_id:pro._id},{$set:{EventType:existingProcess.EventType,EventAction : existingProcess.EventAction, RecordStatus : existingProcess.RecordStatus, PublishedDate : existingProcess.PublishedDate}});
            }
    })
    db.masterDataProcessCopy.find().forEach(function(pro){
        var existingProcess = db.masterDataProcess.findOne({UniqueId :pro.UniqueId});
        var isProcessDelete = db.event.findOne({actor: pro.UniqueId, message: 'ActionType_Delete',modifiedOn : {$gt:pro.ArcherPublishedOn}});
        if(!existingProcess && isProcessDelete){
                pro.EventType = 'Deleted';
                pro.EventAction = 'Update';
                pro.RecordStatus = 'Inactive';
                db.masterDataProcess.insertOne(pro);
        }
    })
    
    db.masterDataProcessCopy.drop(); 
    db.masterDataProcess.aggregate( [   
        { $merge : { into : "masterDataProcessCopy" } }
    ]);

    var calc7Days = 7 * 24 * 60 * 60 * 1000;
    db.masterDataProcess.updateMany({PublishedDate : {$lte:new Date(ISODate().getTime() - calc7Days).toISOString()}},{$set:{EventAction:''}});
} catch (error) {
    print("SYSTEM:Archer Error :: Error at Master Data Process Copy Query",error);
    throw(error)
}