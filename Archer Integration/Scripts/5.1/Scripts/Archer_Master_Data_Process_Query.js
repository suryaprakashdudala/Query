 try{ 
    db = db.getSiblingDB('isqc');

    db.masterDataProcess.drop()
	
	var fiscalYearFilter = process.env.FiscalYear;
	
	fiscalYearFilter = parseInt(fiscalYearFilter,10);

    db.globaldocumentation.aggregate( [
        {
            $match: {
                type: 'Process',
                fiscalYear : fiscalYearFilter
                    }
        },
        {
                $project:{
                _id:{ "$toString": "$_id" },
                UniqueId:"$uniqueId",
                ProcessName:"$name",
                ProcessDescription: "$description",
                isPCAOBRegistered:"$isPCAOBRegistered"
                        
                        }
        },
        {
        $addFields: { "FiscalYear": "FY" + fiscalYearFilter.toString().slice(-2), LastProcessedDate : new Date().toISOString()  }
        },
        {
            $addFields: { "PCAOB_Configured_Object": {
                $cond:{
                    if:"$isPCAOBRegistered",
                    then:"Yes",
                    else:"No"
                }
            } }
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
        var isProcessUpdate = existingProcess && db.event.find({actor: ObjectId(pro._id), fiscalYear: fiscalYearFilter, message: 'ActionType_Update',modifiedOn : {$gt:existingProcess.ArcherPublishedOn}}).sort({'modifiedOn':-1}).limit(1).toArray();

            if (!(existingProcess)) {
                var getNewEventDateProcess = db.event.find({actor: ObjectId(pro._id), fiscalYear: fiscalYearFilter, message: 'ActionType_Add'}).sort({'modifiedOn':-1}).limit(1).toArray();
                db.masterDataProcess.updateOne({_id:pro._id},{$set:{EventType:'New',EventAction : 'Update', LastPublishedRecordStatus : 'Active',FirmPublishedDate:new Date().toISOString(),EventDate : getNewEventDateProcess.length>0?getNewEventDateProcess[0].modifiedOn:''}});
            }
            else if (isProcessUpdate.length>0) {
                db.masterDataProcess.updateOne({_id:pro._id},{$set:{EventType:'Updated',EventAction : 'Update', LastPublishedRecordStatus : 'Active',FirmPublishedDate:new Date().toISOString(), EventDate : isProcessUpdate[0].modifiedOn}});
            }
        else
            {
                db.masterDataProcess.updateOne({_id:pro._id},{$set:{EventType:existingProcess.EventType,EventAction : existingProcess.EventAction, LastPublishedRecordStatus : existingProcess.LastPublishedRecordStatus, FirmPublishedDate : existingProcess.FirmPublishedDate,EventDate : existingProcess.EventDate}});
            }
    })
    db.masterDataProcessCopy.find().forEach(function(pro){
        var existingProcess = db.masterDataProcess.findOne({UniqueId :pro.UniqueId});
        var isProcessDelete = db.event.find({actor: pro.UniqueId, fiscalYear: fiscalYearFilter, message: 'ActionType_Delete',modifiedOn : {$gt:pro.ArcherPublishedOn}}).sort({'modifiedOn':-1}).limit(1).toArray();
        if(!existingProcess && isProcessDelete.length>0){
                pro.EventType = 'Deleted';
                pro.EventAction = 'Update';
                pro.LastPublishedRecordStatus = 'Inactive';
                pro.EventDate = isProcessDelete[0].modifiedOn;
                db.masterDataProcess.insertOne(pro);
        }
    })
    
    db.masterDataProcessCopy.drop(); 
    db.masterDataProcess.aggregate( [   
        { $merge : { into : "masterDataProcessCopy" } }
    ]);

    var calc7Days = 7 * 24 * 60 * 60 * 1000;
    db.masterDataProcess.updateMany({FirmPublishedDate : {$lte:new Date(ISODate().getTime() - calc7Days).toISOString()}},{$set:{EventAction:''}});
} catch (error) {
    print("SYSTEM:Archer Error :: Error at Master Data Process Copy Query",error);
    throw(error)
}