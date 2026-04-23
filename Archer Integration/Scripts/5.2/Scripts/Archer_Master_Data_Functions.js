try {
    db = db.getSiblingDB('isqc');

    db.masterDataFunction.drop()

    var fiscalYearFilter = process.env.FiscalYears

    fiscalYearFilter = fiscalYearFilter.split(",").map(function (year) {
        return parseInt(year, 10);
    });
    db.globaldocumentation.aggregate([
        {
            $match: {
                type: 'Function',
                fiscalYear: { $in: fiscalYearFilter }
            }
        },
        {
            $project: {
                _id:  "$_id",
                FunctionId: { "$toString": "$_id" },
                FunctionName: "$name",
                fiscalYear: "$fiscalYear",
                ArcherPublishedOn: new Date().toISOString()
            }
        },
        {
            $addFields: {
                FiscalYear: { $concat: ["FY", { $substr: ["$fiscalYear", 2, 3] }] },
            }
        },
        {
            $out: "masterDataFunction"
        }
    ]);

    db.masterDataFunction.find().forEach(function (pro) {
        var existingFunction = db.masterDataFunctionCopy.findOne({ _id: pro._id });
        var isFunctionUpdate = existingFunction && db.event.find({
            actor: pro._id,
            fiscalYear: pro.fiscalYear,
            message: 'ActionType_Update',
            modifiedOn: { $gt: existingFunction.ArcherPublishedOn }
        }).sort({ 'modifiedOn': -1 }).limit(1).toArray();

        if (!(existingFunction)) {
            var getNewEventDateFunction = db.event.find({
                actor: pro._id,
                fiscalYear: pro.fiscalYear,
                message: 'ActionType_Add'
            }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
            db.masterDataFunction.updateOne({ _id: pro._id }, { $set: { EventType: 'New', EventAction: 'Update', LastPublishedRecordStatus: 'Active', LastFlagProcessedDate: new Date().toISOString(), EventDate: getNewEventDateFunction.length > 0 ? getNewEventDateFunction[0].modifiedOn : '' } });
        }
        else if (isFunctionUpdate.length > 0) {
            db.masterDataFunction.updateOne({ _id: pro._id }, { $set: { EventType: 'Updated', EventAction: 'Update', LastPublishedRecordStatus: 'Active', LastFlagProcessedDate: new Date().toISOString(), EventDate: isFunctionUpdate[0].modifiedOn } });
        }
        else {
            db.masterDataFunction.updateOne({ _id: pro._id }, { $set: { EventType: existingFunction.EventType, EventAction: existingFunction.EventAction, LastPublishedRecordStatus: existingFunction.LastPublishedRecordStatus, LastFlagProcessedDate: existingFunction.LastFlagProcessedDate, EventDate: existingFunction.EventDate } });
        }
    })
    db.masterDataFunctionCopy.find().forEach(function (pro) {
        var existingFunction = db.masterDataFunction.findOne({ _id: pro._id });
        var isFunctionDelete = db.event.find({
            actor: pro._id,
            fiscalYear: pro.fiscalYear,
            message: 'ActionType_Delete',
            modifiedOn: { $gt: pro.ArcherPublishedOn }
        }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
        if (!existingFunction && isFunctionDelete.length > 0) {
            pro.EventType = 'Deleted';
            pro.EventAction = 'Update';
            if(pro.LastPublishedRecordStatus == 'Active')
            {
                pro.LastFlagProcessedDate = new Date().toISOString();
            }
            else
            {
                pro.LastFlagProcessedDate = pro.LastFlagProcessedDate && pro.LastFlagProcessedDate!==''? pro.LastFlagProcessedDate : new Date().toISOString();
            }
            pro.LastPublishedRecordStatus = 'Inactive';
            pro.EventDate = isFunctionDelete[0].modifiedOn;
            db.masterDataFunction.insertOne(pro);
        }
    })

    db.masterDataFunctionCopy.drop();
    db.masterDataFunction.aggregate([
        { $merge: { into: "masterDataFunctionCopy" } }
    ]);

    var calc7Days = 7 * 24 * 60 * 60 * 1000;
    db.masterDataFunction.updateMany({ LastFlagProcessedDate: { $lte: new Date(ISODate().getTime() - calc7Days).toISOString() } }, { $set: { EventAction: '' } });
} catch (error) {
    print("SYSTEM:Archer Error :: Error at Master Data Function Copy Query", error);
    throw (error)
}
