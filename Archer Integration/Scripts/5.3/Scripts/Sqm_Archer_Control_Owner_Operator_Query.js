try{
    db = db.getSiblingDB('isqc');

    //Drop existing collection
db.sqmcontrolownersoperators.drop();

// var fiscalYearFilter = process.env.FiscalYear;
var fiscalYearFilter = process.env.FiscalYears;

fiscalYearFilter = fiscalYearFilter.split(",").map(function(year){
    return parseInt(year,10);
});

db.documentation.aggregate([{
    $match:{
        //type:'KeyControl'
        $and:[{
            type: { $in: ['KeyControl', 'RequirementControlAssignment'] },
            fiscalYear: {$in: fiscalYearFilter}
        }]
    }
}, {
    $project: {
        controlOperator: '$controlOperator',
        responseOwners: '$responseOwners',
        controOwnersOperators: { $concatArrays: ['$responseOwners', '$controlOperator'] },
        firmId: 1,
        fiscalYear:1,
        _id: 0
    }
}, {
    $unwind: '$controOwnersOperators'
}, {
    $out: 'controlOperatorOwner'
}])
db.controlOperatorOwner.aggregate([
    {
        $project: {
            controlOperator: '$controlOperator',
            responseOwners: '$responseOwners',
            firmId: 1,
            fiscalYear: 1
        }
    }, {
        $group: {
            _id: { firmId: '$firmId', controlOperator: '$controlOperator', fiscalYear: '$fiscalYear' }
        }
    }, {
        $unwind: '$_id.controlOperator'
    }, {
        $project: {
            titleId: '$_id.controlOperator',
            userGroup: 'Control_Operator',
            firmId: '$_id.firmId',
            fiscalYear: '$_id.fiscalYear',
            _id: 0
        }
    }, {
        $out: 'controlOperator'
    }])
db.controlOperatorOwner.aggregate([
    {
        $project: {
            controlOperator: '$controlOperator',
            responseOwners: '$responseOwners',
            firmId: 1,
            fiscalYear:1
        }
    }, {
        $group: {
            _id: { firmId: '$firmId', responseOwners: '$responseOwners', fiscalYear: '$fiscalYear' }
        }
    }, {
        $unwind: '$_id.responseOwners'
    }, {
        $project: {
            titleId: '$_id.responseOwners',
            userGroup: 'Control_Owner',
            firmId: '$_id.firmId',
            fiscalYear: '$_id.fiscalYear',
            _id: 0
        }
    }, {
        $out: 'controlOwner'
    }])

db.createCollection('controlRelatedTitles');
db.controlOperator.aggregate([{ "$merge": { into: "controlRelatedTitles" } }]);
db.controlOwner.aggregate([{ "$merge": { into: "controlRelatedTitles" } }]);
db.controlRelatedTitles.aggregate([
    {
        $lookup:{
            from: 'titleassignment',
            let:{
                titleId:'$titleId',
                firmId:'$firmId',
                firmFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    $expr:{
                        $and:[{
                            $eq: ["$fiscalYear", "$$firmFiscalYear"]
                        },{
                            $eq: ['$$firmId','$firmId']
                        },{
                            $eq:[{ $toString: '$$titleId' }, '$titleId']
                        }]

                    }
                }
            }],
            as:'titleAssignments'
        }
    },{
        $unwind:'$titleAssignments'
    },{
        $unwind:'$titleAssignments.assignments'
    },{
        $project:{
            _id: 0,
            UserName:'$titleAssignments.assignments.displayName',
            Email:'$titleAssignments.assignments.email',
            UserGroup:'$userGroup',
            fiscalYear:'$fiscalYear',
        }
    },{
        $group:{
            _id:{ UserName:'$UserName',Email:'$Email',UserGroup:'$UserGroup', EntityFiscalYear:'$fiscalYear' }
        }
    },{
        $replaceRoot:{newRoot:'$_id'}
    },
        {
        $addFields: { "FiscalYear": {$concat:['FY',{$substr:[{$toString:'$EntityFiscalYear'},2,2]}]}}
    },{
        $out:'sqmcontrolownersoperators'
    }]);

    //Drop temporary collections.
db.controlOperator.drop();
db.controlOwner.drop();
db.controlOperatorOwner.drop();
db.controlRelatedTitles.drop();

    //mongoexport -d isqc -c sqmcontrolownersoperators --fields=UserName,Email --type=csv --out=./sqmcontrolownersoperators.csv
    
    //db.sqmcontrolownersoperators.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY24"}})
}catch(error){
    db.controlOperator.drop();
    db.controlOwner.drop();
    db.controlOperatorOwner.drop();
    db.controlRelatedTitles.drop();
    print("SYSTEM:Archer Error :: Error at Archer Control Owner Operator Query",error);
    throw(error)
}