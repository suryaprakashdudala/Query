try{
    db = db.getSiblingDB('isqc');

    //Drop existing collection
    db.sqmcontrolownersoperators.drop();

    var fiscalYearFilter = 2025;

    db.documentation.aggregate([{
        $match:{
            //type:'KeyControl'
            $and:[{
                type:'KeyControl',
                fiscalYear: fiscalYearFilter
            }]
        }
    },{
        $project:{
            controlTitles:{
                $concatArrays:['$responseOwners','$controlOperator']
            },
            firmId:1
        }
    },{
        $out:'kcTitles'
    }])

    //To be  used while migrating to mongo 5
    // {
    //     $merge:'kcTitles'
    // }

    db.documentation.aggregate([{
        $match:{
            //type:'RequirementControlAssignment'
            $and:[{
                type:'RequirementControlAssignment',
                fiscalYear: fiscalYearFilter
            }]
        }
    },{
        $project:{
            controlTitles:{
                $concatArrays:['$responseOwners','$controlOperator']
            },
            firmId:1
        }
    },{
        $out:'rcTitles'
    }])
    //To be  used while migrating to mongo 5
    // {
    //     $merge:'rcTitles'
    // }

    //db.kcTitles.copyTo('controlRelatedTitles')
    //db.rcTitles.copyTo('controlRelatedTitles')

    db.createCollection('controlRelatedTitles');

    // db.kcTitles.find().forEach(function(doc){
    // db.rcTitles.insert(doc); // start to replace
    // });

    //  db.kcTitles.find().forEach(function(doc){
    //     db.controlRelatedTitles.insert(doc); // start to replace
    //  });

    //  db.rcTitles.find().forEach(function(doc){
    //     db.controlRelatedTitles.insert(doc); // start to replace
    //  });

    db.kcTitles.aggregate([ { "$merge" : { into: "controlRelatedTitles" } } ]);
    db.rcTitles.aggregate([ { "$merge" : { into: "controlRelatedTitles" } } ]);

    db.controlRelatedTitles.aggregate([{
        $unwind:'$controlTitles'
    },{
        $group:{
            _id:{titleId:'$controlTitles',firmId:'$firmId'}
        }
    },{
        $lookup:{
            from:'titleassignment',
            let:{
                titleId:'$_id.titleId',
                firmId:'$_id.firmId'
            },
            pipeline:[{
                $match:{
                    $expr:{
                        $and:[{
                            $eq: ["$fiscalYear", fiscalYearFilter]
                        },{
                            $eq:[{$toString:'$$titleId'},'$titleId']
                        },{
                            $eq:['$$firmId','$firmId']
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
            _id:0,
            UserName:'$titleAssignments.assignments.displayName',
            Email:'$titleAssignments.assignments.email'
        }
    },{
        $group:{
            _id:{UserName:'$UserName',Email:'$Email'}
        }
    },{
        $replaceRoot:{newRoot:'$_id'}
    },
        {
        $addFields: { "FiscalYear": "FY25" }
        },{
        $out:'sqmcontrolownersoperators'
    }]);

    //Drop temporary collections.
    db.kcTitles.drop();
    db.rcTitles.drop();
    db.controlRelatedTitles.drop();

    //mongoexport -d isqc -c sqmcontrolownersoperators --fields=UserName,Email --type=csv --out=./sqmcontrolownersoperators.csv

    //db.sqmcontrolownersoperators.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY24"}})
}catch(error) {
    db.kcTitles.drop();
    db.rcTitles.drop();
    db.controlRelatedTitles.drop();
    print("SYSTEM:Archer Error :: Error at Archer Control Owner Operator Query ",error);
    throw(error)
}