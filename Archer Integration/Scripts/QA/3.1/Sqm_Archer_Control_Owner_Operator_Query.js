db = db.getSiblingDB('isqc');

//Drop existing collection
db.sqmcontrolownersoperators.drop();

db.documentation.aggregate([{
    $match:{
        type:'KeyControl'
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
        type:'RequirementControlAssignment'
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

db.kcTitles.copyTo('controlRelatedTitles')
db.rcTitles.copyTo('controlRelatedTitles')

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
},{
    $out:'sqmcontrolownersoperators'
}]);

//Drop temporary collections.
db.kcTitles.drop();
db.rcTitles.drop();
db.controlRelatedTitles.drop();

//mongoexport -d isqc -c sqmcontrolownersoperators --fields=UserName,Email --type=csv --out=./sqmcontrolownersoperators.csv