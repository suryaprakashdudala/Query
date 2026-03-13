try {

var startTIme = new Date();
print("SYSTEM:Power-BI-Info:: Starting SQM Leadership Roles script at ", startTIme.toISOString());

db.sqmleadership.drop();

var fiscalYearFilter = 2026;

db.firm.aggregate([

{
$match:{
    fiscalYear:fiscalYearFilter,
    type:"EntityType_MemberFirm"
}
},

/* Normalize roles */

{
$addFields:{
    roles:[
        {role:"ultimateResponsibility", ids:{ $ifNull:["$ultimateResponsibility",[]]}},
        {role:"operationalResponsibilitySqm", ids:{ $ifNull:["$operationalResponsibilitySqm",[]]}},
        {role:"orIndependenceRequirement", ids:["$orIndependenceRequirement"]},
        {role:"orMonitoringRemediation", ids:["$orMonitoringRemediation"]}
    ]
}
},

{ $unwind:"$roles" },

/* lookup titles */

{
$lookup:{
    from:"title",
    let:{
        roleIds:"$roles.ids",
        firmId:"$firmGroupId",
        fiscalYear:"$fiscalYear"
    },
    pipeline:[

    {
    $match:{
        $expr:{
            $and:[
                { $in:[{$toString:"$_id"}, {$ifNull:["$$roleIds",[]]}]},
                { $eq:["$fiscalYear","$$fiscalYear"]}
            ]
        }
    }
    },

    /* lookup assignments */

    {
    $lookup:{
        from:"titleassignment",
        let:{
            titleId:{$toString:"$_id"},
            firmId:"$$firmId",
            fiscalYear:"$fiscalYear"
        },
        pipeline:[
        {
        $match:{
            $expr:{
                $and:[
                    {$eq:["$titleId","$$titleId"]},
                    {$eq:["$firmId","$$firmId"]},
                    {$eq:["$fiscalYear","$$fiscalYear"]}
                ]
            }
        }
        }
        ],
        as:"titleAssignments"
    }
    },

    {
    $unwind:{
        path:"$titleAssignments",
        preserveNullAndEmptyArrays:true
    }
    }

    ],
    as:"titles"
}
},

{
$unwind:{
    path:"$titles",
    preserveNullAndEmptyArrays:true
}
},

/* Build assignment string once */

{
$addFields:{
    assignment:{
        $reduce:{
            input:{
                $map:{
                    input:{ $ifNull:["$titles.titleAssignments.assignments",[]]},
                    as:"a",
                    in:{ $concat:["$$a.displayName","(","$$a.email",")"]}
                }
            },
            initialValue:"",
            in:{
                $cond:[
                    {$eq:["$$value",""]},
                    "$$this",
                    {$concat:["$$value","; ","$$this"]}
                ]
            }
        }
    }
}
},

/* final output */

{
$project:{
    role:"$roles.role",
    title:"$titles.name",
    assignment:1,
    fiscalYear:1,
    memberFirmId:1,
    country:1,
    firmGroupId:1,
    name:1,
    type:1,
    leadershipId:{
        $cond:[
            {$ifNull:["$titles._id",false]},
            {$toObjectId:"$titles._id"},
            null
        ]
    }
}
},

{
$sort:{
    fiscalYear:1,
    memberFirmId:1
}
},

{
$out:"sqmleadership"
}

],
{allowDiskUse:true});

var endTime = new Date();

print(
"SYSTEM:Power-BI-Info:: Completed SQM Leadership Roles script at ",
endTime.toISOString(),
" Total execution time (minutes): ",
(endTime.getTime() - startTIme.getTime())/60000
);

}
catch(error){

print("SYSTEM:Power-BI-Error:: Error in SQM_Leadership_Roles ", error);
throw(error);

}