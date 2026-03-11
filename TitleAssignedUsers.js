try{
    // var db = db.getSiblingDB('isqc');

    db.titleAssignedUsers.drop();

    var fiscalYearFilter = 2026;

    db.title.aggregate([
        {
            $match:{
                fiscalYear:fiscalYearFilter
            }
        },
        {
            $lookup:{
                from:'titleassignment',
                let:{
                    id:{$toString:'$_id'}
                },
                pipeline:[
                    {
                        $match:{
                            $expr:{
                                $and:[
                                    {$eq:['$fiscalYear',fiscalYearFilter]},
                                    {$eq:['$titleId','$$id']}
                                ]
                            }
                        }
                    }
                ],
                as:'titleAssignmentsData'
            }
        },
        {
            $unwind:{
                path:'$titleAssignmentsData',
                preserveNullAndEmptyArrays:true
            }
        },
        {
            $unwind:{
                path:'$titleAssignmentsData.assignments',
                preserveNullAndEmptyArrays:true
            }
        },
        {
            $project:{
                _id:0,
                id:'$titleAssignmentsData.titleId',
                firmId:'$titleAssignmentsData.firmId',
                name:'$name',
                isDeactivated:'$titleAssignmentsData.isDeactivated',
                assignedName:'$titleAssignmentsData.assignments.displayName',
                assignedEmail:'$titleAssignmentsData.assignments.email'
                
            }
        },
        {
            $out:'titleAssignedUsers'
        }
    ])

}
catch(error){
    print("SYSTEM:Power-BI-Error:: Error in SQM_TitleAssignedUsers_Query ", error);
    throw (error);
}