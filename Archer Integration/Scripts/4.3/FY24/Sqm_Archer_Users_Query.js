try{
    db = db.getSiblingDB('isqc');

    //Drop existing collection
    db.archerusers.drop();

    var fiscalYearFilter = 2024;

    db.user.aggregate([{
        $lookup: {
            from: 'firm',
            let: {
                geographyIds: '$roles.geographyId'
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: ['$geographyId', 
                        { 
                            $in: ['$geographyId', '$$geographyIds'] 
                        }
                        ,{
                            $eq:["$fiscalYear", fiscalYearFilter]
                        }]
                    }
                }
            }],
            as: 'entities'
        }
    }, {
        $unwind: {
            path: '$entities',
            preserveNullAndEmptyArrays: true
        }
    },{
        $addFields: {
            role: {
                $filter: {
                    input: '$roles', 
                    as: 'r',
                    cond: { $eq: ['$$r.geographyId', { $cond: { if: '$entities', then: '$entities.geographyId', else: '' } }] }
                }
            }
        }
    },{
        $project: {
            _id: 0,
            UserName: '$displayName',
            Email: '$email',
            Entity: { $cond: { if: '$entities', then: '$entities.name', else: '' } },
            Permission: {
                $switch: {
                    branches: [
                        { case: { $in: [ "RoleType_Admin",'$role.roleId'] }, then: "Admin" },
                        { case: { $in: [ "RoleType_Edit",'$role.roleId'] }, then: "Edit" },
                        { case: { $in: [ "RoleType_Bkcc",'$role.roleId'] }, then: "Read only"},
                        { case: { $in: [ "RoleType_ReadOnly",'$role.roleId'] }, then: "Read only" },//both readonly and bkcc has permission read only
                    ],
                    default: ''
                }
            },
            Role: {
                $switch: {
                    branches: [
                        { case: { $in: [ "RoleType_Admin",'$role.roleId'] }, then: "RoleType_Admin" },
                        { case: { $in: [ "RoleType_Edit",'$role.roleId'] }, then: "RoleType_Edit" },
                        { case: { $in: [ "RoleType_Bkcc",'$role.roleId'] }, then: "RoleType_Bkcc"},
                        { case: { $in: [ "RoleType_ReadOnly",'$role.roleId'] }, then: "RoleType_ReadOnly" },
                        
                    ],
                    default: ''
                }
            },
            GlobalAccess: { $cond: { if: '$isSuperAdmin', then: 'Yes', else: 'No' } }
        }
    }, {
        $out: 'archerusers'
    }])
} catch (error) {
    print("SYSTEM:Archer Error:: Error at Users  Query ",error);
    throw(error);
}   