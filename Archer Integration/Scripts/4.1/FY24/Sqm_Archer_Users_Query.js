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
}, {
    $project: {
        _id: 0,
        UserName: '$displayName',
        Email: '$email',
        Entity: { $cond: { if: '$entities', then: '$entities.name', else: '' } },
        Permission: {
            $let: {
                vars: {
                    role: {
                        $arrayElemAt: [{
                            $filter: {
                                input: '$roles',
                                as: 'r',
                                cond: { $eq: ['$$r.geographyId', { $cond: { if: '$entities', then: '$entities.geographyId', else: '' } }] }
                            }
                        }, 0]
                    }
                },
                in: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$$role.roleId', "RoleType_Admin"] }, then: "Admin" },
                            { case: { $eq: ['$$role.roleId', "RoleType_Edit"] }, then: "Edit" },
                            { case: { $eq: ['$$role.roleId', "RoleType_ReadOnly"] }, then: "Read only" },
                            { case: { $eq: ['$$role.roleId', "RoleType_Bkcc"] }, then: "Read only" }
                        ],
                        default: ''
                    }
                }
            }
        },
        GlobalAccess: { $cond: { if: '$isSuperAdmin', then: 'Yes', else: 'No' } }
    }
}, {
    $out: 'archerusers'
}])