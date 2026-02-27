try{

    //Drop existing collection
    db.archerentities.drop();

    var fiscalYearFilter = "2026";
	
	fiscalYearFilter = fiscalYearFilter.split(",").map(function (year) {
        return parseInt(year, 10);
    });

    db.firm.aggregate([{
        $match:{
                    fiscalYear: {$in:fiscalYearFilter}
            }
    },{  $graphLookup: {
        from: "firm",
        startWith: "$parentFirmId",
        connectFromField: "parentFirmId",
        connectToField: "abbreviation",
        as: "parentFirms",
        restrictSearchWithMatch: {
            "isPartOfGroup": "IsPartOfGroupType_No"
        }
    }},
    {
        $addFields:{
            parentFirms:{
                $filter:{
                    input:"$parentFirms",
                    as:'parentFirm',
                    cond:{
                        $eq:['$$parentFirm.fiscalYear','$fiscalYear']
                    }
                }
            }
        }
    },{
        $lookup: {
            from: "firm",
                        let: {
                firmGroupId: "$firmGroupId",
                firmFiscalYear:'$fiscalYear'
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [{
                            $eq: ["$fiscalYear", "$$firmFiscalYear"]
                        },{
                            $ne: ["$abbreviation", null]
                        }, {
                            $eq: ["$$firmGroupId", "$abbreviation"]
                        }]
                    }
                }
            }],
            as: "firmGroup"
        }
    },{
        $unwind:{
            path:'$firmGroup',
            preserveNullAndEmptyArrays:true
        }
    },{  
        $graphLookup: {
            from: "firm",
            startWith: "$firmGroup.parentFirmId",
            connectFromField: "parentFirmId",
            connectToField: "abbreviation",
            as: "groupParentFirms",
            restrictSearchWithMatch: {
                "isPartOfGroup": "IsPartOfGroupType_No"
            }
        }
    },
    {
        $addFields:{
            groupParentFirms:{
                $filter:{
                    input:"$groupParentFirms",
                    as:'groupParentFirm',
                    cond:{
                        $eq:['$$groupParentFirm.fiscalYear','$fiscalYear']
                    }
                }
            }
        }
    },{
        $lookup:{
            from:'title',
            let:{
                ultimateResponsibility:'$ultimateResponsibility',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    $expr:{
                        $and:[{
                            // $eq:['$$ultimateResponsibility','$_id']
                            $in: [{$toString:'$_id'}, { $ifNull: ['$$ultimateResponsibility', []] }]
                        },{
                            $eq:["$fiscalYear", '$$titleFiscalYear']
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'ultimateResponsibility'
        }
    },{
        $lookup:{
            from:'title',
            let:{
                operationalResponsibilitySqm:'$operationalResponsibilitySqm',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$operationalResponsibilitySqm','$_id']
                    // }
                    $expr:{
                        $and:[{
                            // $eq:['$$operationalResponsibilitySqm','$_id']
                            $in: [{$toString:'$_id'}, { $ifNull: ['$$operationalResponsibilitySqm', []] }]
                        },{
                            $eq:["$fiscalYear", "$$titleFiscalYear"]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'operationalResponsibilitySqm'
        }
    },{
        $lookup:{
            from:'title',
            let:{
                orIndependenceRequirement:'$orIndependenceRequirement',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$orIndependenceRequirement','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$orIndependenceRequirement','$_id']
                        },{
                            $eq:["$fiscalYear", "$$titleFiscalYear"]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'orIndependenceRequirement'
        }
    },{
        $unwind:{
            path:'$orIndependenceRequirement',
            preserveNullAndEmptyArrays:true
        }
    },{
        $lookup:{
            from:'title',
            let:{
                orMonitoringRemediation:'$orMonitoringRemediation',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$orMonitoringRemediation','$_id']
                    // }
                    $expr:{
                                $and:[{
                                    $eq:['$$orMonitoringRemediation','$_id']
                                },{
                                    $eq:["$fiscalYear", "$$titleFiscalYear"]
                                }]
                        }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'orMonitoringRemediation'
        }
    },{
        $unwind:{
            path:'$orMonitoringRemediation',
            preserveNullAndEmptyArrays:true
        }
    },
    {
        $lookup:{
            from:'title',
            let:{
                pcaobUltimateResponsibility:'$pcaobUltimateResponsibility',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$operationalResponsibilitySqm','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$pcaobUltimateResponsibility','$_id']
                        },{
                            $eq:["$fiscalYear", "$$titleFiscalYear"]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'pcaobUltimateResponsibility'
        }
    },{
        $unwind:{
            path:'$pcaobUltimateResponsibility',
            preserveNullAndEmptyArrays:true
        }
    },{
        $lookup:{
            from:'title',
            let:{
                pcaobOperationalResponsibilitySqm:'$pcaobOperationalResponsibilitySqm',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$operationalResponsibilitySqm','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$pcaobOperationalResponsibilitySqm','$_id']
                        },{
                            $eq:["$fiscalYear", "$$titleFiscalYear"]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'pcaobOperationalResponsibilitySqm'
        }
    },{
        $unwind:{
            path:'$pcaobOperationalResponsibilitySqm',
            preserveNullAndEmptyArrays:true
        }
    },{
        $lookup:{
            from:'title',
            let:{
                pcaobOrIndependenceRequirement:'$pcaobOrIndependenceRequirement',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'

            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$operationalResponsibilitySqm','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$pcaobOrIndependenceRequirement','$_id']
                        },{
                            $eq:["$fiscalYear", "$$titleFiscalYear"]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'

                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'pcaobOrIndependenceRequirement'
        }
    },{
        $unwind:{
            path:'$pcaobOrIndependenceRequirement',
            preserveNullAndEmptyArrays:true
        }
    },{
        $lookup:{
            from:'title',
            let:{
                pcaobOrMonitoringRemediation:'$pcaobOrMonitoringRemediation',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'

            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$operationalResponsibilitySqm','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$pcaobOrMonitoringRemediation','$_id']
                        },{
                            $eq:["$fiscalYear", "$$titleFiscalYear"]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'

                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'pcaobOrMonitoringRemediation'
        }
    },{
        $unwind:{
            path:'$pcaobOrMonitoringRemediation',
            preserveNullAndEmptyArrays:true
        }
    },{
        $lookup:{
            from:'title',
            let:{
                pcaobOrMonitoringRemediationComponents:'$pcaobOrMonitoringRemediationComponents',
                firmId:'$firmGroupId',
                titleFiscalYear:'$fiscalYear'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$operationalResponsibilitySqm','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$pcaobOrMonitoringRemediationComponents','$_id']
                        },{
                            $eq:["$fiscalYear", "$$titleFiscalYear"]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'},
                        titleAssignmentFiscalYear:'$fiscalYear'

                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", "$$titleAssignmentFiscalYear"]
                                }]
                            }
                        }
                    }],
                    as:'titleAssignments'
                }
            },{
                $unwind:{
                    path:'$titleAssignments',
                    preserveNullAndEmptyArrays:true
                }
            }],
            as:'pcaobOrMonitoringRemediationComponents'
        }
    },{
        $unwind:{
            path:'$pcaobOrMonitoringRemediationComponents',
            preserveNullAndEmptyArrays:true
        }
    },
    {
        $lookup:{
            from:'country',
            //localField:'country',
            //foreignField:'_id',
            let:{
                countryck:'$country',
                countryFiscalYear:'$fiscalYear'
            },
            pipeline: [{
                        $match: {
                            $expr: {
                                $and: [
                                    {
                                        $eq: [ "$_id",  "$$countryck" ] ,
                                    },
                                    {
                                        $in: ["$$countryFiscalYear", "$fiscalYear"]
                                    }
                                ]
                            }
                        }
                    }],
            as: "country"
        }
    },{
        $unwind:{
            path:'$country',
            preserveNullAndEmptyArrays:true
        }
    },
    {
        $lookup: {
            from: 'title',
            let: {
                pcaobOperationalResponsibilityGovernanceAndLeadership: '$pcaobOperationalResponsibilityGovernanceAndLeadership',
                firmId: '$firmGroupId',
                titleFiscalYear: '$fiscalYear'
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [
                            { $eq: ['$$pcaobOperationalResponsibilityGovernanceAndLeadership', '$_id'] },
                            { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                        ]
                    }
                }
            }, {
                $lookup: {
                    from: 'titleassignment',
                    let: {
                        titleId: { $toString: '$_id' },
                        titleAssignmentFiscalYear: '$fiscalYear'
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$$titleId', '$titleId'] },
                                    { $eq: ['$$firmId', '$firmId'] },
                                    { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                ]
                            }
                        }
                    }],
                    as: 'titleAssignments'
                }
            }, {
                $unwind: {
                    path: '$titleAssignments',
                    preserveNullAndEmptyArrays: true
                }
            }],
            as: 'pcaobOperationalResponsibilityGovernanceAndLeadership'
        }
    },
    {
        $unwind: {
            path: '$pcaobOperationalResponsibilityGovernanceAndLeadership',
            preserveNullAndEmptyArrays: true
        }
    },
    {
        $lookup: {
            from: 'title',
            let: {
                pcaobOperationalResponsibilityAcceptanceAndContinuance: '$pcaobOperationalResponsibilityAcceptanceAndContinuance',
                firmId: '$firmGroupId',
                titleFiscalYear: '$fiscalYear'
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [
                            { $eq: ['$$pcaobOperationalResponsibilityAcceptanceAndContinuance', '$_id'] },
                            { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                        ]
                    }
                }
            }, {
                $lookup: {
                    from: 'titleassignment',
                    let: {
                        titleId: { $toString: '$_id' },
                        titleAssignmentFiscalYear: '$fiscalYear'
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$$titleId', '$titleId'] },
                                    { $eq: ['$$firmId', '$firmId'] },
                                    { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                ]
                            }
                        }
                    }],
                    as: 'titleAssignments'
                }
            }, {
                $unwind: {
                    path: '$titleAssignments',
                    preserveNullAndEmptyArrays: true
                }
            }],
            as: 'pcaobOperationalResponsibilityAcceptanceAndContinuance'
        }
    },
    {
        $unwind: {
            path: '$pcaobOperationalResponsibilityAcceptanceAndContinuance',
            preserveNullAndEmptyArrays: true
        }
    },

    {
        $lookup: {
            from: 'title',
            let: {
                pcaobOperationalResponsibilityEngagementPerformance: '$pcaobOperationalResponsibilityEngagementPerformance',
                firmId: '$firmGroupId',
                titleFiscalYear: '$fiscalYear'
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [
                            { $eq: ['$$pcaobOperationalResponsibilityEngagementPerformance', '$_id'] },
                            { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                        ]
                    }
                }
            }, {
                $lookup: {
                    from: 'titleassignment',
                    let: {
                        titleId: { $toString: '$_id' },
                        titleAssignmentFiscalYear: '$fiscalYear'
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$$titleId', '$titleId'] },
                                    { $eq: ['$$firmId', '$firmId'] },
                                    { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                ]
                            }
                        }
                    }],
                    as: 'titleAssignments'
                }
            }, {
                $unwind: {
                    path: '$titleAssignments',
                    preserveNullAndEmptyArrays: true
                }
            }],
            as: 'pcaobOperationalResponsibilityEngagementPerformance'
        }
    },
    {
        $unwind: {
            path: '$pcaobOperationalResponsibilityEngagementPerformance',
            preserveNullAndEmptyArrays: true
        }
    },

    {
        $lookup: {
            from: 'title',
            let: {
                pcaobOperationalResponsibilityResources: '$pcaobOperationalResponsibilityResources',
                firmId: '$firmGroupId',
                titleFiscalYear: '$fiscalYear'
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [
                            { $eq: ['$$pcaobOperationalResponsibilityResources', '$_id'] },
                            { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                        ]
                    }
                }
            }, {
                $lookup: {
                    from: 'titleassignment',
                    let: {
                        titleId: { $toString: '$_id' },
                        titleAssignmentFiscalYear: '$fiscalYear'
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$$titleId', '$titleId'] },
                                    { $eq: ['$$firmId', '$firmId'] },
                                    { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                ]
                            }
                        }
                    }],
                    as: 'titleAssignments'
                }
            }, {
                $unwind: {
                    path: '$titleAssignments',
                    preserveNullAndEmptyArrays: true
                }
            }],
            as: 'pcaobOperationalResponsibilityResources'
        }
    },
    {
        $unwind: {
            path: '$pcaobOperationalResponsibilityResources',
            preserveNullAndEmptyArrays: true
        }
    },

    {
        $lookup: {
            from: 'title',
            let: {
                pcaobOperationalResponsibilityInformationAndCommunication: '$pcaobOperationalResponsibilityInformationAndCommunication',
                firmId: '$firmGroupId',
                titleFiscalYear: '$fiscalYear'
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [
                            { $eq: ['$$pcaobOperationalResponsibilityInformationAndCommunication', '$_id'] },
                            { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                        ]
                    }
                }
            }, {
                $lookup: {
                    from: 'titleassignment',
                    let: {
                        titleId: { $toString: '$_id' },
                        titleAssignmentFiscalYear: '$fiscalYear'
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$$titleId', '$titleId'] },
                                    { $eq: ['$$firmId', '$firmId'] },
                                    { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                ]
                            }
                        }
                    }],
                    as: 'titleAssignments'
                }
            }, {
                $unwind: {
                    path: '$titleAssignments',
                    preserveNullAndEmptyArrays: true
                }
            }],
            as: 'pcaobOperationalResponsibilityInformationAndCommunication'
        }
    },
    {
        $unwind: {
            path: '$pcaobOperationalResponsibilityInformationAndCommunication',
            preserveNullAndEmptyArrays: true
        }
    },
    {
        $project:{
            _id:0,
            "Type":{
                $switch:{
                    branches:[
                        {case:{$eq:['$type',"EntityType_Area"]},then:"Area"},
                        {case:{$eq:['$type',"EntityType_Cluster"]},then:"Cluster"},
                        {case:{$eq:['$type',"EntityType_Group"]},then:"Location"},
                        {case:{$eq:['$type',"EntityType_MemberFirm"]},then:"Member Firm"},
                        {case:{$eq:['$type',"EntityType_Network"]},then:"Network"},
                        {case:{$eq:['$type',"EntityType_BusinessUnit"]},then:"Business Unit"},
                        {case:{$eq:['$type',"EntityType_SubCluster"]},then:"Sub-Cluster"},
                        {case:{$eq:['$type',"EntityType_Region"]},then:"Region"}
                    ],
                    default:''
                }
            },
            "EYNetworkUniqueId": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Network"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "EYNetwork": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Network"]},then:"$$this.name",else:""}}]}
                }
            },
            "EYAreaUniqueId": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Area"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "EYArea": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Area"]},then:"$$this.name",else:""}}]}
                }
            },
            "EYBusinessUnitUniqueId": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_BusinessUnit"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "EYBusinessUnit": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_BusinessUnit"]},then:"$$this.name",else:""}}]}
                }
            },
            "EYClusterUniqueId": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Cluster"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "EYCluster": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Cluster"]},then:"$$this.name",else:""}}]}
                }
            },
            "EYSubClusterUniqueId": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_SubCluster"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "EYSubCluster": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_SubCluster"]},then:"$$this.name",else:""}}]}
                }
            },
            "EYRegionUniqueId": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Region"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "EYRegion": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Region"]},then:"$$this.name",else:""}}]}
                }
            },
            "GroupRegionUniqueId": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Region"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "GroupRegion": {
                $reduce:{
                    input:"$parentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Region"]},then:"$$this.name",else:""}}]}
                }
            },
            "GroupNetworkUniqueId": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Network"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "GroupNetwork": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Network"]},then:"$$this.name",else:""}}]}
                }
            },
            "GroupAreaUniqueId": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Area"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "GroupArea": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Area"]},then:"$$this.name",else:""}}]}
                }
            },
            "GroupBusinessUnitUniqueId": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_BusinessUnit"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "GroupBusinessUnit": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_BusinessUnit"]},then:"$$this.name",else:""}}]}
                }
            }, 
            "GroupClusterUniqueId": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Cluster"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "GroupCluster": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Cluster"]},then:"$$this.name",else:""}}]}
                }
            },
            "GroupSubClusterUniqueId": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_SubCluster"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "GroupSubCluster": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_SubCluster"]},then:"$$this.name",else:""}}]}
                }
            },
            "Name":"$name",
            "Archived":{$cond:{if:{$eq:['$isReadOnly',true]},then:'Yes',else:'No'}},
            "IsPartOfGroup":{$cond:{if:{$eq:['$isPartOfGroup','IsPartOfGroupType_Yes']},then:'Yes',else:'No'}},
            "GroupName":'$firmGroup.name',
            "GcdId":{$cond:{if:{$eq:['$type',"EntityType_Group"]},then:'N/A',else:'$gcdId'}},
            "FirmUniqueId":"$_id",
            "Abbreviation":"$abbreviation",
            "FirmPublishedDate":"$publishedDate",
            "FirstFirmPublishedDate":"$firstPublishedDate",
            "Location":{$cond:{if:{$eq:['$type',"EntityType_Group"]},then:'N/A',else:'$country.name'}},
            "UltimateResponsibilityQCTitle":{$cond:{if:'$pcaobUltimateResponsibility.name',then:'$pcaobUltimateResponsibility.name',else:''}},
            "UltimateResponsibilityQCassignments":{
                $cond:{
                    if:'$pcaobUltimateResponsibility',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobUltimateResponsibility.titleAssignments.assignments'},then:'$pcaobUltimateResponsibility.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "UltimateResponsibilityQCassignmentsName":{
              $cond:{
                    if:'$pcaobUltimateResponsibility',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobUltimateResponsibility.titleAssignments.assignments'},then:'$pcaobUltimateResponsibility.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:''}},'$$this.displayName',': ','$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsiblityQCTitle":{$cond:{if:'$pcaobOperationalResponsibilitySqm.name',then:'$pcaobOperationalResponsibilitySqm.name',else:''}},
            "OperationalResponsiblityQCassignments":{
                $cond:{
                    if:'$pcaobOperationalResponsibilitySqm',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOperationalResponsibilitySqm.titleAssignments.assignments'},then:'$pcaobOperationalResponsibilitySqm.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsiblityQCassignmentsName":{
                $cond:{
                    if:'$pcaobOperationalResponsibilitySqm',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOperationalResponsibilitySqm.titleAssignments.assignments'},then:'$pcaobOperationalResponsibilitySqm.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.displayName',': ','$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsibilityComplianceWithRequirementQCTitle":{$cond:{if:'$pcaobOrIndependenceRequirement.name',then:'$pcaobOrIndependenceRequirement.name',else:''}},
            "OperationalResponsibilityComplianceWithRequirementQCAssignments":{
                $cond:{
                    if:'$pcaobOrIndependenceRequirement',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOrIndependenceRequirement.titleAssignments.assignments'},then:'$pcaobOrIndependenceRequirement.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsibilityComplianceWithRequirementQCAssignmentsName":{
                $cond:{
                    if:'$pcaobOrIndependenceRequirement',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOrIndependenceRequirement.titleAssignments.assignments'},then:'$pcaobOrIndependenceRequirement.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.displayName',': ','$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsibilityMonitoringAndRemediationQCTitle":{$cond:{if:'$pcaobOrMonitoringRemediation.name',then:'$pcaobOrMonitoringRemediation.name',else:''}},
            "OperationalResponsibilityMonitoringAndRemediationQCAssignments":{
                $cond:{
                    if:'$pcaobOrMonitoringRemediation',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOrMonitoringRemediation.titleAssignments.assignments'},then:'$pcaobOrMonitoringRemediation.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsibilityMonitoringAndRemediationQCAssignmentsName":{
                $cond:{
                    if:'$pcaobOrMonitoringRemediation',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOrMonitoringRemediation.titleAssignments.assignments'},then:'$pcaobOrMonitoringRemediation.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.displayName',': ','$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "IndividualoperationalresponsibilityQCTitle":{$cond:{if:'$pcaobOrMonitoringRemediationComponents.name',then:'$pcaobOrMonitoringRemediationComponents.name',else:''}},
            "IndividualoperationalresponsibilityQCassignments":{
                $cond:{
                    if:'$pcaobOrMonitoringRemediationComponents',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOrMonitoringRemediationComponents.titleAssignments.assignments'},then:'$pcaobOrMonitoringRemediationComponents.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "IndividualoperationalresponsibilityQCassignmentsName":{
                $cond:{
                    if:'$pcaobOrMonitoringRemediationComponents',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$pcaobOrMonitoringRemediationComponents.titleAssignments.assignments'},then:'$pcaobOrMonitoringRemediationComponents.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:' '}},'$$this.displayName',': ','$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "UltimateResponsibilitySqmTitle": {
                $reduce: {
                    // input: {ifNull: ['$ultimateResponsibility', []]},
                    input: {$cond: {if: {$isArray: '$ultimateResponsibility'}, then: '$ultimateResponsibility', else: []}},
                    initialValue:"",
                    in: {
                        $concat: ["$$value", {$cond: {if: {$eq: ['$$value', '']}, then: '', else: '; '}}, '$$this.name']
                    }
                }
            },
            "UltimateResponsibilitySqmAssignments": {
                $let: {
                    vars: {
                        allAssignments: {
                            $reduce: {
                                input: {$cond: {if: {$isArray: '$ultimateResponsibility'}, then: '$ultimateResponsibility', else: []}},
                                initialValue: [],
                                in: {
                                    $concatArrays: [
                                        '$$value',
                                        {$cond: {if: {$isArray: '$$this.titleAssignments.assignments'}, then: '$$this.titleAssignments.assignments', else: []}}
                                    ]
                                }
                            }
                        }
                    },
                    in:{
                        $cond: {
                            if: {$gt: [{$size: '$$allAssignments'}, 0]},
                            then: {
                                $reduce: {
                                    input: '$$allAssignments',
                                    initialValue: '',
                                    in: {
                                        $concat: [
                                            '$$value',
                                            {$cond: {if: {$ne: ['$$value', '']}, then: '; ', else: ''}},
                                            '$$this.email'
                                        ]
                                    }
                                }
                            },
                            else: 'no assignment'
                        }
                    }
                }
            },
            "OperationalResponsibilitySqmTitle": {
                $reduce: {
                    input: {$cond: {if: {$isArray: '$operationalResponsibilitySqm'}, then: '$operationalResponsibilitySqm', else: []}},
                    initialValue:"",
                    in: {
                        $concat: ["$$value", {$cond: {if: {$eq: ['$$value', '']}, then: '', else: '; '}}, '$$this.name']
                    }
                }
            },
            "OperationalResponsibilitySqmAssignments": {
                $let: {
                    vars: {
                        allAssignments: {
                            $reduce: {
                                input: {$cond: {if: {$isArray: '$operationalResponsibilitySqm'}, then: '$operationalResponsibilitySqm', else: []}},
                                initialValue: [],
                                in: {
                                    $concatArrays: [
                                        '$$value',
                                        {$cond: {if: {$isArray: '$$this.titleAssignments.assignments'}, then: '$$this.titleAssignments.assignments', else: []}}
                                    ]
                                }
                            }
                        }
                    },
                    in:{
                        $cond: {
                            if: {$gt: [{$size: '$$allAssignments'}, 0]},
                            then: {
                                $reduce: {
                                    input: '$$allAssignments',
                                    initialValue: '',
                                    in: {
                                        $concat: [
                                            '$$value',
                                            {$cond: {if: {$ne: ['$$value', '']}, then: '; ', else: ''}},
                                            '$$this.email'
                                        ]
                                    }
                                }
                            },
                            else: 'no assignment'
                        }
                    }
                }
            },
            "OperationalResponsibilityComplianceWithRequirementTitle":{$cond:{if:'$orIndependenceRequirement.name',then:'$orIndependenceRequirement.name',else:''}},
            "OperationalResponsibilityComplianceWithRequirementAssignments":{
                $cond:{
                    if:'$orIndependenceRequirement',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$orIndependenceRequirement.titleAssignments.assignments'},then:'$orIndependenceRequirement.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:''}},'$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsibilityMonitoringAndRemediationTitle":{$cond:{if:'$orMonitoringRemediation.name',then:'$orMonitoringRemediation.name',else:''}},
            "OperationalResponsibilityMonitoringAndRemediationAssignments":{
                $cond:{
                    if:'$orMonitoringRemediation',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$orMonitoringRemediation.titleAssignments.assignments'},then:'$orMonitoringRemediation.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:''}},'$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "UltimateResponsibilitySqmAssignmentsName":{
                $let: {
                    vars: {
                        allAssignments: {
                            $reduce: {
                                input: {$cond: {if: {$isArray: '$ultimateResponsibility'}, then: '$ultimateResponsibility', else: []}},
                                initialValue: [],
                                in: {
                                    $concatArrays: [
                                        '$$value', 
                                        {$cond: {if: {$isArray: '$$this.titleAssignments.assignments'}, then: '$$this.titleAssignments.assignments', else: []}}
                                    ]
                                }
                            }
                        }
                    },
                    in: {
                        $cond: {
                            if: {$gt: [{$size: '$$allAssignments'}, 0]},
                            then: {
                                $reduce: {
                                    input: '$$allAssignments',
                                    initialValue: '',
                                    in: {
                                        $concat: [
                                            '$$value',
                                            {$cond: {if: {$ne: ['$$value', '']}, then: '; ', else: ''}},
                                            '$$this.displayName',
                                            ': ',
                                            '$$this.email'
                                        ]

                                    }
                                }
                            },
                            else: 'no assignment'
                        }
                    }
                }
            },
            "OperationalResponsibilitySqmAssignmentsName":{
                $let: {
                    vars: {
                        allAssignments: {
                            $reduce: {
                                input: {$cond: {if: {$isArray: '$operationalResponsibilitySqm'}, then: '$operationalResponsibilitySqm', else: []}},
                                initialValue: [],
                                in: {
                                    $concatArrays: [
                                        '$$value', 
                                        {$cond: {if: {$isArray: '$$this.titleAssignments.assignments'}, then: '$$this.titleAssignments.assignments', else: []}}
                                    ]
                                }
                            }
                        }
                    },
                    in: {
                        $cond: {
                            if: {$gt: [{$size: '$$allAssignments'}, 0]},
                            then: {
                                $reduce: {
                                    input: '$$allAssignments',
                                    initialValue: '',
                                    in: {
                                        $concat: [
                                            '$$value',
                                            {$cond: {if: {$ne: ['$$value', '']}, then: '; ', else: ''}},
                                            '$$this.displayName',
                                            ': ',
                                            '$$this.email'
                                        ]

                                    }
                                }
                            },
                            else: 'no assignment'
                        }
                    }
                }
            },
            "OperationalResponsibilityComplianceWithRequirementAssignmentsName":{
                $cond:{
                    if:'$orIndependenceRequirement',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$orIndependenceRequirement.titleAssignments.assignments'},then:'$orIndependenceRequirement.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:''}},'$$this.displayName',': ','$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "OperationalResponsibilityMonitoringAndRemediationAssignmentsName":{
                $cond:{
                    if:'$orMonitoringRemediation',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$orMonitoringRemediation.titleAssignments.assignments'},then:'$orMonitoringRemediation.titleAssignments.assignments',else:[]}}
                            },
                            in:{
                                $cond:{
                                    if:{$gt:[{$size:'$$assignments'},0]},
                                    then:{
                                        $reduce:{
                                            input:'$$assignments',
                                            initialValue:'',
                                            in:{$concat:['$$value',{$cond:{if:{$ne:['','$$value']},then:'; ',else:''}},'$$this.displayName',': ','$$this.email']}
                                        }
                                    },
                                    else:'no assignment'
                                }
                            }
                        }
                    },
                    else:''
                }
            },
            "IndividualOperationalResponsibilityQCGovernanceAndLeadershipTitle": {
                $cond: { if: '$pcaobOperationalResponsibilityGovernanceAndLeadership.name', then: '$pcaobOperationalResponsibilityGovernanceAndLeadership.name', else: '' }
            },
            "IndividualOperationalResponsibilityQCGovernanceAndLeadershipAssignments": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityGovernanceAndLeadership',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityGovernanceAndLeadership.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityGovernanceAndLeadership.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },
            "IndividualOperationalResponsibilityQCGovernanceAndLeadershipAssignmentsName": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityGovernanceAndLeadership',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityGovernanceAndLeadership.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityGovernanceAndLeadership.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.displayName',
                                                    ': ',
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },
            "IndividualOperationalResponsibilityQCAcceptanceAndContinuanceOfEngagementsTitle": {
                $cond: { if: '$pcaobOperationalResponsibilityAcceptanceAndContinuance.name', then: '$pcaobOperationalResponsibilityAcceptanceAndContinuance.name', else: '' }
            },
            "IndividualOperationalResponsibilityQCAcceptanceAndContinuanceOfEngagementsAssignments": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityAcceptanceAndContinuance',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityAcceptanceAndContinuance.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityAcceptanceAndContinuance.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },
            "IndividualOperationalResponsibilityQCAcceptanceAndContinuanceOfEngagementsAssignmentsName": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityAcceptanceAndContinuance',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityAcceptanceAndContinuance.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityAcceptanceAndContinuance.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.displayName',
                                                    ': ',
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },

            "IndividualOperationalResponsibilityQCEngagementPerformanceTitle": {
                $cond: { if: '$pcaobOperationalResponsibilityEngagementPerformance.name', then: '$pcaobOperationalResponsibilityEngagementPerformance.name', else: '' }
            },
            "IndividualOperationalResponsibilityQCEngagementPerformanceAssignments": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityEngagementPerformance',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityEngagementPerformance.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityEngagementPerformance.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },
            "IndividualOperationalResponsibilityQCEngagementPerformanceAssignmentsName": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityEngagementPerformance',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityEngagementPerformance.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityEngagementPerformance.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.displayName',
                                                    ': ',
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },

            "IndividualOperationalResponsibilityQCResourcesTitle": {
                $cond: { if: '$pcaobOperationalResponsibilityResources.name', then: '$pcaobOperationalResponsibilityResources.name', else: '' }
            },
            "IndividualOperationalResponsibilityQCResourcesAssignments": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityResources',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityResources.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityResources.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },
            "IndividualOperationalResponsibilityQCResourcesAssignmentsName": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityResources',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityResources.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityResources.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.displayName',
                                                    ': ',
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },

            "IndividualOperationalResponsibilityQCInformationAndCommunicationTitle": {
                $cond: { if: '$pcaobOperationalResponsibilityInformationAndCommunication.name', then: '$pcaobOperationalResponsibilityInformationAndCommunication.name', else: '' }
            },
            "IndividualOperationalResponsibilityQCInformationAndCommunicationAssignments": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityInformationAndCommunication',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityInformationAndCommunication.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityInformationAndCommunication.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },
            "IndividualOperationalResponsibilityQCInformationAndCommunicationAssignmentsName": {
                $cond: {
                    if: '$pcaobOperationalResponsibilityInformationAndCommunication',
                    then: {
                        $let: {
                            vars: {
                                assignments: {
                                    $cond: {
                                        if: { $isArray: '$pcaobOperationalResponsibilityInformationAndCommunication.titleAssignments.assignments' },
                                        then: '$pcaobOperationalResponsibilityInformationAndCommunication.titleAssignments.assignments',
                                        else: []
                                    }
                                }
                            },
                            in: {
                                $cond: {
                                    if: { $gt: [{ $size: '$$assignments' }, 0] },
                                    then: {
                                        $reduce: {
                                            input: '$$assignments',
                                            initialValue: '',
                                            in: {
                                                $concat: [
                                                    '$$value',
                                                    { $cond: { if: { $ne: ['', '$$value'] }, then: '; ', else: ' ' } },
                                                    '$$this.displayName',
                                                    ': ',
                                                    '$$this.email'
                                                ]
                                            }
                                        }
                                    },
                                    else: 'no assignment'
                                }
                            }
                        }
                    },
                    else: ''
                }
            },
            "FirmGroupId":"$firmGroupId",          
            "UltimateResponsibilityTitleId": {$cond:{if:'$ultimateResponsibility._id',then:'$ultimateResponsibility._id',else:''}},
            "OperationalResponsibilitySqmTitleId": {$cond:{if:'$operationalResponsibilitySqm._id',then:'$operationalResponsibilitySqm._id',else:''}},
            "OrIndependenceRequirementTitleId": {$cond:{if:'$orIndependenceRequirement._id',then:'$orIndependenceRequirement._id',else:''}},
            "OrMonitoringRemediationTitleId": {$cond:{if:'$orMonitoringRemediation._id',then:'$orMonitoringRemediation._id',else:''}},
            "MemberFirmId":{$cond:{if:{$eq:['$type','EntityType_MemberFirm']},then:'$memberFirmId',else:''}},
            "ArcherPublishedOn" : new Date().toISOString(),
            "Rollforwarded":{$cond:{if:{$eq:['$isRollFwdComplete',true]},then:'Yes',else:'No'}},
            "Published":{$cond:{if:{$eq:['$isPublishQueryRun',true]},then:'Yes',else:'No'}},
            "RollforwardedDate":"$rollForwardDate",
            "ArchivedDate":"$archivedDate",
            "PCAOBEnabled":{$cond:{if:{$eq:['$categoryType',"EntityType_PCAOB"]},then:'Yes',else:'No'}},
            "EntityFiscalYear":'$fiscalYear'

        }
    },{
        $out:'archerentitiestemp'
    }])


    db.archerentitiestemp.aggregate([
        {
            $project:{
            _id:0
            }
        },{
            $addFields:{
                SQMLeadership:
                {$concat:[
                    "$UltimateResponsibilitySqmAssignments",
                {$cond:{if:{$eq:["$OperationalResponsibilitySqmAssignments",'']}, then : '',else:';'}},
                    "$OperationalResponsibilitySqmAssignments",
                {$cond:{if:{$eq:["$OperationalResponsibilityComplianceWithRequirementAssignments",'']}, then : '',else:';'}},
                    "$OperationalResponsibilityComplianceWithRequirementAssignments",
                {$cond:{if:{$eq:["$OperationalResponsibilityMonitoringAndRemediationAssignments",'']}, then : '',else:';'}},
                    "$OperationalResponsibilityMonitoringAndRemediationAssignments"
                ]},
                SQMPCAOBLeadership:
                {$concat:[
                    "$UltimateResponsibilityQCassignments",
                {$cond:{if:{$eq:["$OperationalResponsiblityQCassignments",'']}, then : '',else:';'}},
                    "$OperationalResponsiblityQCassignments",
                {$cond:{if:{$eq:["$OperationalResponsibilityComplianceWithRequirementQCAssignments",'']}, then : '',else:';'}},
                    "$OperationalResponsibilityComplianceWithRequirementQCAssignments",
                {$cond:{if:{$eq:["$OperationalResponsibilityMonitoringAndRemediationQCAssignments",'']}, then : '',else:';'}},
                    "$OperationalResponsibilityMonitoringAndRemediationQCAssignments",
                {$cond:{if:{$eq:["$IndividualoperationalresponsibilityQCassignments",'']}, then : '',else:';'}},
                    "$IndividualoperationalresponsibilityQCassignments",
                {$cond:{if:{$eq:["$IndividualOperationalResponsibilityQCGovernanceAndLeadershipAssignments",'']}, then : '',else:';'}},
                    "$IndividualOperationalResponsibilityQCGovernanceAndLeadershipAssignments",
                {$cond:{if:{$eq:["$IndividualOperationalResponsibilityQCAcceptanceAndContinuanceOfEngagementsAssignments",'']}, then : '',else:';'}},
                    "$IndividualOperationalResponsibilityQCAcceptanceAndContinuanceOfEngagementsAssignments",
                {$cond:{if:{$eq:["$IndividualOperationalResponsibilityQCEngagementPerformanceAssignments",'']}, then : '',else:';'}},
                    "$IndividualOperationalResponsibilityQCEngagementPerformanceAssignments",
                {$cond:{if:{$eq:["$IndividualOperationalResponsibilityQCResourcesAssignments",'']}, then : '',else:';'}},
                    "$IndividualOperationalResponsibilityQCResourcesAssignments",
                {$cond:{if:{$eq:["$IndividualOperationalResponsibilityQCInformationAndCommunicationAssignments",'']}, then : '',else:';'}},
                    "$IndividualOperationalResponsibilityQCInformationAndCommunicationAssignments"
                ]},
                "FiscalYear": {$concat:['FY',{$substr:['$EntityFiscalYear',2,2]}]}
            }
        },
        {
            $out:'archerentities'
        }
    ])
    
    //Event logs   
    
    var calc7Days = 7 * 24 * 60 * 60 * 1000;
    db.archerentities.find().forEach(function (entity){
        var existingFirm = db.archerentitiesmaster.findOne({FirmUniqueId :entity.FirmUniqueId});
        
        // To find for any update to entity
        var updatedEventFirm = existingFirm && db.event.find({actor: entity.FirmUniqueId,actorType : "firm",fiscalYear: entity.EntityFiscalYear, message: {$in:['ActionType_Update','ActionType_Publish','ActionType_Rollforward','ActionType_Archive']},modifiedOn : {$gt:existingFirm.ArcherPublishedOn}}).sort({'modifiedOn':-1}).limit(1).toArray();
        // Memeber firms: To find for any Title assignment updated
        if(existingFirm && !updatedEventFirm.length>0)
        {
            if(entity.Type === "Member Firm")
            {
                // Create an array of titles to include in the query
                var titles = [];
                if (entity.UltimateResponsibilityTitleId) {
                    titles.push(entity.UltimateResponsibilityTitleId);
                }
                if (entity.OperationalResponsibilitySqmTitleId) {
                    titles.push(entity.OperationalResponsibilitySqmTitleId);
                }
                if (entity.OrIndependenceRequirementTitleId) {
                    titles.push(entity.OrIndependenceRequirementTitleId);
                }
                if (entity.OrMonitoringRemediationTitleId) {
                    titles.push(entity.OrMonitoringRemediationTitleId);
                }
                var queryTitleUpdate = {
                    publisher: entity.FirmGroupId,
                    actor: { $in: titles },
                    actorType: { $in: ['GlobalTitle','CustomTitle'] },
                    fiscalYear: entity.EntityFiscalYear,
                    message: { $in: ["ActionType_Title_Users_Add", "ActionType_Title_Users_Update", "ActionType_Title_Users_Remove"] },
                    modifiedOn: { $gt: existingFirm.ArcherPublishedOn }
                };
    
                updatedEventFirm = db.event.find(queryTitleUpdate).sort({ modifiedOn: -1 }).limit(1).toArray();
            }
        }

        if (!(existingFirm)) {
            var getNewEventDateFirm = db.event.find({actor: entity.FirmUniqueId,actorType : "firm",fiscalYear: entity.EntityFiscalYear, message: 'ActionType_Add'}).sort({'modifiedOn':-1}).limit(1).toArray();
            db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:'New',EventAction : 'Update', LastPublishedRecordStatus : 'Active', EventDate : getNewEventDateFirm.length>0?getNewEventDateFirm[0].modifiedOn:'', LastFlagProcessedDate:entity.ArcherPublishedOn}});
            }
        else if (updatedEventFirm.length>0) {
            db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:'Updated',EventAction : 'Update', LastPublishedRecordStatus : 'Active', EventDate : updatedEventFirm[0].modifiedOn, LastFlagProcessedDate:entity.ArcherPublishedOn}})
            }
    //    else
    //     {
    //         db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:existingFirm.EventType,EventAction : existingFirm.EventAction, LastPublishedRecordStatus : existingFirm.LastPublishedRecordStatus, EventDate : existingFirm.EventDate, LastFlagProcessedDate:existingFirm.LastFlagProcessedDate}})
    //     }
    })

    db.archerentitiesmaster.find().forEach(function(entity){
        var existingFirm = db.archerentities.findOne({FirmUniqueId :entity.FirmUniqueId});
        var isFirmDelete = db.event.find({actor: entity.FirmUniqueId,actorType : "firm",fiscalYear: entity.EntityFiscalYear, message: 'ActionType_Delete',modifiedOn : {$gt:entity.ArcherPublishedOn}}).sort({'modifiedOn':-1}).limit(1).toArray();
        if(!existingFirm && isFirmDelete.length>0){
            entity.EventType = 'Deleted';
            entity.EventAction = 'Update';
            entity.LastPublishedRecordStatus= 'Inactive';
            entity.EventDate = isFirmDelete[0].modifiedOn;
            entity.LastFlagProcessedDate = entity.ArcherPublishedOn;
            db.archerentities.insertOne(entity);
        }
        // else
        // {
        //     db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:existingFirm.EventType,EventAction : existingFirm.EventAction, LastPublishedRecordStatus : existingFirm.LastPublishedRecordStatus, EventDate : existingFirm.EventDate, LastFlagProcessedDate:existingFirm.LastFlagProcessedDate}});
        // }
    })

    db.archerentities.updateMany({LastFlagProcessedDate : {$lte:new Date(ISODate().getTime() - calc7Days).toISOString()}},{$set:{EventAction:''}});
    db.archerentitiesmaster.drop(); 
    db.archerentities.aggregate( [   
        { $merge : { into : "archerentitiesmaster" } }
     ] )
}catch(error){
    print("SYSTEM:Archer Error:: Error at archer entities Query ", error);
    throw(error);
}
