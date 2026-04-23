try{
    db = db.getSiblingDB('isqc');

    //Drop existing collection
    db.archerentities.drop();

    var fiscalYearFilter = process.env.FiscalYear;
	
	fiscalYearFilter = parseInt(fiscalYearFilter,10);

    db.firm.aggregate([{
        $match:{
                    fiscalYear: fiscalYearFilter,
                    //isRollForwardedFromPreFY:true
            }
    },{  $graphLookup: {
        from: "firm",
        startWith: "$parentFirmId",
        connectFromField: "parentFirmId",
        connectToField: "abbreviation",
        as: "parentFirms",
        restrictSearchWithMatch: {
            "isPartOfGroup": "IsPartOfGroupType_No",
            "fiscalYear": fiscalYearFilter,
            //"isRollForwardedFromPreFY": true
        }
    }},{
        $lookup: {
            from: "firm",
                        let: {
                firmGroupId: "$firmGroupId"
            },
            pipeline: [{
                $match: {
                    $expr: {
                        $and: [{
                            $eq: ["$fiscalYear", fiscalYearFilter]
                        },{
                            $ne: ["$abbreviation", null]
                        }, {
                            $eq: ["$$firmGroupId", "$abbreviation"]
                        }/*, {
                            $eq: ["$isRollForwardedFromPreFY", true]
                        }*/]
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
                "isPartOfGroup": "IsPartOfGroupType_No",
                "fiscalYear": fiscalYearFilter,
                //"isRollForwardedFromPreFY": true
            }
        }
    },{
        $lookup:{
            from:'title',
            let:{
                ultimateResponsibility:'$ultimateResponsibility',
                firmId:'$firmGroupId'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$ultimateResponsibility','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$ultimateResponsibility','$_id']
                        },{
                            $eq:["$fiscalYear", fiscalYearFilter]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'}
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", fiscalYearFilter]
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
        $unwind:{
            path:'$ultimateResponsibility',
            preserveNullAndEmptyArrays:true
        }
    },{
        $lookup:{
            from:'title',
            let:{
                operationalResponsibilitySqm:'$operationalResponsibilitySqm',
                firmId:'$firmGroupId'
            },
            pipeline:[{
                $match:{
                    // $expr:{
                        // $eq:['$$operationalResponsibilitySqm','$_id']
                    // }
                    $expr:{
                        $and:[{
                            $eq:['$$operationalResponsibilitySqm','$_id']
                        },{
                            $eq:["$fiscalYear", fiscalYearFilter]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'}
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", fiscalYearFilter]
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
        $unwind:{
            path:'$operationalResponsibilitySqm',
            preserveNullAndEmptyArrays:true
        }
    },{
        $lookup:{
            from:'title',
            let:{
                orIndependenceRequirement:'$orIndependenceRequirement',
                firmId:'$firmGroupId'
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
                            $eq:["$fiscalYear", fiscalYearFilter]
                        }]
                    }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'}
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", fiscalYearFilter]
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
                firmId:'$firmGroupId'
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
                                    $eq:["$fiscalYear", fiscalYearFilter]
                                }]
                        }
                }
            },{
                $lookup:{
                    from:'titleassignment',
                    let:{
                        titleId:{$toString:'$_id'}
                    },
                    pipeline:[{
                        $match:{
                            $expr:{
                                $and:[{
                                    $eq:['$$titleId','$titleId']
                                },{
                                    $eq:['$$firmId','$firmId']
                                },{
                                    $eq:["$fiscalYear", fiscalYearFilter]
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
    },{
        $lookup:{
            from:'user',
            let:{
                firmId:'$geographyId'
            },
            pipeline:[{
                $match:{
                    $expr:{
                        $or:[{
                            $in:['$$firmId','$roles.geographyId']
                        },{
                            $eq:['$isSuperAdmin',true]
                        }]
                    }
                }
            }],
            as:'firmUsers'
        }
    },{
        $lookup:{
            from:'country',
            //localField:'country',
            //foreignField:'_id',
            let:{
                countryck:'$country'
            },
            pipeline: [{
                        $match: {
                            $expr: {
                                $and: [
                                    {
                                        $eq: [ "$_id",  "$$countryck" ] ,
                                    },
                                    {
                                        $in: [fiscalYearFilter, "$fiscalYear"]
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
    },{
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
                        {case:{$eq:['$type',"EntityType_Region"]},then:"Region"},
                        {case:{$eq:['$type',"EntityType_SubCluster"]},then:"Sub-Cluster"}
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
            "GroupRegionUniqueId": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Region"]},then:"$$this.abbreviation",else:""}}]}
                }
            },
            "GroupRegion": {
                $reduce:{
                    input:"$groupParentFirms",
                    initialValue:"",
                    in:{$concat:["$$value",{$cond:{if:{$eq:["$$this.type","EntityType_Region"]},then:"$$this.name",else:""}}]}
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
            "UltimateResponsibilitySqmTitle":{$cond:{if:'$ultimateResponsibility.name',then:'$ultimateResponsibility.name',else:''}},
            "UltimateResponsibilitySqmAssignments":{
                $cond:{
                    if:'$ultimateResponsibility',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$ultimateResponsibility.titleAssignments.assignments'},then:'$ultimateResponsibility.titleAssignments.assignments',else:[]}}
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
            "OperationalResponsibilitySqmTitle":{$cond:{if:'$operationalResponsibilitySqm.name',then:'$operationalResponsibilitySqm.name',else:''}},
            "OperationalResponsibilitySqmAssignments":{
                $cond:{
                    if:'$operationalResponsibilitySqm',
                    then:{
                        $let:{
                            vars:{
                                name:{$cond:{if:'$operationalResponsibilitySqm.name',then:'$operationalResponsibilitySqm.name',else:''}},
                                assignments:{$cond:{if:{$isArray:'$operationalResponsibilitySqm.titleAssignments.assignments'},then:'$operationalResponsibilitySqm.titleAssignments.assignments',else:[]}}
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
            "SQMAdmin":{
                $reduce:{
                    input:{
                        $filter:{
                            input:'$firmUsers',
                            as:'u',
                            cond:{
                                $in:['$abbreviation','$$u.adminFirmAccess']
                            }
                        }
                    },
                    initialValue:'',
                    in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.email'] }
                }
            },
            "SQMEditor":{
                $reduce:{
                    input:{
                        $filter:{
                            input:'$firmUsers',
                            as:'u',
                            cond:{
                                $in:['$abbreviation','$$u.editFirmAccess']
                            }
                        }
                    },
                    initialValue:'',
                    in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.email'] }
                }
            },
            "SQMViewer":{
                $cond:{
                    if:{$eq:['$isPartOfGroup','IsPartOfGroupType_No']},
                    then:{
                        $reduce:{
                            input:{
                                $filter:{
                                    input:'$firmUsers',
                                    as:'u',
                                    cond:{
                                        $not:{
                                            $or:[{
                                                $in:['$abbreviation','$$u.adminFirmAccess']
                                            },{
                                                $in:['$abbreviation','$$u.editFirmAccess']
                                            }]
                                        }
                                    }
                                }
                            },
                            initialValue:'',
                            in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.email'] }
                        }
                    },
                    else:''
                }
            },
            "UltimateResponsibilitySqmAssignmentsName":{
                $cond:{
                    if:'$ultimateResponsibility',
                    then:{
                        $let:{
                            vars:{
                                assignments:{$cond:{if:{$isArray:'$ultimateResponsibility.titleAssignments.assignments'},then:'$ultimateResponsibility.titleAssignments.assignments',else:[]}}
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
            "OperationalResponsibilitySqmAssignmentsName":{
                $cond:{
                    if:'$operationalResponsibilitySqm',
                    then:{
                        $let:{
                            vars:{
                                name:{$cond:{if:'$operationalResponsibilitySqm.name',then:'$operationalResponsibilitySqm.name',else:''}},
                                assignments:{$cond:{if:{$isArray:'$operationalResponsibilitySqm.titleAssignments.assignments'},then:'$operationalResponsibilitySqm.titleAssignments.assignments',else:[]}}
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
                "FiscalYear": "FY" + fiscalYearFilter.toString().slice(-2)
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
        var updatedEventFirm = existingFirm && db.event.find({actor: entity.FirmUniqueId,actorType : "firm",fiscalYear: fiscalYearFilter, message: {$in:['ActionType_Update','ActionType_Publish','ActionType_Rollforward','ActionType_Archive']},modifiedOn : {$gt:existingFirm.ArcherPublishedOn}}).sort({'modifiedOn':-1}).limit(1).toArray();
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
                    fiscalYear: fiscalYearFilter,
                    message: { $in: ["ActionType_Title_Users_Add", "ActionType_Title_Users_Update", "ActionType_Title_Users_Remove"] },
                    modifiedOn: { $gt: existingFirm.ArcherPublishedOn }
                };
    
                updatedEventFirm = db.event.find(queryTitleUpdate).sort({ modifiedOn: -1 }).limit(1).toArray();
            }
            else
            {
                if(entity.SQMAdmin !== existingFirm.SQMAdmin || entity.SQMEditor !== existingFirm.SQMEditor || entity.SQMViewer !== existingFirm.SQMViewer)
                {
                    //Admins difference
                    var existingAdmins = existingFirm.SQMAdmin.split(";");
                    var newAdmins = entity.SQMAdmin.split(";");
                    
                    var existingAdminsNotinNew = existingAdmins.filter(x => !newAdmins.includes(x));
                    var newAdminsNotinExisting = newAdmins.filter(x => !existingAdmins.includes(x));
                    
                    //Editors difference
                    var existingEditors = existingFirm.SQMEditor.split(";");
                    var newEditor = entity.SQMEditor.split(";");
                    
                    var existingEditorsNotinNew = existingEditors.filter(x => !newEditor.includes(x));
                    var newEditorsNotinExisting = newEditor.filter(x => !existingEditors.includes(x));
                    
                    //Viewers difference
                    var existingViewers = existingFirm.SQMViewer.split(";");
                    var newViewers = entity.SQMViewer.split(";");
                    
                    var existingViewersNotinNew = existingViewers.filter(x => !newViewers.includes(x));
                    var newViewersNotinExisting = newViewers.filter(x => !existingViewers.includes(x));
                    
                    // Concatenate the variables into one array
                    var all_users = existingAdminsNotinNew.concat(newAdminsNotinExisting, existingEditorsNotinNew, newEditorsNotinExisting, existingViewersNotinNew, newViewersNotinExisting);
                    // Convert all values to lowercase
                    var all_users_lower = all_users.map(function(user) {
                        return user.toLowerCase();
                    });
                
                    var query = {
                        "actor": { $in: all_users_lower },
                        "actorType": "User",
                        "fiscalYear": fiscalYearFilter,
                        "modifiedOn" : {$gt:existingFirm.ArcherPublishedOn},
                        "message": { "$in": ["ActionType_Add", "ActionType_Update", "ActionType_Delete"] }
                    };
                    
                    updatedEventFirm = db.event.find(query).sort({'modifiedOn': -1}).limit(1).toArray();
                }
            }
        }

        if (!(existingFirm)) {
            var getNewEventDateFirm = db.event.find({actor: entity.FirmUniqueId,actorType : "firm",fiscalYear: fiscalYearFilter, message: 'ActionType_Add'}).sort({'modifiedOn':-1}).limit(1).toArray();
            db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:'New',EventAction : 'Update', LastPublishedRecordStatus : 'Active', EventDate : getNewEventDateFirm.length>0?getNewEventDateFirm[0].modifiedOn:'', LastFlagProcessedDate:entity.ArcherPublishedOn}});
            }
        else if (updatedEventFirm.length>0) {
            db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:'Updated',EventAction : 'Update', LastPublishedRecordStatus : 'Active', EventDate : updatedEventFirm[0].modifiedOn, LastFlagProcessedDate:entity.ArcherPublishedOn}})
            }
       else
        {
            db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:existingFirm.EventType,EventAction : existingFirm.EventAction, LastPublishedRecordStatus : existingFirm.LastPublishedRecordStatus, EventDate : existingFirm.EventDate, LastFlagProcessedDate:existingFirm.LastFlagProcessedDate}})
        }
    })

    db.archerentitiesmaster.find().forEach(function(entity){
        var existingFirm = db.archerentities.findOne({FirmUniqueId :entity.FirmUniqueId});
        var isFirmDelete = db.event.find({actor: entity.FirmUniqueId,actorType : "firm",fiscalYear: fiscalYearFilter, message: 'ActionType_Delete',modifiedOn : {$gt:entity.ArcherPublishedOn}}).sort({'modifiedOn':-1}).limit(1).toArray();
        if(!existingFirm && isFirmDelete.length>0){
            entity.EventType = 'Deleted';
            entity.EventAction = 'Update';
            entity.LastPublishedRecordStatus= 'Inactive';
            entity.EventDate = isFirmDelete[0].modifiedOn;
            entity.LastFlagProcessedDate = entity.ArcherPublishedOn;
            db.archerentities.insertOne(entity);
        }
        else
        {
            db.archerentities.updateOne({FirmUniqueId:entity.FirmUniqueId},{$set:{EventType:existingFirm.EventType,EventAction : existingFirm.EventAction, LastPublishedRecordStatus : existingFirm.LastPublishedRecordStatus, EventDate : existingFirm.EventDate, LastFlagProcessedDate:existingFirm.LastFlagProcessedDate}});
        }
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
