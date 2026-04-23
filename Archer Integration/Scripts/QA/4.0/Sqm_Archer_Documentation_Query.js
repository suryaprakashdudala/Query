db = db.getSiblingDB('isqc');
var FiscalYear = 2023;
var fiscalYearFilter = 2023;

db.globaldocumentation.aggregate([{
    $match:{
			$and:[{
				type:'Process',
				fiscalYear: fiscalYearFilter
			}]
		}
},{
    $lookup: {
        from: "firmprocess",
		let:{
			uniqueIdCk:'$uniqueId'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$processId",  "$$uniqueIdCk" ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: "firmProcesses"
    }
}, {
    $out: "processdetails"
}]);

db.bkcassignment.aggregate([{
    $lookup:{
        from:'firm',
        //localField:'firmId',
        //foreignField:'abbreviation',
		let:{
			firmIdCk:'$firmId'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$abbreviation",  "$$firmIdCk" ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as:'firm'
    }
},{
    $unwind:'$firm'
},{
    $out:'bkcAssignmentDetails'
}]);

//Requirement control
db.firm.aggregate([{
    $match: {
        $expr: {
            $and: [
                '$publishedDate',
                { $ne: ['$publishedDate', ''] },
                { $eq: ['$isPublishQueryRun', false] },
				{ $eq: ["$fiscalYear", fiscalYearFilter]}
            ]
        }
    }
},{
    $lookup:{
        from:'functionowner',
        let:{
            firmId:'$abbreviation'
        },
        pipeline:[{
            $match:{
                $expr:{
                    $and:[{
                        $eq:['$$firmId','$firmId']
                    },{
                        $eq:['$notApplicable',true]
                    },
					{ 
						$eq: ["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as:'notApplicableFunctions'
    }
},{
    $lookup: {
        from: 'enumeration',
        //localField: 'languageCode',
        //foreignField: 'languageCode',
		let:{
			languageCodeCk:'$languageCode'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$languageCode",  "$$languageCodeCk" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'localLanguage'
    }
},
 {
    $graphLookup: {
        from: "firm",
        startWith: "$parentFirmId",
        connectFromField: "parentFirmId",
        connectToField: "abbreviation",
        as: "parentFirms",
        restrictSearchWithMatch: {
            "isPartOfGroup": "IsPartOfGroupType_No",
			"fiscalYear": fiscalYearFilter
        }
    }
},{
    $lookup:{
        from:'action',
        let:{
            firmIds:{
                $concatArrays: [
                    ["$abbreviation"], "$parentFirms.abbreviation"
                ]
            },
            abbreviation:'$abbreviation'
        },
        pipeline:[{
            $match:{
                $expr:{
                    $cond:{
                        //if:{$eq:['$objectType','RequirementControl']},
						if:{$and:[{$eq:['$objectType','RequirementControl']},{$eq:["$fiscalYear", fiscalYearFilter]}]},
                        then:{
                            $eq:['$firmId','$$abbreviation']
                        },
                        else:{
                            $in:['$firmId','$$firmIds']
                        }
                    }
                }
            }
        },{
            $project:{
                objectId:1
            }
        }],
        as:'actions'
    }
},{
    $lookup:{
        from:'bkcAssignmentDetails',
        let:{
            abbreviation:'$abbreviation',
        },
        pipeline:[{
            $match:{
                $expr:{
                    $in:['$$abbreviation','$assignments.executingEntityId']
                }
            }
        },{
            $unwind:'$assignments'
        }],
        as:'bkcassignments'
    }
},{
    $addFields:{
        assignedBkcIds:{
            $filter:{
                input:'$bkcassignments',
                as:'b',
                cond:{$eq:['$$b.assignments.executingEntityId','$abbreviation']}
            }
        }
    }
},{
    $lookup: {
        from: 'documentation',
        let: {
            actions:'$actions',
            assignedBkcIds:'$assignedBkcIds',
            abbreviation:'$abbreviation'
        },
        pipeline: [{
            $match:{
                // $expr:{
                    // $eq:['$type','RequirementControl']
                // }
				$expr: {
                            $and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$eq:['$type','RequirementControl']
								}
							]
                        }
            }
        },{
            $match: {
                $expr: {
                    $and: [{
                        $in: ['$uniqueId', '$$assignedBkcIds.assignments.bkcId']
                    }, {
                        $ne: ['$status', 'StatusType_Draft']
                    },{
                        $not:{
                            $in:[{$toString:'$_id'},'$$actions.objectId']
                        }
                    }
                ]
                }
            }
        }],
        as: 'requirementcontrol'
    }
}, {
    $unwind: '$requirementcontrol'
},  {
    $lookup: {
        from: 'documentation',
        let: {
            reqId: { $toString: '$requirementcontrol._id' },
            firmId: '$abbreviation'
        },
        pipeline: [{
            $match:{
                $expr:{
                    $eq:['$type','RequirementControlAssignment']
                }
            }
        },{
            $match: {
                $expr: {
                    $and: [{
						$eq: ["$fiscalYear", fiscalYearFilter]
					},{
                        $eq: ['$$firmId', '$firmId']
                    }, {
                        $eq: ['$$reqId', '$requirementControlId']
                    }]
                }
            }
        }],
        as: 'assignment'
    }
},{
    $unwind: '$assignment'
}, {
    $match:{
        'assignment.status':{$ne:'StatusType_Draft'}
    }
},{
    $lookup: {
        from: 'processdetails',
        let: {
            uniqueId: '$requirementcontrol.uniqueId',
            firmId: {
                $cond:{
                    if:'$assignment',
                    then:'$abbreviation',
                    else:'$requirementcontrol.firmId'
                }
            },
            assignedFirmId:'$abbreviation'
        },
        pipeline: [{
            $match: {
                $expr: {
                    $anyElementTrue: {
                        $map: {
                            input: "$firmProcesses",
                            as: 'fp',
                            in: { $and: [{ $in: ['$$uniqueId', '$$fp.mappedKeyControls'] }, { $eq: ['$$fp.firmId', '$$firmId'] }] }
                        }
                    }
                }
            }
        },
        {
            $lookup: {
                from: 'title',
                let: {
                    firmProcess: '$firmProcesses'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: [
                                '$_id',
                                {
                                    $cond: {
                                        if: {
                                            $and: ['$$firmProcess', {
                                                $isArray: '$$firmProcess'
                                            }]
                                        },
                                        then: {
                                            $reduce: {
                                                input: '$$firmProcess',
                                                initialValue: [],
                                                in: { $concatArrays: ['$$value',{$cond:{if:{$and:[{$isArray:'$$this.processOwners'},{ $eq: ['$$assignedFirmId', '$$this.firmId'] }]},then:'$$this.processOwners',else:[]}}] }
                                            }
                                        },
                                        else: []
                                    }
                                }
                            ]
                        }
                    }
                }, {
                    $lookup: {
                        from: 'titleassignment',
                        let: {
                            titleId: {
                                $toString: '$_id'
                            }
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [{
                                        $eq: ['$$titleId', '$titleId']
                                    }, {
                                        $eq: ['$$assignedFirmId', '$firmId']
                                    },
									{ 
										$eq: ["$fiscalYear", fiscalYearFilter]
									}]
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
                as: 'poTitles'
            }
        }],
        as: 'requirementcontrol.processes'
    }
}, {
    $unwind: {
        path: "$requirementcontrol.processes",
        preserveNullAndEmptyArrays: true
    }
}, {
    $lookup:{
        from:'firm',
        //localField:'requirementcontrol.firmId',
        //foreignField:'abbreviation',
		let:{
			reqCtrlFirmId:'$requirementcontrol.firmId'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$abbreviation",  "$$reqCtrlFirmId" ] ,
								},
								{
									 $eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as:'designingEntity'
    }
},{
    $unwind:'$designingEntity'
}, {
    $addFields: {
        'requirementcontrol.controlName': { $cond: { if: '$assignment.controlName', then: '$assignment.controlName', else: '$requirementcontrol.controlName' } },
		'requirementcontrol.controlNameAlt': { $cond: { if: '$assignment.controlNameAlt', then: '$assignment.controlNameAlt', else: '$requirementcontrol.controlNameAlt' } },
        'requirementcontrol.nameAlt': { $cond: { if: '$assignment.nameAlt', then: '$assignment.nameAlt', else: '$requirementcontrol.nameAlt' } },
        'requirementcontrol.relatedPolicy': { $cond: { if: '$assignment.relatedPolicy', then: '$assignment.relatedPolicy', else: '$requirementcontrol.relatedPolicy' } },
		'requirementcontrol.relatedPolicyAlt': { $cond: { if: '$assignment.relatedPolicyAlt', then: '$assignment.relatedPolicyAlt', else: '$requirementcontrol.relatedPolicyAlt' } },
        'requirementcontrol.nameOfControlServiceProvider': { $cond: { if: '$assignment.nameOfControlServiceProvider', then: '$assignment.nameOfControlServiceProvider', else: '$requirementcontrol.nameOfControlServiceProvider' } },
        'requirementcontrol.nameOfControlServiceProviderAlt': { $cond: { if: '$assignment.nameOfControlServiceProviderAlt', then: '$assignment.nameOfControlServiceProviderAlt', else: '$requirementcontrol.nameOfControlServiceProviderAlt' } },
        'requirementcontrol.addInformationExecutionControls': { $cond: { if: '$assignment.addInformationExecutionControls', then: '$assignment.addInformationExecutionControls', else: '$requirementcontrol.addInformationExecutionControls' } },
        'requirementcontrol.addControlEvidenceControls': { $cond: { if: '$assignment.addControlEvidenceControls', then: '$assignment.addControlEvidenceControls', else: '$requirementcontrol.addControlEvidenceControls' } },
        'requirementcontrol.attachments': { $cond: { if: '$assignment.attachments', then: '$assignment.attachments', else: '$requirementcontrol.attachments' } },
        'requirementcontrol.keyControlObjective': { $cond: { if: '$assignment.keyControlObjective', then: '$assignment.keyControlObjective', else: '$requirementcontrol.keyControlObjective' } },
        'requirementcontrol.keyControlObjectiveAlt': { $cond: { if: '$assignment.keyControlObjectiveAlt', then: '$assignment.keyControlObjectiveAlt', else: '$requirementcontrol.keyControlObjectiveAlt' } },
        'requirementcontrol.controlCriteria': { $cond: { if: '$assignment.controlCriteria', then: '$assignment.controlCriteria', else: '$requirementcontrol.controlCriteria' } },
        'requirementcontrol.controlCriteriaAlt': { $cond: { if: '$assignment.controlCriteriaAlt', then: '$assignment.controlCriteriaAlt', else: '$requirementcontrol.controlCriteriaAlt' } },
        'requirementcontrol.controlNature': { $cond: { if: '$assignment.controlNature', then: '$assignment.controlNature', else: '$requirementcontrol.controlNature' } },
        'requirementcontrol.controlOperator': { $cond: { if: '$assignment.controlOperator', then: '$assignment.controlOperator', else: '$requirementcontrol.controlOperator' } },
        'requirementcontrol.controlType': { $cond: { if: '$assignment.controlType', then: '$assignment.controlType', else: '$requirementcontrol.controlType' } },
        'requirementcontrol.description': { $cond: { if: '$assignment.description', then: '$assignment.description', else: '$requirementcontrol.description' } },
        'requirementcontrol.descriptionAlt': { $cond: { if: '$assignment.descriptionAlt', then: '$assignment.descriptionAlt', else: '$requirementcontrol.descriptionAlt' } },
        'requirementcontrol.frequencyType': { $cond: { if: '$assignment.frequencyType', then: '$assignment.frequencyType', else: '$requirementcontrol.frequencyType' } },
        'requirementcontrol.status': { $cond: { if: '$assignment.status', then: '$assignment.status', else: '$requirementcontrol.status' } },
        'requirementcontrol.relatedProcesses': { $cond: { if: '$assignment.relatedProcesses', then: '$assignment.relatedProcesses', else: '$requirementcontrol.relatedProcesses' } },
        'requirementcontrol.responseOwners': { $cond: { if: '$assignment.responseOwners', then: '$assignment.responseOwners', else: '$requirementcontrol.responseOwners' } },
        'requirementcontrol.typeOfEngagements': { $cond: { if: '$assignment.typeOfEngagements', then: '$assignment.typeOfEngagements', else: '$requirementcontrol.typeOfEngagements' } },
        'requirementcontrol.serviceLine': { $cond: { if: '$assignment.serviceLine', then: '$assignment.serviceLine', else: '$requirementcontrol.serviceLine' } },
        'requirementcontrol.tags': { $cond: { if: '$assignment.tags', then: '$assignment.tags', else: '$requirementcontrol.tags' } },
        'requirementcontrol.supportingITApplication': { $cond: { if: '$assignment.supportingITApplication', then: '$assignment.supportingITApplication', else: '$requirementcontrol.supportingITApplication' } },
        'requirementcontrol.mitigatedResources': { $cond: { if: '$assignment.mitigatedResources', then: '$assignment.mitigatedResources', else: '$requirementcontrol.mitigatedResources' } },
        'requirementcontrol.controlFunction':{ $cond: { if: '$assignment.controlFunction', then: '$assignment.controlFunction', else: '$requirementcontrol.controlFunction' } },
        'requirementcontrol.localControlFunction':{ $cond: { if: '$assignment.localControlFunction', then: '$assignment.localControlFunction', else: '$requirementcontrol.localControlFunction' } },
        'requirementcontrol.executionControlFunction':{ $cond: { if: '$assignment.executionControlFunction', then: '$assignment.executionControlFunction', else: '$requirementcontrol.executionControlFunction' } },
        'requirementcontrol.relatedQualityRisks':{ $cond: { if: '$assignment.relatedQualityRisks', then: '$assignment.relatedQualityRisks', else: '$requirementcontrol.relatedQualityRisks' } },
        'requirementcontrol.relatedSubRisks':{ $cond: { if: '$assignment.relatedSubRisks', then: '$assignment.relatedSubRisks', else: '$requirementcontrol.relatedSubRisks' } },
        'requirementcontrol.localLanguage':{ $cond: { if: '$assignment.LocalLanguage', then: '$assignment.LocalLanguage', else: '$requirementcontrol.LocalLanguage' } }
    }
},
{
    $lookup: {
        from: 'globaldocumentation',
        let: {
            firmId: '$abbreviation',
            controlFunction: "$requirementcontrol.controlFunction"
        },
        pipeline: [{
            $match:{
                $expr:{
                    //$eq:['$type','Function']
					$and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$eq:['$type','Function']
								}
						  ]
                }
            }
        },{
            $match: {
                $expr: {
                    $eq: [{ $toString: "$_id" }, "$$controlFunction"]
                }
            }
        }, {
            $lookup: {
                from: 'functionowner',
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [{
									$eq: ['$$firmId', '$firmId']
								},
								{
									$eq: ['$$controlFunction', '$functionId']
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
                            ]
                        }
                    }
                }],
                as: 'functionowner'
            }
        }, {
            $unwind: {
                path: '$functionowner',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    foTitle: '$functionowner.title'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            //$eq: ['$$foTitle', '$_id']
							$and: [{
									$eq: ['$$foTitle', '$_id']
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
                            ]
                        }
                    }
                }, {
                    $lookup: {
                        from: 'titleassignment',
                        let: {
                            titleId: {
                                $toString: '$_id'
                            }
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [{
                                        $eq: ['$$titleId', '$titleId']
                                    }, {
                                        $eq: ['$$firmId', '$firmId']
                                    },{
										$eq: ["$fiscalYear", fiscalYearFilter]
									}
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
                as: 'foTitle'
            }
        }, {
            $unwind: {
                path: '$foTitle',
                preserveNullAndEmptyArrays: true
            }
        }],
        as: 'firmFunctions'
    }
}, {
    $unwind: {
        path: '$firmFunctions',
        preserveNullAndEmptyArrays: true
    }
},{
    $lookup: {
        from: 'documentation',
        //localField: 'requirementcontrol.relatedSubRisks',
        //foreignField: 'uniqueId',
		let:{
			reqCtrlRelatedSubRisk:'$requirementcontrol.relatedSubRisks'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  "$$reqCtrlRelatedSubRisk" ] ,
								},
								{
									 $eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.relatedSubRisks'
    }
}, {
    $addFields: {
        'requirementcontrol.relatedQualityRisks': {
            $setUnion: ['$requirementcontrol.relatedQualityRisks', {
                $reduce: {
                    input: '$requirementcontrol.relatedSubRisks.relatedQualityRisks',
                    initialValue: [],
                    in: { $concatArrays: ['$$value', '$$this'] }
                }
            }]
        }
    }
},{
    $match:{
        $expr:{
            $and:[{
                $not:{
                    $allElementsTrue:{
                        $map:{
                            input:'$requirementcontrol.relatedQualityRisks',
                            as:'qr',
                            in:{$in:['$$qr','$actions.objectId']}
                        }
                    }
                }
            },{
                $cond:{
                    if:{
                        $ne:['$requirementcontrol.isControlOverResource',true]
                    },
                    then:{
                        $not:{
                            $and: [
                                { $and: [{ $isArray: '$requirementcontrol.supportingITApplication' }, { $gt: [{ $size: '$requirementcontrol.supportingITApplication' }, 0] }] },
                                { $eq: [{ $size: { $setDifference: ['$requirementcontrol.supportingITApplication', '$actions.objectId'] } }, 0] }
                            ]
                        }
                    },
                    else:{
                        $not:{
                            $and: [
                                { $and: [{ $isArray: '$requirementcontrol.mitigatedResources' }, { $gt: [{ $size: '$requirementcontrol.mitigatedResources' }, 0] }] },
                                { $eq: [{ $size: { $setDifference: ['$requirementcontrol.mitigatedResources', '$actions.objectId'] } }, 0] }
                            ]
                        }
                    }
                }
            }]
        }
    }
},{
    $lookup: {
        from: 'documentation',
        //localField: 'requirementcontrol.relatedQualityRisks',
        //foreignField: 'uniqueId',
		let:{
			reqCtrlRelatedQtrRisk:'$requirementcontrol.relatedQualityRisks'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  "$$reqCtrlRelatedQtrRisk" ] ,
								},
								{
									 $eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.relatedQualityRisks'
    }
},{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'requirementcontrol.controlType',
        //foreignField: '_id',
		let:{
			reqCtrlType:'$requirementcontrol.controlType'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$_id",  "$$reqCtrlType" ] ,
								},
								{
									 $in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.controlType'
    }
}, {
    $unwind: {
        path: '$requirementcontrol.controlType',
        preserveNullAndEmptyArrays: true
    }
}, {
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'requirementcontrol.controlNature',
        //foreignField: '_id',
		let:{
			reqCtrlTypeNature:'$requirementcontrol.controlNature'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$_id",  "$$reqCtrlTypeNature" ] ,
								},
								{
									 $in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.controlNature'
    }
}, {
    $unwind: {
        path: '$requirementcontrol.controlNature',
        preserveNullAndEmptyArrays: true
    }
}, {
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'requirementcontrol.frequencyType',
        //foreignField: '_id',
		let:{
			reqCtrlfreqType:'$requirementcontrol.frequencyType'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$reqCtrlfreqType" ] ,
								},
								{
									 $in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.frequencyType'
    }
},{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'requirementcontrol.serviceLine',
        //foreignField: '_id',
		let:{
			reqCtrlServiceLine:'$requirementcontrol.serviceLine'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$reqCtrlServiceLine" ] ,
								},
								{
									 $in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.serviceLine'
    }
},{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'requirementcontrol.addInformationExecutionControls.ipeCategory',
        //foreignField: '_id',
		let:{
			reqCtrlAddIpeCategory:'$requirementcontrol.addInformationExecutionControls.ipeCategory'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$reqCtrlAddIpeCategory" ] ,
								},
								{
									 $in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.ipeCategory'
    }
},  {
    $lookup: {
        from: 'documentation',
        //localField: 'requirementcontrol.relatedQualityRisks.relatedObjectives',
        //foreignField: 'uniqueId',
		let:{
			reqCtrlRelSubRiskRelObj:'$requirementcontrol.relatedObjectives'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  { $ifNull: ['$$reqCtrlRelSubRiskRelObj',[]] } ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.relatedObjectives'
    }
}, {
    $lookup: {
        from: 'documentation',
        let: {
            systemIds: {
                $cond: {
                    if: { $isArray: '$requirementcontrol.addInformationExecutionControls' },
                    then: {
                        $reduce: {
                            input: '$requirementcontrol.addInformationExecutionControls.ipeSystems',
                            initialValue: [],
                            in: { $concatArrays: ['$$value', '$$this'] }
                        }
                    },
                    else: []
                }
            }
        },
        pipeline: [{
            $match:{
				//type:'Resource'
				$and:[{
						type:'Resource',
						fiscalYear: fiscalYearFilter
					}]
				}
        },{
            $match: {
                $expr: {
                    $in: [{ $toString: '$_id' }, '$$systemIds']
                }
            }
        }],
        as: 'requirementcontrol.ipeSystems'
    }
}, {
    $lookup: {
        from: 'documentation',
        let: {
            mitigatedResourceIds: {
                $cond: {
                    if: { $isArray: '$requirementcontrol.mitigatedResources' },
                    then:'$requirementcontrol.mitigatedResources',
                    else: []
                }
            }
        },
        pipeline: [{
            $match:{
					$and:[{
						type:'Resource',
						fiscalYear: fiscalYearFilter
					}]
				}
			}
        ,{
            $match: {
                $expr: {
                    $in: [{ $toString: '$_id' }, '$$mitigatedResourceIds']
                }
            }
        }],
        as: 'requirementcontrol.mitigatedResourcesObjs'
    }
}, {
    $lookup: {
        from: 'documentation',
        let: {
            supportingITApplicationIds: {
                $cond: {
                    if: { $isArray: '$requirementcontrol.supportingITApplication' },
                    then:'$requirementcontrol.supportingITApplication' ,
                    else: []
                }
            }
        },
        pipeline: [{
            $match:{
				//type:'Resource'
					$and:[{
						type:'Resource',
						fiscalYear: fiscalYearFilter
					}]
				}
        },{
            $match: {
                $expr: {
                    $in: [{ $toString: '$_id' }, '$$supportingITApplicationIds']
                }
            }
        }],
        as: 'requirementcontrol.supportingITApplicationIdsObjs'
    }
}, {
    $lookup: {
        from: 'title',
        let: {
            titleIds: '$requirementcontrol.responseOwners'
        },
        pipeline: [{
            $match: {
                // $expr: {
                    // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                // }
				$expr: {
                            $and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
								}
							]
                        }
            }
        }],
        as: 'requirementcontrol.responseOwnerTitles'
    }
}, {
    $lookup: {
        from: 'titleassignment',
        let: {
            titleIds: {
                $map: {
                    input: '$requirementcontrol.responseOwnerTitles._id',
                    as: 'titleId',
                    in: { $toString: '$$titleId' }
                }
            },
            firmId: '$abbreviation'
        },
        pipeline: [{
            $match: {
                $expr: {
                    $and: [{
                        $in: ['$titleId', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                    }, {
                        $eq: ['$firmId', '$$firmId']
                    },{
						$eq: ["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as: 'requirementcontrol.responseOwnerTitleAssignments'
    }
}, {
    $lookup: {
        from: 'title',
        let: {
            titleIds: '$requirementcontrol.controlOperator'
        },
        pipeline: [{
            $match: {
                // $expr: {
                    // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                // }
				$expr: {
                            $and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
								}
							]
                        }
            }
        }],
        as: 'requirementcontrol.controlOperatorTitles'
    }
},
{
    $lookup: {
        from: 'keycontrolresource',
		let:{
			reqCtrlTypeEngagements:'$requirementcontrol.typeOfEngagements'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$reqCtrlTypeEngagements" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'requirementcontrol.typeOfEngagements'
    }
}, 
{
    $lookup: {
        from: 'globaldocumentation',
        let: {
		       tagIds: '$requirementcontrol.tags'
            },
        pipeline: [{
            $match: {
                // $expr: {
                    // $in: [{$toString: '$_id'}, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
                // }
				$expr: {
                            $and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$in: [{$toString: '$_id'}, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
								}
							]
                        }
            }
        }],
        as: 'requirementcontrol.tags'
    }
},
{
    $lookup: {
        from: 'titleassignment',
        let: {
            titleIds: {
                $map: {
                    input: '$requirementcontrol.controlOperatorTitles._id',
                    as: 'titleId',
                    in: { $toString: '$$titleId' }
                }
            },
            firmId: '$abbreviation'
        },
        pipeline: [{
            $match: {
                $expr: {
                    $and: [{
                        $in: ['$titleId', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                    }, {
                        $eq: ['$firmId', '$$firmId']
                    },
					{
						$eq:["$fiscalYear", fiscalYearFilter]
					}
					]
                }
            }
        }],
        as: 'requirementcontrol.controlOperatorTitleAssignments'
    }
}, {
    $project: {
        _id: 0,
        UniqueControlID: { $concat: ['$abbreviation', '-', '$requirementcontrol.uniqueId'] },
        EntityId: '$abbreviation',
        NetworkId: {
            $cond: { if: { $eq: ['$type', "EntityType_Network"] }, then: '$abbreviation', else: '' }
        },
        AreaId: {
            $cond: { if: { $eq: ['$type', "EntityType_Area"] }, then: '$abbreviation', else: '' }
        },
        RegionId: {
            $cond: { if: { $eq: ['$type', "EntityType_Region"] }, then: '$abbreviation', else: '' }
        },
        ClusterId: {
            $cond: { if: { $eq: ['$type', "EntityType_Cluster"] }, then: '$abbreviation', else: '' }
        },
        SubClusterId: {
            $cond: { if: { $eq: ['$type', "EntityType_SubCluster"] }, then: '$abbreviation', else: '' }
        },
        MemberFirmId: {
            $cond: { if: { $or: [{ $eq: ['$type', "EntityType_MemberFirm"] }, { $eq: ['$type', "EntityType_Group"] }] }, then: '$abbreviation', else: '' }
        },
        EntityType: {
            $switch: {
                branches: [
                    { case: { $eq: ['$type', "EntityType_Area"] }, then: "Area" },
                    { case: { $eq: ['$type', "EntityType_Cluster"] }, then: "Cluster" },
                    { case: { $eq: ['$type', "EntityType_Group"] }, then: "Location" },
                    { case: { $eq: ['$type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                    { case: { $eq: ['$type', "EntityType_Network"] }, then: "Network" },
                    { case: { $eq: ['$type', "EntityType_Region"] }, then: "Region" },
                    { case: { $eq: ['$type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                ],
                default: ''
            }
        },
        ControlName: '$requirementcontrol.controlName',
		ControlNameLocal: '$requirementcontrol.controlNameAlt',
        ControlDescription: '$requirementcontrol.description',
		ControlDescriptionLocal: '$requirementcontrol.descriptionAlt',
		RelatedPolicyProcedureManualGuidance: '$requirementcontrol.relatedPolicy',
		RelatedPolicyProcedureManualGuidanceLocal: '$requirementcontrol.relatedPolicyAlt',
		ServiceProviderName: '$requirementcontrol.nameOfControlServiceProvider',
		ServiceProviderNameLocal: '$requirementcontrol.nameOfControlServiceProviderAlt',
        ControlType: { $cond: { if: '$requirementcontrol.controlType', then: '$requirementcontrol.controlType.name', else: '' } },
        ControlNature: { $cond: { if: '$requirementcontrol.controlNature', then: '$requirementcontrol.controlNature.name', else: '' } },
        Frequency: {
            $reduce: {
                input: '$requirementcontrol.frequencyType',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        ControlOwnerTitles: {
            $reduce: {
                input: '$requirementcontrol.responseOwnerTitles',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        ControlOwnerTitleAssignments: {
            $reduce: {
                input: {
                    $reduce: {
                        input: '$requirementcontrol.responseOwnerTitleAssignments',
                        initialValue: [],
                        in: { $concatArrays: ['$$value', '$$this.assignments'] }
                    }
                },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.email'] }
            }
        },
        ControlOperatorTitles: {
            $reduce: {
                input: '$requirementcontrol.controlOperatorTitles',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        ControlOperatorTitleAssignments: {
            $reduce: {
                input: {
                    $reduce: {
                        input: '$requirementcontrol.controlOperatorTitleAssignments',
                        initialValue: [],
                        in: { $concatArrays: ['$$value', '$$this.assignments'] }
                    }
                },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.email'] }
            }
        },
        'FunctionId': {
            $reduce: {
                input: {
                    $cond:{
                        if:{
                            $anyElementTrue:{
                                $map:{
                                    input:'$requirementcontrol.executionControlFunction',
                                    as:'lf',
                                    in:{$in:['$$lf','$notApplicableFunctions.functionId']}
                                }
                            }
                        },
                        then:[],
                        else: '$requirementcontrol.executionControlFunction'
                    }
                },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        // {
        //     $toString: { $toString: '$firmFunctions._id' }
        // },
        'FunctionName': '$firmFunctions.name',
        'FunctionOwnerTitle': {
            $cond: {
                if: '$firmFunctions.foTitle.name',
                then: '$firmFunctions.foTitle.name',
                else: ''
            }
        },
        'FunctionOwnerAssignments': {
            $cond: {
                if: '$firmFunctions.foTitle',
                then: {
                    $let: {
                        vars: {
                            assignments: {
                                $cond: {
                                    if: {
                                        $isArray: '$firmFunctions.foTitle.titleAssignments.assignments'
                                    },
                                    then: '$firmFunctions.foTitle.titleAssignments.assignments',
                                    else: []
                                }
                            }
                        },
                        in: {
                            $cond: {
                                if: {
                                    $and: [{ $isArray: '$$assignments' }, {
                                        $gt: [{
                                            $size: '$$assignments'
                                        }, 0]
                                    }]
                                },
                                then: {
                                    $reduce: {
                                        input: '$$assignments',
                                        initialValue: '',
                                        in: {
                                            $concat: ['$$value', {
                                                $cond: {
                                                    if: {
                                                        $ne: ['', '$$value']
                                                    },
                                                    then: '; ',
                                                    else: ''
                                                }
                                            }, '$$this.email']
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
       'ProcessUniqueId': '$requirementcontrol.processes.uniqueId',
        'ProcessName': '$requirementcontrol.processes.name',
        'ProcessDescription': '$requirementcontrol.processes.description',
        'ProcessOwnerTitles': {
            $reduce: {
                input: '$requirementcontrol.processes.poTitles',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        'ProcessOwnerAssignments': {
            $cond: {
                if: { $and: [{ $isArray: '$requirementcontrol.processes.poTitles' }, { $gt: [{ $size: '$requirementcontrol.processes.poTitles' }, 0] }] },
                then: {
                    $let: {
                        vars: {
                            processOwnerAssignments: {
                                $reduce: {
                                    input: '$requirementcontrol.processes.poTitles',
                                    initialValue: [],
                                    in: {
                                        $cond: {
                                            if: {$and:[{ $isArray: '$$this.titleAssignments.assignments' },{$gt:[{$size:'$$this.titleAssignments.assignments'},0]}]},
                                            then: { $concatArrays: ['$$value', '$$this.titleAssignments.assignments'] },
                                            else: { $concatArrays: ['$$value', [{email:'no assignment'}]] }
                                        }
                                    }
                                }
                            }
                        },
                        in: {
                            $cond: {
                                if: {
                                    $and: [{ $isArray: '$$processOwnerAssignments' }, {
                                        $gt: [{
                                            $size: '$$processOwnerAssignments'
                                        }, 0]
                                    }]
                                },
                                then: {
                                    $reduce: {
                                        input: '$$processOwnerAssignments',
                                        initialValue: '',
                                        in: {
                                            $concat: ['$$value', {
                                                $cond: {
                                                    if: {
                                                        $ne: ['', '$$value']
                                                    },
                                                    then: '; ',
                                                    else: ''
                                                }
                                            }, '$$this.email']
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
        ControlObjective: '$requirementcontrol.keyControlObjective',
        ControlObjectiveLocal: '$requirementcontrol.keyControlObjectiveAlt',
        ControlCriteria: '$requirementcontrol.controlCriteria',
		ControlCriteriaLocal: '$requirementcontrol.controlCriteriaAlt',
        LocalLanguage: {
            $reduce: {
                input: '$localLanguage.name',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
            }
        },
        SupportingEvidence: {
            $reduce: {
                input: '$requirementcontrol.addControlEvidenceControls',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
            }
        },
        SupportingEvidenceLocal: {
            $reduce: {
                input: '$requirementcontrol.addControlEvidenceControls.nameAlt',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
            }
        },
        IEC: {
            $reduce: {
                input: '$requirementcontrol.addInformationExecutionControls',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        IECLocal: {
            $reduce: {
                input: '$requirementcontrol.addInformationExecutionControls.nameAlt',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }

            }
        },
        IECReportType: {
            $reduce: {
                input: '$requirementcontrol.ipeCategory',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        IsControlOverResource:'$requirementcontrol.isControlOverResource',
		KeyControlOperatedByServiceProvider:'$requirementcontrol.isControlOperatedByServiceProvider',
        ResourceType:  {
            $switch: {
                branches: [
                    { case: { $eq: ['$requirementcontrol.resourceType', "ResourceType_Communication"] }, then: "Communication" },
                    { case: { $eq: ['$requirementcontrol.resourceType', "ResourceType_Intellectual"] }, then: "Intellectual" },
                    { case: { $eq: ['$requirementcontrol.resourceType', "ResourceType_Learning"] }, then: "Learning" },
                    { case: { $eq: ['$requirementcontrol.resourceType', "ResourceType_Technology"] }, then: "Technology" }
                ],
                default: ''
            },
        },
        ResourceName:{
            $reduce: {
                input: '$requirementcontrol.mitigatedResourcesObjs',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        SupportingITApplication:{
            $reduce: {
                input: '$requirementcontrol.supportingITApplicationIdsObjs',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        ServiceLine: {
            $cond:{
                if:{$ne:['$requirementcontrol.isControlOverResource',true]},
                then:{
                    $reduce: {
                        input: '$requirementcontrol.serviceLine',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                    }
                },
                else:''
            }
        }, 
        EngagementType:{
            $reduce: {
                input: '$requirementcontrol.typeOfEngagements',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        QualityObjectiveUniquesIds: {
            $reduce: {
                input: '$requirementcontrol.relatedObjectives.qualityObjectiveId',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        QualityRiskUniqueIds: {
            $reduce: {
                input: '$requirementcontrol.relatedQualityRisks.uniqueId',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        QualitySubRiskUniqueIds: {
            $reduce: {
                input: '$requirementcontrol.relatedSubRisks.uniqueId',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        Draft: { $eq: ['$requirementcontrol.status', 'StatusType_Draft'] },
        IECSystems: {
            $reduce: {
                input: '$requirementcontrol.ipeSystems',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        qualityRiskUniqueIdArray: '$requirementcontrol.relatedQualityRisks.uniqueId',
        qualityObjectiveUniqueIdArray: {
            $setUnion: [{
                $reduce: {
                    input: '$requirementcontrol.relatedQualityRisks.relatedObjectives',
                    initialValue: [],
                    in: { $concatArrays: ['$$value', '$$this'] }
                }
            }]
        },
		Tags: {
            $reduce: {
                input: '$requirementcontrol.tags',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        FunctionBaselineControlDesign: {
            $reduce: {
                input: { $cond: { if: { $isArray: '$requirementcontrol.controlFunction' }, then: '$requirementcontrol.controlFunction', else: [] } },
              //  input: '$requirementcontrol.controlFunction',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        FunctionBaselineLocalControlDesign:{
            $reduce: {
                input: {
                    $cond:{
                        if:{
                            $anyElementTrue:{
                                $map:{
                                    input:'$requirementcontrol.localControlFunction',
                                    as:'lf',
                                    in:{$in:['$$lf','$notApplicableFunctions.functionId']}
                                }
                            }
                        },
                        then:[],
                        else:'$requirementcontrol.localControlFunction'
                    }
                },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        FunctionBaselineControlExecution:{
            $reduce: {
                input: {
                    $cond:{
                        if:{
                            $anyElementTrue:{
                                $map:{
                                    input:'$requirementcontrol.executionControlFunction',
                                    as:'lf',
                                    in:{$in:['$$lf','$notApplicableFunctions.functionId']}
                                }
                            }
                        },
                        then:[],
                        else: '$requirementcontrol.executionControlFunction'
                    }
                },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        ResourceIds: {
            $reduce: {
                input: { $cond: { if: { $isArray: '$requirementcontrol.mitigatedResources' }, then: '$requirementcontrol.mitigatedResources', else: [] } },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        ExecutingEntity:'$name',
        ControlDesignedByType:{
            $switch: {
                branches: [
                    { case: { $eq: ['$designingEntity.type', "EntityType_Area"] }, then: "Area" },
                    { case: { $eq: ['$designingEntity.type', "EntityType_Cluster"] }, then: "Cluster" },
                    { case: { $eq: ['$designingEntity.type', "EntityType_Group"] }, then: "Location" },
                    { case: { $eq: ['$designingEntity.type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                    { case: { $eq: ['$designingEntity.type', "EntityType_Network"] }, then: "Network" },
                    { case: { $eq: ['$designingEntity.type', "EntityType_Region"] }, then: "Region" },
                    { case: { $eq: ['$designingEntity.type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                ],
                default: ''
            }
        },
        ControlDesignedByName:'$designingEntity.name',
        PerformedOnBehalfOf:{
            $reduce:{
                input:'$assignedBkcIds',
                initialValue:'',
                in:{
                    $cond:{
                        if:{
                            $and:[{$eq:['$$this.assignments.executingEntityId','$abbreviation']},{$eq:['$$this.assignments.bkcId','$requirementcontrol.uniqueId']}]
                        },
                        then:{
                            $cond:{
                                if:{$eq:['$$value','']},
                                then:'$$this.firm.name',
                                else:{$concat:['$$value','; ','$$this.firm.name']}
                            }
                        },
                        else:'$$value'
                    }
                }
            }
        },
		IsControlOverResourceNew:   {
			$switch: {
                branches: [
                    { case: { $eq: ['$requirementcontrol.isControlOverResource', true] }, then: "Yes" },
                    { case: { $eq: ['$requirementcontrol.isControlOverResource', false] }, then: "No" },
                ],
                default: ''
            }
        },
		KeyControlOperatedByServiceProviderNew:{
			$switch: {
				branches: [
					{ case: { $eq: ['$requirementcontrol.isControlOperatedByServiceProvider', true] }, then: "Yes" },
					{ case: { $eq: ['$requirementcontrol.isControlOperatedByServiceProvider', false] }, then: "No" },
				],
				default: ''
			},
		}
    }
}, {
    $out: 'sqmarcherrequirementcontroltemp'
}])

//To be  used while migrating to mongo 5
// {
//     $merge: {
//         into:'sqmarcherkeycontrol'
//         on:'UniqueControlID',
//         whenMatched:'replace',
//         whenNotMatched: "insert"
//     }
// }

//Key Control
db.firm.aggregate([{
    $match: {
        $expr: {
            $and: [
                '$publishedDate',
                { $ne: ['$publishedDate', ''] },
                { $eq: ['$isPublishQueryRun', false] },
				{ $eq:["$fiscalYear", fiscalYearFilter] }
            ]
        }
    }
}, {
    $lookup: {
        from: 'enumeration',
        //localField: 'languageCode',
        //foreignField: 'languageCode',
		let:{
			languageCodeCk:'$languageCode'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$languageCode",  "$$languageCodeCk" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'localLanguage'
    }
},
{
    $lookup: {
        from: 'documentation',
        let: {
            firmId: '$abbreviation'
        },
        pipeline: [{
            $match:{
                $expr:{
                    $eq:['$type','KeyControl']
                }
            }
        },{
            $match: {
                $expr: {
                    $and: [{
                        $eq: ['$$firmId', '$firmId']
                    }, {
                            $ne: ['$status', 'StatusType_Draft']
                    },{
						$eq:["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as: 'keyControl'
    }
}, {
    $unwind: '$keyControl'
}, {
    $lookup: {
        from: 'globaldocumentation',
        let: {
            firmId: '$abbreviation',
            controlFunction: "$keyControl.controlFunction"
        },
        pipeline: [{
            $match:{
                // $expr:{
                    // $eq:['$type','Function']
                // }
				$expr: {
                            $and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$eq:['$type','Function']
								}
							]
                        }
            }
        },{
            $match: {
                $expr: {
                    $eq: [{ $toString: "$_id" }, "$$controlFunction"]
                }
            }
        }, {
            $lookup: {
                from: 'functionowner',
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [{
                                $eq: ['$$firmId', '$firmId']
                            },
                            {
                                $eq: ['$$controlFunction', '$functionId']
                            },
							{
								$eq:["$fiscalYear", fiscalYearFilter]
							}
                            ]
                        }
                    }
                }],
                as: 'functionowner'
            }
        }, {
            $unwind: {
                path: '$functionowner',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    foTitle: '$functionowner.title'
                },
                pipeline: [{
                    $match: {
                        // $expr: {
                            // $eq: ['$$foTitle', '$_id']
                        // }
						$expr: {
                            $and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$eq: ['$$foTitle', '$_id']
								}
							]
                        }
                    }
                }, {
                    $lookup: {
                        from: 'titleassignment',
                        let: {
                            titleId: {
                                $toString: '$_id'
                            }
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [{
                                        $eq: ['$$titleId', '$titleId']
                                    }, {
                                        $eq: ['$$firmId', '$firmId']
                                    },{
										$eq:["$fiscalYear", fiscalYearFilter]
									}]
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
                as: 'foTitle'
            }
        }, {
            $unwind: {
                path: '$foTitle',
                preserveNullAndEmptyArrays: true
            }
        }],
        as: 'firmFunctions'
    }
}, {
    $unwind: {
        path: '$firmFunctions',
        preserveNullAndEmptyArrays: true
    }
}, {
    $lookup: {
        from: 'keycontrolresource',
        localField: 'keyControl.controlType',
        foreignField: '_id',
        as: 'keyControl.controlType'
    }
}, {
    $unwind: {
        path: '$keyControl.controlType',
        preserveNullAndEmptyArrays: true
    }
}, {
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'keyControl.controlNature',
        //foreignField: '_id',
		let:{
			keyCtrlNature:'$keyControl.controlNature'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$_id",  "$$keyCtrlNature" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.controlNature'
    }
}, {
    $unwind: {
        path: '$keyControl.controlNature',
        preserveNullAndEmptyArrays: true
    }
}, {
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'keyControl.frequencyType',
        //foreignField: '_id',
		let:{
			keyCtrlFreqType:'$keyControl.frequencyType'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$keyCtrlFreqType" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.frequencyType'
    }
},
{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'keyControl.serviceLine',
        //foreignField: '_id',
		let:{
			keyCtrlServiceLine:'$keyControl.serviceLine'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									  $in: [ "$_id",  "$$keyCtrlServiceLine"] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.serviceLine'
    }
},{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'keyControl.addInformationExecutionControls.ipeCategory',
        //foreignField: '_id',
		let:{
			keyCtrlAddInfoIpeCategory:'$keyControl.addInformationExecutionControls.ipeCategory'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$keyCtrlAddInfoIpeCategory" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.ipeCategory'
    }
}, {
    $lookup: {
        from: 'processdetails',
        let: {
            uniqueId: '$keyControl.uniqueId',
            firmId: '$abbreviation'
        },
        pipeline: [{
            $match: {
                $expr: {
                    $anyElementTrue: {
                        $map: {
                            input: "$firmProcesses",
                            as: 'fp',
                            in: { $and: [{ $in: ['$$uniqueId', '$$fp.mappedKeyControls'] }, { $eq: ['$$fp.firmId', '$$firmId'] }] }
                        }
                    }
                }
            }
        },
        {
            $lookup: {
                from: 'title',
                let: {
                    firmProcess: '$firmProcesses'
                },
                pipeline: [{
                    $match: {
                        $expr: {
							$and:[{
									$in: [
									'$_id',
									{
										$cond: {
											if: {
												$and: ['$$firmProcess', {
													$isArray: '$$firmProcess'
												}]
											},
											then: {
												$reduce: {
													input: '$$firmProcess',
													initialValue: [],
													in: { $concatArrays: ['$$value',{$cond:{if:{$and:[{$isArray:'$$this.processOwners'},{ $eq: ['$$firmId', '$$this.firmId'] }]},then:'$$this.processOwners',else:[]}}] }
												}
											},
											else: []
										}
									}
								]
							},{
								$eq:["$fiscalYear", fiscalYearFilter]
							}]
                        }
                    }
                }, {
                    $lookup: {
                        from: 'titleassignment',
                        let: {
                            titleId: {
                                $toString: '$_id'
                            }
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [{
                                        $eq: ['$$titleId', '$titleId']
                                    }, {
                                        $eq: ['$$firmId', '$firmId']
                                    },{
										$eq:["$fiscalYear", fiscalYearFilter]
									}]
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
                as: 'poTitles'
            }
        }],
        as: 'keyControl.processes'
    }
}, {
    $unwind: {
        path: "$keyControl.processes",
        preserveNullAndEmptyArrays: true
    }
}, {
    $lookup: {
        from: 'documentation',
        //localField: 'keyControl.relatedSubRisks',
        //foreignField: 'uniqueId',
		let:{
			keyCtrlRelSubRisks:'$keyControl.relatedSubRisks'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  "$$keyCtrlRelSubRisks" ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.relatedSubRisks'
    }
}, {
    $addFields: {
        'keyControl.relatedQualityRisks': {
            $setUnion: ['$keyControl.relatedQualityRisks', {
                $reduce: {
                    input: '$keyControl.relatedSubRisks.relatedQualityRisks',
                    initialValue: [],
                    in: { $concatArrays: ['$$value', '$$this'] }
                }
            }]
        }
	    }
}, {
    $lookup: {
        from: 'documentation',
        //localField: 'keyControl.relatedQualityRisks',
        //foreignField: 'uniqueId',
		let:{
			keyCtrlRelQtlRisks:'$keyControl.relatedQualityRisks'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  "$$keyCtrlRelQtlRisks" ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.relatedQualityRisks'
    }
}, {
    $lookup: {
        from: 'documentation',
        //localField: 'keyControl.relatedQualityRisks.relatedObjectives',
        //foreignField: 'uniqueId',
		let:{
			keyCtrlRelQtlRisksRelObj:'$keyControl.relatedObjectives'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  { $ifNull: ['$$keyCtrlRelQtlRisksRelObj',[]] } ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.relatedObjectives'
    }
}, {
    $lookup: {
        from: 'documentation',
        let: {
            systemIds: {
                $cond: {
                    if: { $isArray: '$keyControl.addInformationExecutionControls' },
                    then: {
                        $reduce: {
                            input: '$keyControl.addInformationExecutionControls.ipeSystems',
                            initialValue: [],
                            in: { $concatArrays: ['$$value', '$$this'] }
                        }
                    },
                    else: []
                }
            }
        },
        pipeline: [{
            $match:{
					//type:'Resource'
					$and:[{
						type: 'Resource',
						fiscalYear: fiscalYearFilter
					}]
				}
        },{
            $match: {
                $expr: {
                    $in: [{ $toString: '$_id' }, '$$systemIds']
                }
            }
        }],
        as: 'keyControl.ipeSystems'
    }
}, {
    $lookup: {
        from: 'documentation',
        let: {
            mitigatedResourceIds: {
                $cond: {
                    if: { $isArray: '$keyControl.mitigatedResources' },
                    then:'$keyControl.mitigatedResources' ,
                    else: []
                }
            }
        },
        pipeline: [{
            $match:{
					//type:'Resource'
					$and:[{
						type: 'Resource',
						fiscalYear: fiscalYearFilter
					}]
				}
        },{
            $match: {
                $expr: {
                    $in: [{ $toString: '$_id' }, '$$mitigatedResourceIds']
                }
            }
        }],
        as: 'keyControl.mitigatedResourceObjs'
    }
}, {
    $lookup: {
        from: 'documentation',
        let: {
            supportingITApplicationIds: {
                $cond: {
                    if: { $isArray: '$keyControl.supportingITApplication' },
                    then:'$keyControl.supportingITApplication' ,
                    else: []
                }
            }
        },
        pipeline: [{
            $match:{
					//type:'Resource'
					$and:[{
						type: 'Resource',
						fiscalYear: fiscalYearFilter
					}]
				}
        },{
            $match: {
                $expr: {
                    $in: [{ $toString: '$_id' }, '$$supportingITApplicationIds']
                }
            }
        }],
        as: 'keyControl.supportingITApplicationObjs'
    }
},{
    $lookup: {
        from: 'title',
        let: {
            titleIds: '$keyControl.responseOwners'
        },
        pipeline: [{
            $match: {
                // $expr: {
                    // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                // }
				$expr:{
                    $and:[{
                        $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                    },{
						$eq:["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as: 'keyControl.responseOwnerTitles'
    }
}, {
    $lookup: {
        from: 'titleassignment',
        let: {
            titleIds: {
                $map: {
                    input: '$keyControl.responseOwnerTitles._id',
                    as: 'titleId',
                    in: { $toString: '$$titleId' }
                }
            },
            firmId: '$keyControl.firmId'
        },
        pipeline: [{
            $match: {
                $expr: {
                    $and: [{
                        $in: ['$titleId', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                    }, {
                        $eq: ['$firmId', '$$firmId']
                    },{
						$eq:["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as: 'keyControl.responseOwnerTitleAssignments'
    }
}, {
    $lookup: {
        from: 'title',
        let: {
            titleIds: '$keyControl.controlOperator'
        },
        pipeline: [{
            $match: {
                // $expr: {
                    // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                // }
				$expr:{
                    $and:[{
                        $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                    },{
						$eq:["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as: 'keyControl.controlOperatorTitles'
    }
},
{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'keyControl.typeOfEngagements',
        //foreignField: '_id',
		let:{
			keyCtrlTypeEngagements:'$keyControl.typeOfEngagements'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$keyCtrlTypeEngagements" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'keyControl.typeOfEngagements'
    }
},
{
    $lookup: {
        from: 'globaldocumentation',
        let: {
		       tagIds: '$keyControl.tags'
            },
        pipeline: [{
            $match: {
                // $expr: {
                    // $in: [{$toString: '$_id'}, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
                // }
				$expr:{
                    $and:[{
                        $in: [{$toString: '$_id'}, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
                    },{
						$eq:["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as: 'keyControl.tags'
    }
},{
    $lookup: {
        from: 'titleassignment',
        let: {
            titleIds: {
                $map: {
                    input: '$keyControl.controlOperatorTitles._id',
                    as: 'titleId',
                    in: { $toString: '$$titleId' }
                }
            },
            firmId: '$keyControl.firmId'
        },
        pipeline: [{
            $match: {
                $expr: {
                    $and: [{
                        $in: ['$titleId', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                    }, {
                        $eq: ['$firmId', '$$firmId']
                    },{
						$eq:["$fiscalYear", fiscalYearFilter]
					}]
                }
            }
        }],
        as: 'keyControl.controlOperatorTitleAssignments'
    }
}, {
    $project: {
        _id: 0,
        UniqueControlID: '$keyControl.uniqueId',
        EntityId: '$abbreviation',
        NetworkId: {
            $cond: { if: { $eq: ['$type', "EntityType_Network"] }, then: '$abbreviation', else: '' }
        },
        AreaId: {
            $cond: { if: { $eq: ['$type', "EntityType_Area"] }, then: '$abbreviation', else: '' }
        },
        RegionId: {
            $cond: { if: { $eq: ['$type', "EntityType_Region"] }, then: '$abbreviation', else: '' }
        },
        ClusterId: {
            $cond: { if: { $eq: ['$type', "EntityType_Cluster"] }, then: '$abbreviation', else: '' }
        },
        SubClusterId: {
            $cond: { if: { $eq: ['$type', "EntityType_SubCluster"] }, then: '$abbreviation', else: '' }
        },
        MemberFirmId: {
            $cond: { if: { $or: [{ $eq: ['$type', "EntityType_MemberFirm"] }, { $eq: ['$type', "EntityType_Group"] }] }, then: '$abbreviation', else: '' }
        },
        EntityType: {
            $switch: {
                branches: [
                    { case: { $eq: ['$type', "EntityType_Area"] }, then: "Area" },
                    { case: { $eq: ['$type', "EntityType_Cluster"] }, then: "Cluster" },
                    { case: { $eq: ['$type', "EntityType_Group"] }, then: "Location" },
                    { case: { $eq: ['$type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                    { case: { $eq: ['$type', "EntityType_Network"] }, then: "Network" },
                    { case: { $eq: ['$type', "EntityType_Region"] }, then: "Region" },
                    { case: { $eq: ['$type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                ],
                default: ''
            }
        },
        ControlName: '$keyControl.controlName',
		ControlNameLocal: '$keyControl.controlNameAlt',
        ControlDescription: '$keyControl.description',
		ControlDescriptionLocal: '$keyControl.descriptionAlt',
        ControlType: { $cond: { if: '$keyControl.controlType', then: '$keyControl.controlType.name', else: '' } },
        ControlNature: { $cond: { if: '$keyControl.controlNature', then: '$keyControl.controlNature.name', else: '' } },
		RelatedPolicyProcedureManualGuidance: '$keyControl.relatedPolicy',
		RelatedPolicyProcedureManualGuidanceLocal: '$keyControl.relatedPolicyAlt',
		ServiceProviderName: '$keyControl.nameOfControlServiceProvider',
		ServiceProviderNameLocal: '$keyControl.nameOfControlServiceProviderAlt',
        Frequency: {
            $reduce: {
                input: '$keyControl.frequencyType',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        EngagementType:{
            $reduce: {
                input: '$keyControl.typeOfEngagements',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        Tags: {
            $reduce: {
                input: '$keyControl.tags',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        FunctionBaselineLocalControlDesign: {
            $reduce: {
                input: '$keyControl.controlFunction',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        FunctionBaselineControlExecution: {
            $reduce: {
                input: '$keyControl.executionControlFunction',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
		ControlOwnerTitles: {
            $reduce: {
                input: '$keyControl.responseOwnerTitles',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        ControlOwnerTitleAssignments: {
            $reduce: {
                input: {
                    $reduce: {
                        input: '$keyControl.responseOwnerTitleAssignments',
                        initialValue: [],
                        in: { $concatArrays: ['$$value', '$$this.assignments'] }
                    }
                },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.email'] }
            }
        },
		ControlOperatorTitles: {
            $reduce: {
                input: '$keyControl.controlOperatorTitles',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        ControlOperatorTitleAssignments: {
            $reduce: {
                input: {
                    $reduce: {
                        input: '$keyControl.controlOperatorTitleAssignments',
                        initialValue: [],
                        in: { $concatArrays: ['$$value', '$$this.assignments'] }
                    }
                },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.email'] }
            }
        },
        'FunctionId':{
            $reduce: {
                input: '$keyControl.executionControlFunction',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        }, 
        // {
        //     $toString: { $toString: '$firmFunctions._id' }
        // },
        'FunctionName': '$firmFunctions.name',
        'FunctionOwnerTitle': {
            $cond: {
                if: '$firmFunctions.foTitle.name',
                then: '$firmFunctions.foTitle.name',
                else: ''
            }
        },
        'FunctionOwnerAssignments': {
            $cond: {
                if: '$firmFunctions.foTitle',
                then: {
                    $let: {
                        vars: {
                            assignments: {
                                $cond: {
                                    if: {
                                        $isArray: '$firmFunctions.foTitle.titleAssignments.assignments'
                                    },
                                    then: '$firmFunctions.foTitle.titleAssignments.assignments',
                                    else: []
                                }
                            }
                        },
                        in: {
                            $cond: {
                                if: {
                                    $and: [{ $isArray: '$$assignments' }, {
                                        $gt: [{
                                            $size: '$$assignments'
                                        }, 0]
                                    }]
                                },
                                then: {
                                    $reduce: {
                                        input: '$$assignments',
                                        initialValue: '',
                                        in: {
                                            $concat: ['$$value', {
                                                $cond: {
                                                    if: {
                                                        $ne: ['', '$$value']
                                                    },
                                                    then: '; ',
                                                    else: ''
                                                }
                                            }, '$$this.email']
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
        'ProcessUniqueId': '$keyControl.processes.uniqueId',
        'ProcessName': '$keyControl.processes.name',
        'ProcessDescription': '$keyControl.processes.description',
        'ProcessOwnerTitlesa': '$keyControl.processes.poTitles',
        'ProcessOwnerTitles': {
            $reduce: {
                input: '$keyControl.processes.poTitles',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        'ProcessOwnerAssignments': {
            $cond: {
                if: { $and: [{ $isArray: '$keyControl.processes.poTitles' }, { $gt: [{ $size: '$keyControl.processes.poTitles' }, 0] }] },
                then: {
                    $let: {
                        vars: {
                            processOwnerAssignments: {
                                $reduce: {
                                    input: '$keyControl.processes.poTitles',
                                    initialValue: [],
                                    in: {
                                        $cond: {
                                            if: {$and:[{ $isArray: '$$this.titleAssignments.assignments' },{$gt:[{$size:'$$this.titleAssignments.assignments'},0]}]},
                                            then: { $concatArrays: ['$$value', '$$this.titleAssignments.assignments'] },
                                            else: { $concatArrays: ['$$value', [{email:'no assignment'}]] }
                                        }
                                    }
                                }
                            }
                        },
                        in: {
                            $cond: {
                                if: {
                                    $and: [{ $isArray: '$$processOwnerAssignments' }, {
                                        $gt: [{
                                            $size: '$$processOwnerAssignments'
                                        }, 0]
                                    }]
                                },
                                then: {
                                    $reduce: {
                                        input: '$$processOwnerAssignments',
                                        initialValue: '',
                                        in: {
                                            $concat: ['$$value', {
                                                $cond: {
                                                    if: {
                                                        $ne: ['', '$$value']
                                                    },
                                                    then: '; ',
                                                    else: ''
                                                }
                                            }, '$$this.email']
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
        ControlObjective: '$keyControl.keyControlObjective',
		ControlObjectiveLocal: '$keyControl.keyControlObjectiveAlt',
        ControlCriteria: '$keyControl.controlCriteria',
		ControlCriteriaLocal: '$keyControl.controlCriteriaAlt',
        LocalLanguage: {
            $reduce: {
                input: '$localLanguage.name',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
            }
        },
        SupportingEvidence: {
            $reduce: {
                input: '$keyControl.addControlEvidenceControls',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
            }
        },
        SupportingEvidenceLocal: {
            $reduce: {
                input: '$keyControl.addControlEvidenceControls.nameAlt',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
            }
        },
        IEC: {
            $reduce: {
                input: '$keyControl.addInformationExecutionControls',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        IECLocal: {
            $reduce: {
                input: '$keyControl.addInformationExecutionControls.nameAlt',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }

            }
        },
        IECReportType: {
            $reduce: {
                input: '$keyControl.ipeCategory',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        IsControlOverResource: '$keyControl.isControlOverResource',
		KeyControlOperatedByServiceProvider: '$keyControl.isControlOperatedByServiceProvider',
        ResourceType:  {
            $switch: {
                branches: [
                    { case: { $eq: ['$keyControl.resourceType', "ResourceType_Communication"] }, then: "Communication" },
                    { case: { $eq: ['$keyControl.resourceType', "ResourceType_Intellectual"] }, then: "Intellectual" },
                    { case: { $eq: ['$keyControl.resourceType', "ResourceType_Learning"] }, then: "Learning" },
                    { case: { $eq: ['$keyControl.resourceType', "ResourceType_Technology"] }, then: "Technology" }
                ],
                default: ''
            },
        },
        ResourceName:{
            $reduce: {
                input: '$keyControl.mitigatedResourceObjs',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        SupportingITApplication:{
            $reduce: {
                input: '$keyControl.supportingITApplicationObjs',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

            }
        },
        ServiceLine: {
            $cond:{
                if:{$ne:['$keyControl.isControlOverResource',true]},
                then:{
                    $reduce: {
                        input: '$keyControl.serviceLine',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                    }
                },
                else:''
            }
        },
        QualityObjectiveUniquesIds: {
            $reduce: {
                input: '$keyControl.relatedObjectives.qualityObjectiveId',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        QualityRiskUniqueIds: {
            $reduce: {
                input: '$keyControl.relatedQualityRisks.uniqueId',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        QualitySubRiskUniqueIds: {
            $reduce: {
                input: '$keyControl.relatedSubRisks.uniqueId',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
        Draft: { $eq: ['$keyControl.status', 'StatusType_Draft'] },
        IECSystems: {
            $reduce: {
                input: '$keyControl.ipeSystems',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        qualityRiskUniqueIdArray: '$keyControl.relatedQualityRisks.uniqueId',
        qualityObjectiveUniqueIdArray: {
            $setUnion: [{
                $reduce: {
                    input: '$keyControl.relatedQualityRisks.relatedObjectives',
                    initialValue: [],
                    in: { $concatArrays: ['$$value', '$$this'] }
                }
            }]
        },
        ResourceIds: {
            $reduce: {
                input: { $cond: { if: { $isArray: '$keyControl.mitigatedResources' }, then: '$keyControl.mitigatedResources', else: [] } },
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        },
		IsControlOverResourceNew:{
			$switch: {
                branches: [
                    { case: { $eq: ['$keyControl.isControlOverResource', true] }, then: "Yes" },
                    { case: { $eq: ['$keyControl.isControlOverResource', false] }, then: "No" },
                ],
                default: ''
            }
        },
		KeyControlOperatedByServiceProviderNew:{
			$switch: {
                branches: [
                    { case: { $eq: ['$keyControl.isControlOperatedByServiceProvider', true] }, then: "Yes" },
                    { case: { $eq: ['$keyControl.isControlOperatedByServiceProvider', false] }, then: "No" },
                ],
                default: ''
            },
        }
    }
}, {
    $out: 'sqmarcherkeycontroltemp'
}])

//To be  used while migrating to mongo 5
// {
//     $merge: {
//         into:'sqmarcherkeycontrol'
//         on:'UniqueControlID',
//         whenMatched:'replace',
//         whenNotMatched: "insert"
//     }
// }


//Quality Risk
db.documentation.aggregate([{
    $match:{
        //type:'QualityRisk'
		$and:[{
            type:'QualityRisk',
			fiscalYear: fiscalYearFilter
        }]
    }
},{
    $lookup: {
        from: 'firm',
        //localField: 'firmId',
        //foreignField: 'abbreviation',
		let:{
			firmIdCk:'$firmId'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$abbreviation",  "$$firmIdCk" ] ,
								},
								{
									$eq:["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'firm'
    }
}, {
    $unwind: '$firm'
}, {
    $match: {
        $expr: {
            $and: [
                '$firm.publishedDate',
                { $ne: ['$firm.publishedDate', ''] },
                { $eq: ['$firm.isPublishQueryRun', false] }
            ]
        }
    }
}, {
    $lookup: {
        from: 'documentation',
        //localField: 'relatedObjectives',
        //foreignField: 'uniqueId',
		let:{
			relatedObjectivesCk:'$relatedObjectives'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  "$$relatedObjectivesCk" ] ,
								},
								{
									$eq:["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'relatedObjectives'
    }
}, {
    $project: {
        QualityRiskUniqueId: '$uniqueId',
        EntityId: '$firmId',
        NetworkId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Network"] }, then: '$firmId', else: '' }
        },
        AreaId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Area"] }, then: '$firmId', else: '' }
        },
        RegionId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Region"] }, then: '$firmId', else: '' }
        },
        ClusterId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: '$firmId', else: '' }
        },
        SubClusterId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: '$firmId', else: '' }
        },
        MemberFirmId: {
            $cond: { if: { $or: [{ $eq: ['$firm.type', "EntityType_MemberFirm"] }, { $eq: ['$firm.type', "EntityType_Group"] }] }, then: '$firmId', else: '' }
        },
        EntityType: {
            $switch: {
                branches: [
                    { case: { $eq: ['$firm.type', "EntityType_Area"] }, then: "Area" },
                    { case: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: "Cluster" },
                    { case: { $eq: ['$firm.type', "EntityType_Group"] }, then: "Location" },
                    { case: { $eq: ['$firm.type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                    { case: { $eq: ['$firm.type', "EntityType_Network"] }, then: "Network" },
                    { case: { $eq: ['$firm.type', "EntityType_Region"] }, then: "Region" },
                    { case: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                ],
                default: ''
            }
        },
        QualityRiskName: '$name',
        QualityRiskDescription: '$description',
        QualityObjectiveUniqueIds: {
            $reduce: {
                input: '$relatedObjectives.qualityObjectiveId',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        }
    }
}, {
    $out: 'sqmarcherqualityrisktemp'
}])


//Sub-Risk 
db.documentation.aggregate([{
    $match:{
        //type:'SubRisk'
		$and:[{
			type:'SubRisk',
			fiscalYear: fiscalYearFilter
        }]
    }
},{
    $lookup: {
        from: 'firm',
        //localField: 'firmId',
        //foreignField: 'abbreviation',
		let:{
			firmIdCk:'$firmId'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$abbreviation",  "$$firmIdCk" ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'firm'
    }
}, {
    $unwind: '$firm'
}, {
    $match: {
        $expr: {
            $and: [
                '$firm.publishedDate',
                { $ne: ['$firm.publishedDate', ''] },
                { $eq: ['$firm.isPublishQueryRun', false] }
            ]
        }
    }
}, {
    $project: {
        QualitySubRiskUniqueId: '$uniqueId',
        EntityId: '$firmId',
        NetworkId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Network"] }, then: '$firmId', else: '' }
        },
        AreaId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Area"] }, then: '$firmId', else: '' }
        },
        RegionId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Region"] }, then: '$firmId', else: '' }
        },
        ClusterId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: '$firmId', else: '' }
        },
        SubClusterId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: '$firmId', else: '' }
        },
        MemberFirmId: {
            $cond: { if: { $or: [{ $eq: ['$firm.type', "EntityType_MemberFirm"] }, { $eq: ['$firm.type', "EntityType_Group"] }] }, then: '$firmId', else: '' }
        },
        EntityType: {
            $switch: {
                branches: [
                    { case: { $eq: ['$firm.type', "EntityType_Area"] }, then: "Area" },
                    { case: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: "Cluster" },
                    { case: { $eq: ['$firm.type', "EntityType_Group"] }, then: "Location" },
                    { case: { $eq: ['$firm.type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                    { case: { $eq: ['$firm.type', "EntityType_Network"] }, then: "Network" },
                    { case: { $eq: ['$firm.type', "EntityType_Region"] }, then: "Region" },
                    { case: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                ],
                default: ''
            }
        },
        QualitySubRiskName: '$name',
        QualitySubRiskDescription: '$description',
        QualityRiskId: {
            $reduce: {
                input: '$relatedQualityRisks',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
            }
        }
    }
}, {
    $out: 'sqmarcherqualitysubrisktemp'
}])


//To be  used while migrating to mongo 5
// {
//     $merge: {
//         into:'sqmarcherqualityrisk'
//         on:'QualityRiskUniqueId',
//         whenMatched:'replace',
//         whenNotMatched: "insert"
//     }
// }

//Quality Objective
db.documentation.aggregate([{
    $match:{
        //type:'QualityObjective'
		$and:[{
			type:'QualityObjective',
			fiscalYear: fiscalYearFilter
        }]
    }
},{
    $lookup: {
        from: 'firm',
        //localField: 'firmId',
        //foreignField: 'abbreviation',
		let:{
			firmIdCk:'$firmId'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$abbreviation",  "$$firmIdCk" ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'firm'
    }
}, {
    $unwind: '$firm'
}, {
    $match: {
        $expr: {
            $and: [
                '$firm.publishedDate',
                { $ne: ['$firm.publishedDate', ''] },
                { $eq: ['$firm.isPublishQueryRun', false] }
            ]
        }
    }
}, {
    $lookup: {
        from: 'component',
        //localField: 'componentId',
        //foreignField: '_id',
		let:{
			componentIdCk:'$componentId'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $eq: [ "$_id",  "$$componentIdCk" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'component'
    }
}, {
    $unwind: '$component'
}, {
    $project: {
        QualityObjectiveID: '$qualityObjectiveId',
        EntityId: '$firmId',
        NetworkId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Network"] }, then: '$firmId', else: '' }
        },
        AreaId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Area"] }, then: '$firmId', else: '' }
        },
        RegionId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Region"] }, then: '$firmId', else: '' }
        },
        ClusterId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: '$firmId', else: '' }
        },
        SubClusterId: {
            $cond: { if: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: '$firmId', else: '' }
        },
        MemberFirmId: {
            $cond: { if: { $or: [{ $eq: ['$firm.type', "EntityType_MemberFirm"] }, { $eq: ['$firm.type', "EntityType_Group"] }] }, then: '$firmId', else: '' }
        },
        EntityType: {
            $switch: {
                branches: [
                    { case: { $eq: ['$firm.type', "EntityType_Area"] }, then: "Area" },
                    { case: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: "Cluster" },
                    { case: { $eq: ['$firm.type', "EntityType_Group"] }, then: "Location" },
                    { case: { $eq: ['$firm.type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                    { case: { $eq: ['$firm.type', "EntityType_Network"] }, then: "Network" },
                    { case: { $eq: ['$firm.type', "EntityType_Region"] }, then: "Region" },
                    { case: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                ],
                default: ''
            }
        },
        QualityObjectiveName: '$name',
        QualityObjectiveDescription: '$description',
        Component: '$component.name'
    }
}, {
    $out: 'sqmarcherqualityobjectivetemp'
}])

//To be  used while migrating to mongo 5
// {
//     $merge: {
//         into:'sqmarcherqualityobjective'
//         on:'QualityObjectiveID',
//         whenMatched:'replace',
//         whenNotMatched: "insert"
//     }
// }


//Resource
db.firm.aggregate([{
    $match: {
        $expr: {
            $and: [
                '$publishedDate',
                { $ne: ['$publishedDate', ''] },
                { $eq: ['$isPublishQueryRun', false] },
				{ $eq: ["$fiscalYear", fiscalYearFilter]}
            ]
        }
    }
},{
    $lookup: {
        from: 'documentation',
        let: {
            firmId: '$abbreviation'
        },
        pipeline: [
            { $match:
               { $expr:
                  { $and:
                    [
                       { $eq: [ "$type",  "Resource" ] },
                       { $eq: [ "$firmId", "$$firmId" ] },
					   { $eq: ["$fiscalYear", fiscalYearFilter]}
                    ]
                  }
               }
            }
        ],
        as: "resourcesCreatedAtPublishedEntity"
    }
},{
    $unwind: "$resourcesCreatedAtPublishedEntity"
},{
    $addFields:{
        'resourcesCreatedAtPublishedEntity.entityType':'$type'
    }
},{
    $replaceRoot:{
        newRoot:'$resourcesCreatedAtPublishedEntity'
    }
},{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'engagementType',
        //foreignField: '_id',
		let:{
			engagementTypeCk:'$engagementType'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$engagementTypeCk" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'engagementType'
    }
},{
    $lookup: {
        from: 'keycontrolresource',
        //localField: 'serviceLine',
        //foreignField: '_id',
		let:{
			serviceLineCk:'$serviceLine'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$_id",  "$$serviceLineCk" ] ,
								},
								{
									$in: [fiscalYearFilter, "$fiscalYear"]
								}
							]
                        }
                    }
                }],
        as: 'serviceLine'
    }
},{
    $lookup: {
        from: 'globaldocumentation',
        let: {
            functionId: '$relatedFunction'
        },
        pipeline: [{
            $match:{
                $expr:{
                    //$eq:['$type','Function']
					$and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$eq:['$type','Function']
								}
						  ]
                }
            }
        },{
            $match: {
                $expr: {
                    $eq: ['$$functionId', { $toString: '$_id' }]
                }
            }
        }],
        as: 'function/SL'
    }
},{
    $lookup: {
        from: 'globaldocumentation',
        let: {
            functionId: '$techRelatedFunction'
        },
        pipeline: [{
            $match:{
                $expr:{
                    //$eq:['$type','Function']
					$and: [
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								},
								{
									$eq:['$type','Function']
								}
						  ]
                }
            }
        },{
            $match: {
                $expr: {
                    $eq: ['$$functionId', { $toString: '$_id' }]
                }
            }
        }],
        as: 'technologyRelatedFunction'
    }
},{
    $lookup: {
        from: 'documentation',
        //localField: 'relatedQualityRisks',
        //foreignField: 'uniqueId',
		let:{
			relatedQualityRisksCk:'$relatedQualityRisks'
        },
		pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
								{
									 $in: [ "$uniqueId",  "$$relatedQualityRisksCk" ] ,
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                        }
                    }
                }],
        as: 'qualityRisks'
    }
},{
    $lookup: {
        from: 'globaldocumentation',
        let: {
            tagId: '$tags'
        },
        pipeline: [{
            $match: {
                $expr: {
                    //$eq: ['$type', 'Tag']
					$and: [
								{
									 $eq: ['$type', 'Tag'] 
								},
								{
									$eq: ["$fiscalYear", fiscalYearFilter]
								}
							]
                }
            }
        },{
            $match: {
                $expr: {
                    $in: [{ $toString: '$_id' }, {$cond:{if:{$isArray:'$$tagId'},then:'$$tagId',else:[]}}]
                }
            }
        }],
        as: 'tag'
    }
},{
    $project: {
        '_id': 0,
        'UniqueId': { $toString: '$_id' },
        'ResourceType': {
            $switch: {
                branches: [
                    { case: { $eq: ['$resourceType', "ResourceType_Communication"] }, then: "Communication" },
                    { case: { $eq: ['$resourceType', "ResourceType_Intellectual"] }, then: "Intellectual" },
                    { case: { $eq: ['$resourceType', "ResourceType_Learning"] }, then: "Learning" },
                    { case: { $eq: ['$resourceType', "ResourceType_Technology"] }, then: "Technology" }
                ],
                default: ''
            }
        },
        'Name': '$name',
        'Description': '$description',
        'EntityId': '$firmId',
        'EntityType': {
            $switch: {
                branches: [
                    { case: { $eq: ['$entityType', "EntityType_Area"] }, then: "Area" },
                    { case: { $eq: ['$entityType', "EntityType_Cluster"] }, then: "Cluster" },
                    { case: { $eq: ['$entityType', "EntityType_Group"] }, then: "Location" },
                    { case: { $eq: ['$entityType', "EntityType_MemberFirm"] }, then: "Member Firm" },
                    { case: { $eq: ['$entityType', "EntityType_Network"] }, then: "Network" },
                    { case: { $eq: ['$entityType', "EntityType_Region"] }, then: "Region" },
                    { case: { $eq: ['$entityType', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                ],
                default: ''
            }
        },
        'ResourceCategory':  {
            $switch: {
                branches: [
                    { case: { $eq: ['$categoryofResource', "ResourceCategory_A"] }, then: "Category A" },
                    { case: { $eq: ['$categoryofResource', "ResourceCategory_B"] }, then: "Category B" },
                    { case: { $eq: ['$categoryofResource', "ResourceCategory_C"] }, then: "Category C" },
                    ],
                default: ''
            }
        },
        'Function/SL': {
            $reduce: {
                input: '$function/SL',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        'TechnologyFunction': {
            $reduce: {
                input: '$technologyRelatedFunction',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        'TypeOfEngagements': {
            $reduce: {
                input: '$engagementType',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        'ServiceLineToWhichThisResourceApplies': {
            $reduce: {
                input: '$serviceLine',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        'CommunicationMethod': '$communication',
        'IsResourceProvidedByService': {
            $switch: {
                branches: [
                    {case: {$eq: ['$isResourceProvidedByService', true]}, then: "Yes"},
                    {case: {$eq: ['$isResourceProvidedByService', false]}, then: "No"}
                ],
                default: ''
            }
        },
        'NameOfServiceProvider': '$nameOfServiceProvider',
        'RelatedQualityRisks': {
            $reduce: {
                input: '$qualityRisks',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } },'$$this.uniqueId', ':', '$$this.name'] }
            }
        },
        'LinkToResource': '$link',
        'LinkToUserGuide': '$linkToUserGuide',
        'Tags': {
            $reduce: {
                input: '$tag',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        }
    }
},{
    $out:'sqmarcherresourcestemp'
}])


//function
db.firm.aggregate([{
    $match: {
        $expr: {
            $and: [
                '$publishedDate',
                { $ne: ['$publishedDate', ''] },
                { $eq: ['$isPublishQueryRun', false] },
				{ $eq: ["$fiscalYear", fiscalYearFilter] }
            ]
        }
    }
},{  
    $graphLookup: {
        from: "firm",
        startWith: "$parentFirmId",
        connectFromField: "parentFirmId",
        connectToField: "abbreviation",
        as: "parentFirm",
        restrictSearchWithMatch: {
            "isPartOfGroup": "IsPartOfGroupType_No",
			"fiscalYear": fiscalYearFilter
        }        
    }
},
{ $addFields:{
    firmIds:{
        $concatArrays: [
            ["$abbreviation"], "$parentFirm.abbreviation"
        ]
    },
}
},
{
    $lookup: {
        from :'globaldocumentation',
        let: { "firms": "$firmIds" ,
                },
        pipeline: [
            { "$match": 
                { "$expr":{
                    "$and":[ 
                            { "$eq": [ "$type" , 'Function' ] },                       
                            {
                                $gt:[{$size:{$setIntersection:['$$firms','$hierarchy']}},0]
                            },
							{
								$eq: ["$fiscalYear", fiscalYearFilter]
							}
                        ]
                    }
                 }
             },
             { "$project": { "_id": 1, "name": 1 }}
          ],
        as:'functions'
    }
},
{
    $unwind:'$functions'
},
{
    $addFields: {
       "functions.firmIds": "$firmIds",
       "functions.abbreviation":"$abbreviation",
       "functions.parentFirms": "$parentFirm",
       "functions.type": "$type",
       "functions.functionId": { "$toString": "$_id" },
    }
 },
 {
    $replaceRoot:{
        newRoot:'$functions'
    }
},
{
    $lookup: {
        from :'functionowner',
        let: { "functionId":{$toString: "$_id"} ,
                firmId:'$abbreviation'
            },
        pipeline: [
            { "$match": 
                { "$expr":{
                    "$and":[ 
                            { "$eq": ["$functionId", "$$functionId" ] },
                            { "$eq" :["$firmId",'$$firmId']},
							{ "$eq": ["$fiscalYear", fiscalYearFilter]}
                        ]
                    }
                 }
             },
             { "$project": { "title": 1,"notApplicable":1}}
          ],
        as:'functionTitle'
    }
},
{
    $unwind:{
        path:'$functionTitle',
        preserveNullAndEmptyArrays: true
    }
},{
    $match:{
        'functionTitle.notApplicable':{
            $ne:true
        }
    }
},
{
    $lookup: {
        from :'title',
        let: { "titleId": "$functionTitle.title"},
        pipeline: [
            { "$match": 
                { "$expr":{
                    "$and":[ 
                            { "$eq": [ "$$titleId", "$_id" ] },
							{ "$eq": ["$fiscalYear", fiscalYearFilter] }
                        ]
                    }
                 }
             },
             { "$project": { "name": 1}}
          ],
        as:'functionLeaderTitle'
    }
},
{
    $unwind: {
        path:'$functionLeaderTitle',
        preserveNullAndEmptyArrays: true
    }
},
{
    $lookup: {
        from :'titleassignment',
        let: { "titleId": "$functionLeaderTitle._id" ,
                "firmId": "$abbreviation"},
        pipeline: [
            { "$addFields": { "titleId": { "$toObjectId": "$titleId" }}},
            { "$match": 
                { "$expr":{
                    "$and":[ 
                            { "$eq": [ "$titleId", "$$titleId" ] },
                            { "$eq" :["$firmId",'$$firmId']},
							{ "$eq": ["$fiscalYear", fiscalYearFilter] }
                        ]
                    }
                 }
             }
          ],
        as:'functionLeaderTitleAssignment'
    }
},
{
    $unwind: {
        path:'$functionLeaderTitleAssignment',
        preserveNullAndEmptyArrays: true
    }
},{
    $unwind:{
        path:'$functionLeaderTitleAssignment.assignments',
        preserveNullAndEmptyArrays: true
    }
},{
    $project:{
            UniqueRowId:{$concat:[{$toString:"$_id"},'$abbreviation',{
                $cond:{
                    if:'$functionLeaderTitleAssignment.assignments.email',
                    then:'$functionLeaderTitleAssignment.assignments.email',
                    else:''
                }
            }]}, 
            FunctionId: {$toString:"$_id"},
            EntityFunctionId:{ $concat: [{$toString:"$_id"}, '$abbreviation'] },
            EntityId:'$abbreviation',
            NetworkId: {
                $cond: { if: { $eq: ['$type', "EntityType_Network"] }, then: '$abbreviation', else: '' }
            },
            AreaId: {
                $cond: { if: { $eq: ['$type', "EntityType_Area"] }, then: '$abbreviation', else: '' }
            },
            RegionId: {
                $cond: { if: { $eq: ['$type', "EntityType_Region"] }, then: '$abbreviation', else: '' }
            },
            ClusterId: {
                $cond: { if: { $eq: ['$type', "EntityType_Cluster"] }, then: '$abbreviation', else: '' }
            },
            SubClusterId: {
                $cond: { if: { $eq: ['$type', "EntityType_SubCluster"] }, then: '$abbreviation', else: '' }
            },
            MemberFirmId: {
                $cond: { if: { $or: [{ $eq: ['$type', "EntityType_MemberFirm"] }, { $eq: ['$type', "EntityType_Group"] }] }, then: '$abbreviation', else: '' }
            },
            EntityType: {
                        $switch: {
                            branches: [
                                { case: { $eq: ['$type', "EntityType_Area"] }, then: "Area" },
                                { case: { $eq: ['$type', "EntityType_Cluster"] }, then: "Cluster" },
                                { case: { $eq: ['$type', "EntityType_Group"] }, then: "Location" },
                                { case: { $eq: ['$type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                                { case: { $eq: ['$type', "EntityType_Network"] }, then: "Network" },
                                { case: { $eq: ['$type', "EntityType_Region"] }, then: "Region" },
                                { case: { $eq: ['$type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                            ],
                            default: ''
                        }
                    },
            FunctionName:'$name',     
            FunctionLeaderTitle:{
                $cond: {
                    if: '$functionLeaderTitle',
                        then: '$functionLeaderTitle.name',
                    else: ''
                }
            },
            FunctionLeaderTitleAssignment:'$functionLeaderTitleAssignment.assignments.email',
            FunctionLeaderTitleAssignmentDisplayName:'$functionLeaderTitleAssignment.assignments.displayName',
            _id:0
            },

},
{
    $out:'sqmarcherfunctiontemp'
}
])

             
//To be  used while migrating to mongo 5
// {
//     $merge: {
//         into:'sqmarcherresources'
//         on:'UniqueId',
//         whenMatched:'replace',
//         whenNotMatched: "insert"
//     }
// }

var publishedEntityIds = db.firm.find({ isPublishQueryRun: false }, { abbreviation: 1, _id: 0 }).toArray().map(f => f.abbreviation);

db.sqmarchersubrisk.remove({ EntityId: { $in: publishedEntityIds } });
db.sqmarcherkeycontrol.remove({ EntityId: { $in: publishedEntityIds } });
db.sqmarcherqualityrisk.remove({ EntityId: { $in: publishedEntityIds } });
db.sqmarcherqualityobjective.remove({ EntityId: { $in: publishedEntityIds } });
db.sqmarchertitleassignments.remove({ EntityId: { $in: publishedEntityIds } });
db.sqmarcherresources.remove({ EntityId: { $in: publishedEntityIds } });
db.sqmarcherfunction.remove({ EntityId: { $in: publishedEntityIds } });


//db.sqmarcherqualitysubrisktemp.copyTo('sqmarchersubrisk');
//db.sqmarcherkeycontroltemp.copyTo('sqmarcherkeycontrol');
// db.sqmarcherrequirementcontroltemp.copyTo('sqmarcherkeycontrol');
// db.sqmarcherqualityrisktemp.copyTo('sqmarcherqualityrisk');
// db.sqmarcherqualityobjectivetemp.copyTo('sqmarcherqualityobjective');
// db.sqmarcherresourcestemp.copyTo('sqmarcherresources');
// db.sqmarcherfunctiontemp.copyTo('sqmarcherfunction');

db.sqmarcherqualitysubrisktemp.aggregate( [   
   { $merge : { into : "sqmarchersubrisk" } }
] )
db.sqmarcherkeycontroltemp.aggregate( [   
   { $merge : { into : "sqmarcherkeycontrol" } }
] )
db.sqmarcherrequirementcontroltemp.aggregate( [   
   { $merge : { into : "sqmarcherkeycontrol" } }
] )
db.sqmarcherqualityrisktemp.aggregate( [   
   { $merge : { into : "sqmarcherqualityrisk" } }
] )
db.sqmarcherqualityobjectivetemp.aggregate( [   
   { $merge : { into : "sqmarcherqualityobjective" } }
] )
db.sqmarcherresourcestemp.aggregate( [   
   { $merge : { into : "sqmarcherresources" } }
] )
db.sqmarcherfunctiontemp.aggregate( [   
   { $merge : { into : "sqmarcherfunction" } }
] )

// //db.createCollection('sqmarchersubrisk');
// db.sqmarcherqualitysubrisktemp.find().forEach(function(doc){
//     db.sqmarchersubrisk.insert(doc); // start to replace
//  });

// //db.createCollection('sqmarcherkeycontrol');
// db.sqmarcherkeycontroltemp.find().forEach(function(doc){
//     db.sqmarcherkeycontrol.insert(doc); // start to replace
//  });
 
// //db.createCollection('sqmarcherkeycontrol');
// db.sqmarcherrequirementcontroltemp.find().forEach(function(doc){
//     db.sqmarcherkeycontrol.insert(doc); // start to replace
//  });
 
// //db.createCollection('sqmarcherqualityrisk');
// db.sqmarcherqualityrisktemp.find().forEach(function(doc){
//     db.sqmarcherqualityrisk.insert(doc); // start to replace
//  });
 
// //db.createCollection('sqmarcherqualityobjective');
// db.sqmarcherqualityobjectivetemp.find().forEach(function(doc){
//     db.sqmarcherqualityobjective.insert(doc); // start to replace
//  });
// //db.createCollection('sqmarcherresources');
// db.sqmarcherresourcestemp.find().forEach(function(doc){
//     db.sqmarcherresources.insert(doc); // start to replace
//  });
 
// //db.createCollection('sqmarcherfunction');
// db.sqmarcherfunctiontemp.find().forEach(function(doc){
//     db.sqmarcherfunction.insert(doc); // start to replace
//  });

//Update Fiscal Year
db.sqmarchersubrisk.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY23"}})
db.sqmarcherkeycontrol.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY23"}})
db.sqmarcherfunction.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY23"}})
db.sqmarcherresources.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY23"}})
db.sqmarcherqualityobjective.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY23"}})
db.sqmarcherqualityrisk.updateMany({"FiscalYear": { $exists: false }}, {$set: {"FiscalYear": "FY23"}})


//Drop temp collections
 db.sqmarcherkeycontroltemp.drop();
 db.sqmarcherqualityrisktemp.drop();
 db.sqmarcherqualityobjectivetemp.drop();
 db.sqmarcherresourcestemp.drop();
 db.sqmarcherrequirementcontroltemp.drop();
 db.sqmarcherfunctiontemp.drop();
 db.sqmarcherqualitysubrisktemp.drop();
//Update isPublishQueryRun property
db.firm.updateMany({isPublishQueryRun:false},{$set:{isPublishQueryRun:true}},{multi:true})