try {

    var fiscalYearFilter = "2026";

    fiscalYearFilter = fiscalYearFilter.split(",").map(function (year) {
        return parseInt(year, 10);
    });

    var autoQoNotReqFirms = 'USA';
    autoQoNotReqFirms = autoQoNotReqFirms.split(",").map(function (entity) {
        return entity;
    });
    var globalQOApplicabilityPolicyId = 'EXC-GLOBAL-LOC-GLOBAL-USA';
    var reBacPolicyQOIds = [];
    if (globalQOApplicabilityPolicyId) {
        reBacPolicyQOIds = db.rebacpolicy.distinct('objectId', {
            fiscalYear: {$in: fiscalYearFilter},
            policyId: globalQOApplicabilityPolicyId,
            active: true
        });
    }

    var publishedEntityIds =[ { abbreviation: 'USA', fiscalYear: 2026 },  { abbreviation: 'NTW', fiscalYear: 2026 }]
    if (!publishedEntityIds || publishedEntityIds.length === 0) {
        print("No published entities found.");
    } else {
        var orConditions = publishedEntityIds.map(entity => ({
            $and: [
                { $eq: ["$firm.abbreviation", entity.abbreviation] },
                { $eq: ["$firm.fiscalYear", entity.fiscalYear] }
            ]
        }));

        db.globaldocumentation.aggregate([{
            $match: {
                $expr: {
                    $and: [
                        { $in: ["$fiscalYear", fiscalYearFilter] },
                        { $eq: ['$type', "Process"] }
                    ]
                }
            }
        }, {
            $lookup: {
                from: "firmprocess",
                let: {
                    uniqueIdCk: '$uniqueId',
                    fiscalYearOfProcess: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$processId", "$$uniqueIdCk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfProcess"]
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
            $match: {
                $expr: {
                    $and: [
                        { $in: ["$fiscalYear", fiscalYearFilter] }
                    ]
                }
            }
        }, {
            $lookup: {
                from: 'firm',
                let: {
                    firmIdCk: '$firmId',
                    fiscalYearOfBKC: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $or: publishedEntityIds.map((entity) => ({
                                $and: [
                                    { $eq: ["$fiscalYear", "$$fiscalYearOfBKC"] },
                                    { $eq: ["$abbreviation", entity.abbreviation] },
                                ]
                            }))
                        }
                    }
                }],
                as: 'firm'
            }
        }, {
            $unwind: '$firm'
        }, {
            $project: {
                _id: 0
            }
        }, {
            $out: 'bkcAssignmentDetails'
        }]);

        db.firm.aggregate([{
            $match: {
                $expr: {
                    $or: publishedEntityIds.map((entity) => ({
                        $and: [
                            { $eq: ["$fiscalYear", entity.fiscalYear] },
                            { $eq: ["$abbreviation", entity.abbreviation] },
                        ]
                    }))
                }
            }
        }, {
            $lookup: {
                from: 'functionowner',
                let: {
                    firmId: '$abbreviation',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [{
                                $eq: ['$$firmId', '$firmId']
                            }, {
                                $eq: ['$notApplicable', true]
                            },
                            {
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                            }]
                        }
                    }
                }],
                as: 'notApplicableFunctions'
            }
        }, {
            $lookup: {
                from: 'enumeration',
                let: {
                    languageCodeCk: '$languageCode',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$languageCode", "$$languageCodeCk"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", { $cond: { if: { $isArray: "$fiscalYear" }, then: "$fiscalYear", else: [] } }]
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
                    "$or": [
                        { "isCreatedInCurrentFY": true },
                        { "isRollForwardedFromPreFY": true }
                    ]
                }
            }
        }, {
            $addFields: {
                parentFirms: {
                    $filter: {
                        input: "$parentFirms",
                        as: 'parentFirm',
                        cond: {
                            $eq: ['$$parentFirm.fiscalYear', '$fiscalYear']
                        }
                    }
                }
            }
        }, {
            $lookup: {
                from: 'action',
                let: {
                    firmIds: {
                        $concatArrays: [
                            ["$abbreviation"], "$parentFirms.abbreviation"
                        ]
                    },
                    fiscalYearOfFirm: '$fiscalYear',
                    abbreviation: '$abbreviation'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ['$fiscalYear', '$$fiscalYearOfFirm']
                                },
                                {
                                    $cond: {
                                        if: { $and: [{ $eq: ['$objectType', 'RequirementControl'] }, { $eq: ["$fiscalYear", "$$fiscalYearOfFirm"] }] },
                                        then: {
                                            $eq: ['$firmId', '$$abbreviation']
                                        },
                                        else: {
                                            $in: ['$firmId', '$$firmIds']
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }, {
                    $project: {
                        objectId: 1
                    }
                }],
                as: 'actions'
            }
        }, {
            $lookup: {
                from: 'bkcAssignmentDetails',
                let: {
                    abbreviation: '$abbreviation',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$$abbreviation', '$assignments.executingEntityId']
                        }
                    }
                }, {
                    $unwind: '$assignments'
                }, {
                    $group: {
                        _id: {
                            executingEntityId: '$assignments.executingEntityId',
                            bkcId: '$assignments.bkcId',
                        },
                        firmId: { $push: '$firmId' }

                    }
                }, {
                    $project: {
                        assignments: '$_id',
                        firmId: 1,
                        _id: 0
                    }
                }],
                as: 'bkcassignments'
            }
        }, {
            $addFields: {
                assignedBkcIds: {
                    $filter: {
                        input: '$bkcassignments',
                        as: 'b',
                        cond: { $eq: ['$$b.assignments.executingEntityId', '$abbreviation'] }
                    }
                }
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    actions: '$actions',
                    assignedBkcIds: '$assignedBkcIds',
                    abbreviation: '$abbreviation',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                },
                                {
                                    $eq: ['$type', 'RequirementControl']
                                },
                                {
                                    $eq: ['$uniqueId', 'RC-NTW-452']
                                }
                            ]
                        }
                    }
                }, {
                    $match: {
                        $expr: {
                            $and: [{
                                $in: ['$uniqueId', '$$assignedBkcIds.assignments.bkcId']
                            }, {
                                $ne: ['$status', 'StatusType_Draft']
                            }, {
                                $not: {
                                    $in: [{ $toString: '$_id' }, '$$actions.objectId']
                                }
                            }
                            ]
                        }
                    }
                }],
                as: 'requirementcontrol'
            }
        }, {
            $addFields: {
                debug_requirementcontrol_all: '$requirementcontrol'
            }
        }, {
            $unwind: {
                path: '$requirementcontrol',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    reqId: { $toString: '$requirementcontrol._id' },
                    firmId: '$abbreviation',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $eq: ['$type', 'RequirementControlAssignment']
                        }
                    }
                }, {
                    $match: {
                        $expr: {
                            $and: [{
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                            }, {
                                $eq: ['$$firmId', '$firmId']
                            }, {
                                $eq: ['$$reqId', '$requirementControlId']
                            }]
                        }
                    }
                }],
                as: 'assignment'
            }
        }, {
            $unwind: {
                path: '$assignment',
                preserveNullAndEmptyArrays: true
            }
/*        }, {
            $match: {
                'assignment': { $exists: true },
                'assignment.status': { $ne: 'StatusType_Draft' }
            }
*/
        }, {
            $addFields: {
                PerformedOnBehalfOfFirmsIds: {
                    $reduce: {
                        input: '$assignedBkcIds',
                        initialValue: '',
                        in: {
                            $cond: {
                                if: {
                                    $and: [{ $eq: ['$$this.assignments.executingEntityId', '$abbreviation'] }, { $eq: ['$$this.assignments.bkcId', '$requirementcontrol.uniqueId'] }]
                                },
                                then: {
                                    $cond: {
                                        if: { $eq: ['$$value', ''] },
                                        then: '$$this.firmId',
                                        else: { $concat: ['$$value', '; ', '$$this.firmId'] }
                                    }
                                },
                                else: '$$value'
                            }
                        }
                    }
                }
            }
        }, {
            $lookup: {
                from: 'firm',
                let: {
                    firmIds: '$PerformedOnBehalfOfFirmsIds',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $in: ['$abbreviation', '$$firmIds'] },
                                { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] }
                            ]
                        }
                    }
                }],
                as: "firmNames"
            }
        }, {
            $addFields: {
                PerformedOnBehalfOf: {
                    $reduce: {
                        input: '$firmNames',
                        initialValue: '',
                        in: {
                            $cond: {
                                if: { $eq: ['$$value', ''] },
                                then: '$$this.name',
                                else: { $concat: ['$$value', '; ', '$$this.name'] }
                            }
                        }
                    }
                }
            }
        }, {
            $lookup: {
                from: 'processdetails',
                let: {
                    uniqueId: '$requirementcontrol.uniqueId',
                    firmId: {
                        $cond: {
                            if: '$assignment',
                            then: '$abbreviation',
                            else: '$requirementcontrol.firmId'
                        }
                    },
                    fiscalYearOfFirm: '$fiscalYear',
                    assignedFirmId: '$abbreviation'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $anyElementTrue: {
                                $map: {
                                    input: "$firmProcesses",
                                    as: 'fp',
                                    in: { $and: [{ $in: ['$$uniqueId', '$$fp.mappedKeyControls'] }, { $eq: ['$$fp.firmId', '$$firmId'] }, { $eq: ['$$fp.fiscalYear', '$$fiscalYearOfFirm'] }] }
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
                                                        in: { $concatArrays: ['$$value', { $cond: { if: { $and: [{ $isArray: '$$this.processOwners' }, { $eq: ['$$assignedFirmId', '$$this.firmId'] }] }, then: '$$this.processOwners', else: [] } }] }
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
                                    fiscalYearOfFirm: '$fiscalYear',
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
                                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
            $lookup: {
                from: 'firm',
                let: {
                    reqCtrlFirmId: '$requirementcontrol.firmId',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$abbreviation", "$$reqCtrlFirmId"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                },
                                {
                                    $or: [{ $eq: ["$isRollForwardedFromPreFY", true] }, { $eq: ["$isCreatedInCurrentFY", true] }]
                                }
                            ]
                        }
                    }
                }],
                as: 'designingEntity'
            }
        }, {
            $unwind: '$designingEntity'
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
                'requirementcontrol.controlFunction': { $cond: { if: '$assignment.controlFunction', then: '$assignment.controlFunction', else: '$requirementcontrol.controlFunction' } },
                'requirementcontrol.localControlFunction': { $cond: { if: '$assignment.localControlFunction', then: '$assignment.localControlFunction', else: '$requirementcontrol.localControlFunction' } },
                'requirementcontrol.executionControlFunction': { $cond: { if: '$assignment.executionControlFunction', then: '$assignment.executionControlFunction', else: '$requirementcontrol.executionControlFunction' } },
                'requirementcontrol.relatedQualityRisks': { $cond: { if: '$assignment.relatedQualityRisks', then: '$assignment.relatedQualityRisks', else: '$requirementcontrol.relatedQualityRisks' } },
                'requirementcontrol.relatedSubRisks': { $cond: { if: '$assignment.relatedSubRisks', then: '$assignment.relatedSubRisks', else: '$requirementcontrol.relatedSubRisks' } },
                'requirementcontrol.localLanguage': { $cond: { if: '$assignment.LocalLanguage', then: '$assignment.LocalLanguage', else: '$requirementcontrol.LocalLanguage' } },
                'requirementcontrol.additionalQOs': { $cond: { if: '$assignment.additionalQOs', then: '$assignment.additionalQOs', else: '' } },
                'requirementcontrol.additionalQRs': { $cond: { if: '$assignment.additionalQRs', then: '$assignment.additionalQRs', else: '' } },
                'requirementcontrol.iecRationale': { $cond: { if: '$assignment.iecRationale', then: '$assignment.iecRationale', else: '' } },
                'requirementcontrol.iecRationaleAlt': { $cond: { if: '$assignment.iecRationaleAlt', then: '$assignment.iecRationaleAlt', else: '' } },
                'requirementcontrol.controlsByPeriod': { $cond: { if: { $eq: ['$assignment.prevId', ''] }, then: { $concat :[ "Control added in FY", { $substr:['$fiscalYear',2,2] } ] }, else: "Rolled forward Control" } },
                'requirementcontrol.PCAOB_Configured_Object': { $cond: { if: { $eq: ['$assignment.isPCAOBRegistered', true] }, then: 'Yes', else: 'No' } },
                'requirementcontrol.isQoOverrideEnabled': { $cond: { if: '$assignment.isQoOverrideEnabled', then: '$assignment.isQoOverrideEnabled', else: '$requirementcontrol.isQoOverrideEnabled' } },
                'requirementcontrol.relatedObjectives': { $cond: { if: '$assignment.relatedObjectives', then: '$assignment.relatedObjectives', else: '$requirementcontrol.relatedObjectives' } }
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    firmId: '$abbreviation',
                    controlFunction: "$requirementcontrol.controlFunction",
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                },
                                {
                                    $eq: ['$type', 'Function']
                                }
                            ]
                        }
                    }
                }, {
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
                                        $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                            foTitle: '$functionowner.title',
                            fiscalYearOfFirm: '$fiscalYear',
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [{
                                        $eq: ['$$foTitle', '$_id']
                                    },
                                    {
                                        $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                    }
                                    ]
                                }
                            }
                        }, {
                            $lookup: {
                                from: 'titleassignment',
                                let: {
                                    fiscalYearOfFirm: '$fiscalYear',
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
                                            }, {
                                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    reqCtrlRelatedSubRisk: '$requirementcontrol.relatedSubRisks',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", "$$reqCtrlRelatedSubRisk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
/*        }, {
            $match: {
                $expr: {
                    $and: [{
                        $not: {
                            $allElementsTrue: {
                                $map: {
                                    input: '$requirementcontrol.relatedQualityRisks',
                                    as: 'qr',
                                    in: { $in: ['$$qr', '$actions.objectId'] }
                                }
                            }
                        }
                    }, {
                        $cond: {
                            if: {
                                $ne: ['$requirementcontrol.isControlOverResource', true]
                            },
                            then: {
                                $not: {
                                    $and: [
                                        { $and: [{ $isArray: '$requirementcontrol.supportingITApplication' }, { $gt: [{ $size: '$requirementcontrol.supportingITApplication' }, 0] }] },
                                        { $eq: [{ $size: { $setDifference: ['$requirementcontrol.supportingITApplication', '$actions.objectId'] } }, 0] }
                                    ]
                                }
                            },
                            else: {
                                $not: {
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
*/
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    reqCtrlRelatedQtrRisk: '$requirementcontrol.relatedQualityRisks',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", "$$reqCtrlRelatedQtrRisk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                }
                            ]
                        }
                    }
                }],
                as: 'requirementcontrol.relatedQualityRisks'
            }
        }, {
            $lookup: {
                from: 'keycontrolresource',
                let: {
                    reqCtrlType: '$requirementcontrol.controlType',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$_id", "$$reqCtrlType"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
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
                let: {
                    reqCtrlTypeNature: '$requirementcontrol.controlNature',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$_id", "$$reqCtrlTypeNature"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
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
                let: {
                    reqCtrlfreqType: '$requirementcontrol.frequencyType',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$reqCtrlfreqType"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
                                }
                            ]
                        }
                    }
                }],
                as: 'requirementcontrol.frequencyType'
            }
        }, {
            $lookup: {
                from: 'keycontrolresource',
                let: {
                    reqCtrlServiceLine: '$requirementcontrol.serviceLine',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$reqCtrlServiceLine"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
                                }
                            ]
                        }
                    }
                }],
                as: 'requirementcontrol.serviceLine'
            }
        }, {
            $lookup: {
                from: 'keycontrolresource',
                let: {
                    reqCtrlAddIpeCategory: '$requirementcontrol.addInformationExecutionControls.ipeCategory',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$reqCtrlAddIpeCategory"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
                                }
                            ]
                        }
                    }
                }],
                as: 'requirementcontrol.ipeCategory'
            }
        },{
            $lookup: {
                from: 'documentation',
                let: {
                    reqCtrlRelSubRiskRelObj: '$requirementcontrol.relatedObjectives',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", { $ifNull: ['$$reqCtrlRelSubRiskRelObj', []] }],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                }
                            ]
                        }
                    }
                }],
                as: 'requirementcontrol.relatedObjectives'
            }
        },
        {
            $lookup: {
                from: 'documentation',
                let: {
                    fiscalYearOfFirm: '$fiscalYear',
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
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$fiscalYear", "$$fiscalYearOfFirm"] },
                                    { $eq: ['$type', 'Resource'] }
                                ]
                            }
                        }
                    },
                    {
                        $match: {
                            $expr: {
                                $in: [{ $toString: '$_id' }, '$$systemIds']
                            }
                        }
                    },
                    {
                        $project: {
                            _id: 0,
                            id: { $toString: '$_id' },
                            name: 1
                        }
                    }
                ],
                as: 'ipeSystemsWithId'
            }
        },
        {
            $lookup: {
                from: 'documentation',
                let: {
                    fiscalYearOfFirm:'$fiscalYear',
                    mitigatedResourceIds: {
                        $cond: {
                            if: { $isArray: '$requirementcontrol.mitigatedResources' },
                            then: '$requirementcontrol.mitigatedResources',
                            else: []
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$fiscalYear", "$$fiscalYearOfFirm"] },
                                { $eq: ['$type', 'Resource'] }
                            ]
                        }
                    }
                }
                    , {
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
                    fiscalYearOfFirm: '$fiscalYear',
                    supportingITApplicationIds: {
                        $cond: {
                            if: { $isArray: '$requirementcontrol.supportingITApplication' },
                            then: '$requirementcontrol.supportingITApplication',
                            else: []
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$fiscalYear", "$$fiscalYearOfFirm"] },
                                { $eq: ['$type', 'Resource'] }
                            ]
                        }
                    }
                }, {
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
                    titleIds: '$requirementcontrol.responseOwners',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                    fiscalYearOfFirm: '$fiscalYear',
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
                            }, {
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                    titleIds: '$requirementcontrol.controlOperator',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                let: {
                    reqCtrlTypeEngagements: '$requirementcontrol.typeOfEngagements',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$reqCtrlTypeEngagements"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
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
                    tagIds: '$requirementcontrol.tags',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                },
                                {
                                    $in: [{ $toString: '$_id' }, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
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
                    fiscalYearOfFirm: '$fiscalYear',
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
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                            }
                            ]
                        }
                    }
                }],
                as: 'requirementcontrol.controlOperatorTitleAssignments'
            }
        }, {
            $addFields: {
                'objectives':
                {
                    $cond: {
                        if: { $in: ['$abbreviation', autoQoNotReqFirms] },
                        then: {
                            $cond: {
                                if: {
                                    $not:
                                    { $or: [{ $eq: ['$requirementcontrol.isQoOverrideEnabled', undefined] }, { $eq: ['$requirementcontrol.isQoOverrideEnabled', ''] }, { $eq: ['$requirementcontrol.isQoOverrideEnabled', null] }] }
                                },
                                then: '$requirementcontrol.relatedObjectives',
                                else: {
                                    $reduce: {
                                        input: '$requirementcontrol.relatedQualityRisks.relatedObjectives',
                                        initialValue: [],
                                        in: { $concatArrays: ['$$value', '$$this'] }
                                    }
                                }
                            }
                        },
                        else: []
                    }
                }
            }
        },
        {
            $set: {
                "objectives": {
                    $cond: {
                        if: { $in: ['$abbreviation', autoQoNotReqFirms] },
                        then: {
                            $filter: {
                                input: '$objectives',
                                as: 'qo',
                                cond: {
                                    $not: {
                                        $in: ['$$qo', reBacPolicyQOIds]
                                    }
                                }
                            }
                        },
                        else: '$objectives'
                    }
                }
            }
        },
        {
            $set: {
                'requirementcontrol.relatedQualityRisks': {
                    $cond: {
                        if: { $in: ['$abbreviation', autoQoNotReqFirms] },
                        then: {
                            $filter: {
                                input: {
                                    $map: {
                                        input: { $ifNull: ['$requirementcontrol.relatedQualityRisks', []] },
                                        as: 'qr',
                                        in: {
                                            $mergeObjects: [
                                                '$$qr',
                                                {
                                                    relatedObjectives: {
                                                        $filter: {
                                                            input: { $ifNull: ['$$qr.relatedObjectives', []] },
                                                            as: 'objId',
                                                            cond: {
                                                                $not: {
                                                                    $in: ['$$objId', reBacPolicyQOIds]
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                },
                                as: 'filteredQr',
                                cond: {
                                    $gt: [{ $size: '$$filteredQr.relatedObjectives' }, 0]
                                }
                            }
                        },
                        else: '$requirementcontrol.relatedQualityRisks'
                    }
                }
            }
        },
        {
            $lookup: {
                from: 'documentation',
                let: {
                    fiscalYearOfFirm: '$fiscalYear',
                    relatedObjectives: '$objectives',

                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", '$$fiscalYearOfFirm']
                                },
                                {
                                    $in: ['$uniqueId', '$$relatedObjectives']
                                }
                            ]
                        }
                    }
                },
                { $project: { _id: 0, qualityObjectiveId: 1 } }],
                as: 'associatedQualityObjectives'
            }
        }, {
            $project: {
                _id: 0,
                requirementcontrol:'$requirementcontrol',
                UniqueControlID: { $concat: ['$abbreviation', '-', '$requirementcontrol.uniqueId'] },
                EntityId: '$abbreviation',
                FirmPublishedDate: "$publishedDate",
                QualityObjectiveUniquesIds: {
                    $reduce: {
                        input: '$associatedQualityObjectives',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.qualityObjectiveId'] }
                    }
                },
                ActiveQualityRiskUniqueIds: {
                    $reduce: {
                        input: '$requirementcontrol.relatedQualityRisks.uniqueId',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
                    }
                },
                ActiveQualitySubRiskUniqueIds: {
                    $reduce: {
                        input: '$requirementcontrol.relatedSubRisks.uniqueId',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
                    }
                },
                qualityRiskUniqueIdArray: '$requirementcontrol.relatedQualityRisks.uniqueId',
                qualityObjectiveUniqueIdArray: { $ifNull: ['$objectives', []] },
                qualitySubRiskUniqueIdArray: '$requirementcontrol.relatedSubRisks.uniqueId',
                EntityFiscalYear: '$fiscalYear',
                FiscalYear: {$concat:['FY',{$substr:['$fiscalYear',2,2]}]},                
                IsQoOverrideEnabled: '$requirementcontrol.isQoOverrideEnabled',
                DEBUG_REQ_STATUS: { $cond: { if: '$requirementcontrol', then: 'FOUND', else: 'NOT_FOUND' } },
                DEBUG_REQ_COUNT: { $size: { $ifNull: ['$debug_requirementcontrol_all', []] } },
                DEBUG_ASSIGN_STATUS: { $cond: { if: '$assignment', then: 'FOUND', else: 'NOT_FOUND' } }
            }
        }, {
                $out: 'sqmarcherrequirementcontroltemp'
            }
        ])
        
    }
} catch (error) {
    print("SYSTEM:Archer Error :: Error at Documentation  Query ", error);
    throw (error);
}
