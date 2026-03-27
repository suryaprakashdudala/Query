try {
    // db = db.getSiblingDB('isqc');

    var fiscalYearFilter = "2026";

    fiscalYearFilter = fiscalYearFilter.split(",").map(function (year) {
        return parseInt(year, 10);
    });

    var autoQoNotReqFirms = 'USA';
    var globalQOApplicabilityPolicyId = 'EXC-GLOBAL-LOC-GLOBAL-USA';

    var publishedEntityIds = db.firm.aggregate([{
        $match: {
            $expr: {
                $and: [
                    '$publishedDate',
                    { $ne: ['$publishedDate', ''] },
                    { $and: [{ $eq: ['$isPublishQueryRun', false] }, { $or: [{ $eq: ['$isAutoPublished', true] }, { $eq: ['$isReadOnly', false] }] }] },
                    { $in: ["$fiscalYear", fiscalYearFilter] },
                    { $or: [{ $eq: ["$isRollForwardedFromPreFY", true] }, { $eq: ["$isCreatedInCurrentFY", true] }] }                ]
            }
        }
    },
    {
        $project: {
            abbreviation: 1,
            fiscalYear:1,
            _id: 0
        }
    }]).toArray(); // [ { abbreviation: 'AME', fiscalYear: 2025 },{ abbreviation: 'EME', fiscalYear: 2026 } ]
    if (!publishedEntityIds || publishedEntityIds.length === 0) {
        print("No published entities found.");
    } else {
        var orConditions = publishedEntityIds.map(entity => ({
            $and: [
                { $eq: ["$firm.abbreviation", entity.abbreviation] },
                { $eq: ["$firm.fiscalYear", entity.fiscalYear] }
            ]
        }));

        var iecIpeCategoryResources = db.keycontrolresource.find({ "type": "IpeCategory", fiscalYear: { $in: fiscalYearFilter } }, { _id: 1, name: 1 }).toArray();
        var iecReportNameResources = db.keycontrolresource.find({ "type": "IecReport", fiscalYear: { $in: fiscalYearFilter } }, { _id: 1, name: 1 }).toArray();

        //Drop temp collections
        db.sqmarcherkeycontroltemp.drop();
        db.sqmarcherqualityrisktemp.drop();
        db.sqmarcherqualityobjectivetemp.drop();
        db.sqmarcherresourcestemp.drop();
        db.sqmarcherrequirementcontroltemp.drop();
        db.sqmarcherfunctiontemp.drop();
        db.sqmarcherqualitysubrisktemp.drop();

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
                //localField:'firmId',
                //foreignField:'abbreviation',
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
                            /*$and: [
                                '$publishedDate',
                                { $ne: ['$publishedDate', ''] },
                                { $eq: ['$isPublishQueryRun', false] },
                                { $eq: ["$fiscalYear", fiscalYearFilter]},
                                { $or: [{ $eq: ["$isRollForwardedFromPreFY" , true]},{ $eq: ["$isCreatedInCurrentFY" , true]}]}
                            ]*/
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

        //Requirement control
        db.firm.aggregate([{
            $match: {
                $expr: {
                    $or: publishedEntityIds.map((entity) => ({
                        $and: [
                            { $eq: ["$fiscalYear", entity.fiscalYear] },
                            { $eq: ["$abbreviation", entity.abbreviation] },
                        ]
                    }))
                    /*$and: [
                        '$publishedDate',
                        { $ne: ['$publishedDate', ''] },
                        { $eq: ['$isPublishQueryRun', false] },
                        { $eq: ["$fiscalYear", fiscalYearFilter]},
                        { $or: [{ $eq: ["$isRollForwardedFromPreFY" , true]},{ $eq: ["$isCreatedInCurrentFY" , true]}]}
                    ]*/
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
                //localField: 'languageCode',
                //foreignField: 'languageCode',
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
                                        //if:{$eq:['$objectType','RequirementControl']},
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
                        // $expr:{
                        // $eq:['$type','RequirementControl']
                        // }
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                },
                                {
                                    $eq: ['$type', 'RequirementControl']
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
            $unwind: '$requirementcontrol'
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
            //$unwind: '$assignment'
            $unwind: {
                path: '$assignment',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $match: {
                'assignment': { $exists: true },
                'assignment.status': { $ne: 'StatusType_Draft' }
            }
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
                //localField:'requirementcontrol.firmId',
                //foreignField:'abbreviation',
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
                'requirementcontrol.controlsByPeriod': { $cond: { if: { $eq: ['$assignment.prevId', ''] }, then: { $concat :[ "Control added in FY", { $substr:['$fiscalYear',2,2] } ] }, else: "Rolled forward Control" } }, //Additional Column controlsByPeriod
                'requirementcontrol.PCAOB_Configured_Object': { $cond: { if: { $eq: ['$assignment.isPCAOBRegistered', true] }, then: 'Yes', else: 'No' } }, //Added New column PCAOB_Configured_Object requirementcontrol
                'requirementcontrol.isQoOverrideEnabled': { $cond: { if: '$assignment.isQoOverrideEnabled', then: '$assignment.isQoOverrideEnabled', else: '$requirementcontrol.isQoOverrideEnabled' } }
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
                            //$eq:['$type','Function']
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
                                    //$eq: ['$$foTitle', '$_id']
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
                //localField: 'requirementcontrol.relatedSubRisks',
                //foreignField: 'uniqueId',
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
        }, {
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
        }, {
            $lookup: {
                from: 'documentation',
                //localField: 'requirementcontrol.relatedQualityRisks',
                //foreignField: 'uniqueId',
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
                //localField: 'requirementcontrol.controlType',
                //foreignField: '_id',
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
                //localField: 'requirementcontrol.controlNature',
                //foreignField: '_id',
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
                //localField: 'requirementcontrol.frequencyType',
                //foreignField: '_id',
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
                //localField: 'requirementcontrol.serviceLine',
                //foreignField: '_id',
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
                //localField: 'requirementcontrol.addInformationExecutionControls.ipeCategory',
                //foreignField: '_id',
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
        },/*{
            $lookup: {
                from: 'keycontrolresource',
                let:{
                    reqCtrlAddIecReports:'$requirementcontrol.addInformationExecutionControls.iecReports'
                },
                pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [
                                        {
                                            $in: [ "$_id",  "$$reqCtrlAddIecReports" ] ,
                                        },
                                        {
                                            $in: [fiscalYearFilter, "$fiscalYear"]
                                        }
                                    ]
                                }
                            }
                        }],
                as: 'requirementcontrol.iecReportsName'
            }
        },*/  {
            $lookup: {
                from: 'documentation',
                //localField: 'requirementcontrol.relatedQualityRisks.relatedObjectives',
                //foreignField: 'uniqueId',
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
                        //type:'Resource'
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
                        // $expr: {
                        // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                        // }
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
                        // $expr: {
                        // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                        // }
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
                        // $expr: {
                        // $in: [{$toString: '$_id'}, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
                        // }
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
                        if: { $eq: ['$abbreviation', 'USA'] },
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
            $lookup: {
                from: 'rebacpolicy',
                let: {
                    firmId: '$abbreviation',
                    objectives: '$objectives'
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$policyId', globalQOApplicabilityPolicyId] },
                                    { $eq: ['$$firmId', autoQoNotReqFirms] },
                                    { $eq: ['$toEntity', '$$firmId'] },
                                    {
                                        $in: ['$objectId', '$$objectives']
                                    }
                                ]
                            }
                        }
                    }
                ],
                as: 'rebacPoliciesRelatedToQOs'
            }
        },
        {
            $set: {
                "objectives": {
                    $cond: {
                        if: { $eq: ['$abbreviation', autoQoNotReqFirms] },
                        then: {
                            $filter: {
                                input: '$objectives',
                                as: 'qo',
                                cond: {
                                    $not: {
                                        $in: ['$$qo', '$rebacPoliciesRelatedToQOs.objectId']
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
                        if: { $eq: ['$abbreviation', autoQoNotReqFirms] },
                        then: {
                            $filter: {
                                input: { $ifNull: ['$requirementcontrol.relatedQualityRisks', []] },
                                as: 'qr',
                                cond: {
                                    $let:{
                                        vars: {
                                            objectives: { $ifNull: ['$$qr.relatedObjectives', []] }
                                        },
                                        in: {
                                            $anyElementTrue: {
                                                $map: {
                                                    input: '$$objectives',
                                                    as: 'objId',
                                                    in: {
                                                        $not: {
                                                            $in: ['$$objId', '$rebacPoliciesRelatedToQOs.objectId']
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
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
                UniqueControlID: { $concat: ['$abbreviation', '-', '$requirementcontrol.uniqueId'] },
                EntityId: '$abbreviation',
                FirmPublishedDate: "$publishedDate",
                LastProcessedDate: new Date().toISOString(),
                NetworkId: {
                    $cond: { if: { $eq: ['$type', "EntityType_Network"] }, then: '$abbreviation', else: '' }
                },
                AreaId: {
                    $cond: { if: { $eq: ['$type', "EntityType_Area"] }, then: '$abbreviation', else: '' }
                },
                RegionId: {
                    $cond: { if: { $eq: ['$type', "EntityType_Region"] }, then: '$abbreviation', else: '' }
                },
                BusinessUnitId: {
                    $cond: { if: { $eq: ['$type', "EntityType_BusinessUnit"] }, then: '$abbreviation', else: '' }
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
                            { case: { $eq: ['$type', "EntityType_BusinessUnit"] }, then: "Business Unit" },
                            { case: { $eq: ['$type', "EntityType_SubCluster"] }, then: "Sub-Cluster" }
                        ],
                        default: ''
                    }
                },
                ControlName: '$requirementcontrol.controlName',
                ControlNameLocal: '$requirementcontrol.controlNameAlt',
                ControlDescription: '$requirementcontrol.description',
                ControlDescriptionLocal: '$requirementcontrol.descriptionAlt',
                Ref_UniqueId: '$requirementcontrol.uniqueId',
                RelatedPolicyProcedureManualGuidance: { $concat: [' ', '$requirementcontrol.relatedPolicy'] },
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
                ControlOwnerTitlesIDs: {
                    $reduce: {
                        input: '$requirementcontrol.responseOwnerTitles',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, { $toString: '$$this._id' }] }
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
                ControlOperatorTitleIDs: {
                    $reduce: {
                        input: '$requirementcontrol.controlOperatorTitles',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, { $toString: '$$this._id' }] }
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
                            $cond: {
                                if: {
                                    $anyElementTrue: {
                                        $map: {
                                            input: '$requirementcontrol.executionControlFunction',
                                            as: 'lf',
                                            in: { $in: ['$$lf', '$notApplicableFunctions.functionId'] }
                                        }
                                    }
                                },
                                then: [],
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
                'ProcessId': '$requirementcontrol.processes._id',
                'ProcessUniqueId': {
                  $reduce: {
                    input: {
                      $cond: {
                        if: { $isArray: '$requirementcontrol.processes' },
                        then: '$requirementcontrol.processes.uniqueId',
                        else: []
                      }
                    },
                    initialValue: '',
                    in: {
                      $concat: [
                        '$$value',
                        { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } },
                        { $toString: '$$this' }
                      ]
                    }
                  }
                },
                AdditionalQOs: '$requirementcontrol.additionalQOs',
                AdditionalQRs: '$requirementcontrol.additionalQRs',
                ControlObjective: { $concat: [' ', '$requirementcontrol.keyControlObjective'] },
                ControlObjectiveLocal: '$requirementcontrol.keyControlObjectiveAlt',
                ControlCriteria: { $concat: [' ', '$requirementcontrol.controlCriteria'] },
                ControlCriteriaLocal: '$requirementcontrol.controlCriteriaAlt',
                LocalLanguage: {
                    $reduce: {
                        input: '$localLanguage.name',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
                    }
                },
                SupportingEvidence: {
                    $concat: ['', {
                        $reduce: {
                            input: '$requirementcontrol.addControlEvidenceControls',
                            initialValue: '',
                            in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                        }
                    }]
                },
                SupportingEvidenceLocal: {
                    $reduce: {
                        input: '$requirementcontrol.addControlEvidenceControls.nameAlt',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
                    }
                },
                IEC: {
                    $concat: ['', {
                        $reduce: {
                            input: '$requirementcontrol.addInformationExecutionControls',
                            initialValue: '',
                            in: {
                                $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, {
                                    $cond: {
                                        if: { $eq: ['$requirementcontrol.controlType._id', 'ControlType_ItDependentManual'] }, then: {
                                            $concat: ['IEC-ID-', "$$this.uniqueId", ':']
                                        }, else: ''
                                    }
                                }, '$$this.name']
                            }

                        }
                    }]
                },
                IECLocal: {
                    $reduce: {
                        input: '$requirementcontrol.addInformationExecutionControls.nameAlt',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, 'IEC-ID-', "$$this.uniqueId", ':', '$$this'] }

                    }
                },
                IECReportType: {
                    $reduce: {
                        input: {
                            $map: {
                                input: { $cond: { if: { $isArray: "$requirementcontrol.addInformationExecutionControls" }, then: "$requirementcontrol.addInformationExecutionControls", else: [] } },
                                as: "iecRep",
                                in: {
                                    $reduce: {
                                        input: iecIpeCategoryResources,
                                        initialValue: "",
                                        in: {
                                            $cond: {
                                                if: {
                                                    $eq: ["$$this._id", "$$iecRep.ipeCategory"]
                                                },
                                                then: {
                                                    $concat: [
                                                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: { $concat: ["$$value", ";"] } } },
                                                        'IEC-ID-', "$$iecRep.uniqueId", ':',
                                                        "$$this.name"
                                                    ]
                                                },
                                                else: "$$value"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        initialValue: "",
                        in: {
                            $cond: {
                                if: { $eq: ["$$value", ""] },
                                then: "$$this",
                                else: { $concat: ["$$value", ";", "$$this"] }
                            }
                        }
                    }
                },
                IsControlOverResource: '$requirementcontrol.isControlOverResource',
                KeyControlOperatedByServiceProvider: '$requirementcontrol.isControlOperatedByServiceProvider',
                ResourceType: {
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
                ResourceName: {
                    $reduce: {
                        input: '$requirementcontrol.mitigatedResourcesObjs',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

                    }
                },
                SupportingITApplication: {
                    $reduce: {
                        input: '$requirementcontrol.supportingITApplicationIdsObjs',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

                    }
                },
                ServiceLine: {
                    $cond: {
                        if: { $ne: ['$requirementcontrol.isControlOverResource', true] },
                        then: {
                            $reduce: {
                                input: '$requirementcontrol.serviceLine',
                                initialValue: '',
                                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                            }
                        },
                        else: ''
                    }
                },
                EngagementType: {
                    $reduce: {
                        input: '$requirementcontrol.typeOfEngagements',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
                    }
                },
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
                Draft: { $eq: ['$requirementcontrol.status', 'StatusType_Draft'] },
                IECSystems: {
                    $cond: {
                        if: { $eq: ['$requirementcontrol.controlType._id', 'ControlType_ItDependentManual'] }, then: {
                            $reduce: {
                                input: {
                                    $map: {
                                        input: "$requirementcontrol.addInformationExecutionControls",
                                        as: "control",
                                        in: {
                                            $let: {
                                                vars: {
                                                    reducedValue: {
                                                        $reduce: {
                                                            input: {
                                                                $map: {
                                                                    input: "$$control.ipeSystems",
                                                                    as: "ipe",
                                                                    in: {
                                                                        $let: {
                                                                            vars: {
                                                                                matched: {
                                                                                    $filter: {
                                                                                        input: "$ipeSystemsWithId",
                                                                                        as: "sys",
                                                                                        cond: { $eq: ["$$sys.id", "$$ipe"] }
                                                                                    }
                                                                                }
                                                                            },
                                                                            in: {
                                                                                $cond: {
                                                                                    if: { $gt: [{ $size: "$$matched" }, 0] },
                                                                                    then: { $concat: ["IEC-ID-", "$$control.uniqueId", ":", { $arrayElemAt: ["$$matched.name", 0] }] },
                                                                                    else: { $concat: ["IEC-ID-", "$$control.uniqueId", ":N/A"] }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            initialValue: "",
                                                            in: {
                                                                $concat: [
                                                                    "$$value",
                                                                    { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: ";" } },
                                                                    "$$this"
                                                                ]
                                                            }
                                                        }
                                                    }
                                                },
                                                in: {
                                                    $cond: {
                                                        if: { $eq: ["$$reducedValue", ""] },
                                                        then: { $concat: ["IEC-ID-", "$$control.uniqueId", ":N/A"] },
                                                        else: "$$reducedValue"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                initialValue: "",
                                in: {
                                    $concat: [
                                        "$$value",
                                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: ";" } },
                                        "$$this"
                                    ]
                                }
                            }
                        }, else: ''
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
                qualitySubRiskUniqueIdArray: '$requirementcontrol.relatedSubRisks.uniqueId',
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
                FunctionBaselineLocalControlDesign: {
                    $reduce: {
                        input: {
                            $cond: {
                                if: {
                                    $anyElementTrue: {
                                        $map: {
                                            input: '$requirementcontrol.localControlFunction',
                                            as: 'lf',
                                            in: { $in: ['$$lf', '$notApplicableFunctions.functionId'] }
                                        }
                                    }
                                },
                                then: [],
                                else: '$requirementcontrol.localControlFunction'
                            }
                        },
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
                    }
                },
                FunctionBaselineControlExecution: {
                    $reduce: {
                        input: {
                            $cond: {
                                if: {
                                    $anyElementTrue: {
                                        $map: {
                                            input: '$requirementcontrol.executionControlFunction',
                                            as: 'lf',
                                            in: { $in: ['$$lf', '$notApplicableFunctions.functionId'] }
                                        }
                                    }
                                },
                                then: [],
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
                ExecutingEntity: '$name',
                ControlDesignedByType: {
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
                ControlDesignedByName: '$designingEntity.name',
                PerformedOnBehalfOf: '$PerformedOnBehalfOf',
                IsControlOverResourceNew: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$requirementcontrol.isControlOverResource', true] }, then: "Yes" },
                            { case: { $eq: ['$requirementcontrol.isControlOverResource', false] }, then: "No" },
                        ],
                        default: ''
                    }
                },
                KeyControlOperatedByServiceProviderNew: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$requirementcontrol.isControlOperatedByServiceProvider', true] }, then: "Yes" },
                            { case: { $eq: ['$requirementcontrol.isControlOperatedByServiceProvider', false] }, then: "No" },
                        ],
                        default: ''
                    },
                },
                ArcherPublishedOn: new Date().toISOString(),

                IsThisIECReportOrOther: {
                    $reduce: {
                        input: {
                            $map: {
                                input: { $cond: { if: { $isArray: "$requirementcontrol.addInformationExecutionControls" }, then: "$requirementcontrol.addInformationExecutionControls", else: [] } },
                                as: "iecRepName",
                                in: {
                                    $reduce: {
                                        input: iecReportNameResources,
                                        initialValue: "",
                                        in: {
                                            $cond: {
                                                if: {
                                                    $eq: ["$$this._id", "$$iecRepName.iecReports"]
                                                },
                                                then: {
                                                    $concat: [
                                                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: { $concat: ["$$value", ";"] } } },
                                                        'IEC-ID-', "$$iecRepName.uniqueId", ':',
                                                        "$$this.name"
                                                    ]
                                                },
                                                else: "$$value"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        initialValue: "",
                        in: {
                            $cond: {
                                if: { $eq: ["$$value", ""] },
                                then: "$$this",
                                else: { $concat: ["$$value", ";", "$$this"] }
                            }
                        }
                    }
                },
                ControlOwnerProceduresOverCompleteness: {
                    $reduce: {
                        input: '$requirementcontrol.addInformationExecutionControls',
                        initialValue: '',
                        in: {
                            $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, {
                                $cond: {
                                    if: { $eq: ['$requirementcontrol.controlType._id', 'ControlType_ItDependentManual'] }, then: {
                                        $concat: ['IEC-ID-', "$$this.uniqueId", ':']
                                    }, else: ''
                                }
                            }, '$$this.iecOwnerProcedures']
                        }
                    }
                },
                ControlOwnerProceduresOverCompletenessLocal: {
                    $reduce: {
                        input: '$requirementcontrol.addInformationExecutionControls',
                        initialValue: '',
                        in: {
                            $concat: [
                                '$$value',
                                {
                                    $cond: {
                                        if: { $eq: ['$$value', ''] },
                                        then: '',
                                        else: '; '
                                    }
                                },
                                {
                                    $cond: {
                                        if: { $eq: ['$requirementcontrol.controlType._id', 'ControlType_ItDependentManual'] },
                                        then: {
                                            $cond: {
                                                if: { $ne: ['$$this.iecOwnerProceduresAlt', ''] },
                                                then: { $concat: ['IEC-ID-', "$$this.uniqueId", ':'] },
                                                else: ''
                                            }
                                        },
                                        else: ''
                                    }
                                },
                                '$$this.iecOwnerProceduresAlt'
                            ]
                        }
                    }
                },
                IECRationale: '$requirementcontrol.iecRationale',
                IECRationaleLocal: '$requirementcontrol.iecRationaleAlt',
                IECIDs: {
                    $reduce: {
                        input: '$requirementcontrol.addInformationExecutionControls.uniqueId',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, { $cond: { if: { $eq: ['$requirementcontrol.controlType._id', 'ControlType_ItDependentManual'] }, then: '$$this', else: '' } }] }

                    }
                },
                //Additional Column controlsByPeriod
                ControlsByPeriod: '$requirementcontrol.controlsByPeriod',
                PCAOB_Configured_Object: '$requirementcontrol.PCAOB_Configured_Object',
                EntityFiscalYear: '$fiscalYear',
                FiscalYear: {$concat:['FY',{$substr:['$fiscalYear',2,2]}]},                
                IsQoOverrideEnabled: '$requirementcontrol.isQoOverrideEnabled'
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
                from: 'enumeration',
                //localField: 'languageCode',
                //foreignField: 'languageCode',
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
            $lookup: {
                from: 'documentation',
                let: {
                    firmId: '$abbreviation',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $eq: ['$type', 'KeyControl']
                        }
                    }
                }, {
                    $match: {
                        $expr: {
                            $and: [{
                                $eq: ['$$firmId', '$firmId']
                            }, {
                                $ne: ['$status', 'StatusType_Draft']
                            }, {
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                    controlFunction: "$keyControl.controlFunction",
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        // $expr:{
                        // $eq:['$type','Function']
                        // }
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
                            fiscalYearOfFirm: '$fiscalYear'
                        },
                        pipeline: [{
                            $match: {
                                // $expr: {
                                // $eq: ['$$foTitle', '$_id']
                                // }
                                $expr: {
                                    $and: [
                                        {
                                            $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                let: {
                    keyCtrlNature: '$keyControl.controlNature',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$_id", "$$keyCtrlNature"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
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
                let: {
                    keyCtrlFreqType: '$keyControl.frequencyType',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$keyCtrlFreqType"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
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
                let: {
                    keyCtrlServiceLine: '$keyControl.serviceLine',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$keyCtrlServiceLine"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
                                }
                            ]
                        }
                    }
                }],
                as: 'keyControl.serviceLine'
            }
        }, {
            $lookup: {
                from: 'keycontrolresource',
                //localField: 'keyControl.addInformationExecutionControls.ipeCategory',
                //foreignField: '_id',
                let: {
                    keyCtrlAddInfoIpeCategory: '$keyControl.addInformationExecutionControls.ipeCategory',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$keyCtrlAddInfoIpeCategory"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
                                }
                            ]
                        }
                    }
                }],
                as: 'keyControl.ipeCategory'
            }
        },
         {
            $lookup: {
                from: 'processdetails',
                let: {
                    uniqueId: '$keyControl.uniqueId',
                    firmId: '$abbreviation',
                    fiscalYearOfFirm: '$fiscalYear',
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
                            firmProcess: '$firmProcesses',
                            fiscalYearOfFirm: '$fiscalYear'
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [{
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
                                                            in: { $concatArrays: ['$$value', { $cond: { if: { $and: [{ $isArray: '$$this.processOwners' }, { $eq: ['$$firmId', '$$this.firmId'] }] }, then: '$$this.processOwners', else: [] } }] }
                                                        }
                                                    },
                                                    else: []
                                                }
                                            }
                                        ]
                                    }, {
                                        $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                    }]
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
            $lookup: {
                from: 'documentation',
                //localField: 'keyControl.relatedSubRisks',
                //foreignField: 'uniqueId',
                let: {
                    keyCtrlRelSubRisks: '$keyControl.relatedSubRisks',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", "$$keyCtrlRelSubRisks"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                let: {
                    keyCtrlRelQtlRisks: '$keyControl.relatedQualityRisks',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", "$$keyCtrlRelQtlRisks"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                }
                            ]
                        }
                    }
                }],
                as: 'keyControl.relatedQualityRisks'
            }
        }, /*{
            $lookup: {
                from: 'documentation',
                //localField: 'keyControl.relatedQualityRisks.relatedObjectives',
                //foreignField: 'uniqueId',
                let: {
                    keyCtrlRelQtlRisksRelObj: '$keyControl.relatedObjectives',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", { $ifNull: ['$$keyCtrlRelQtlRisksRelObj', []] }],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                }
                            ]
                        }
                    }
                }],
                as: 'keyControl.relatedDocObjectives'
            }
        },*/
        {
            $lookup: {
                from: 'documentation',
                let: {
                    fiscalYearOfFirm: '$fiscalYear',
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
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$type', 'Resource'] },
                                    { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] }
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
                    fiscalYearOfFirm: '$fiscalYear',
                    mitigatedResourceIds: {
                        $cond: {
                            if: { $isArray: '$keyControl.mitigatedResources' },
                            then: '$keyControl.mitigatedResources',
                            else: []
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        //type:'Resource'
                        $expr: {
                            $and: [
                                { $eq: ['$type', 'Resource'] },
                                { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] }
                            ]
                        }
                    }
                }, {
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
                    fiscalYearOfFirm: '$fiscalYear',
                    supportingITApplicationIds: {
                        $cond: {
                            if: { $isArray: '$keyControl.supportingITApplication' },
                            then: '$keyControl.supportingITApplication',
                            else: []
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        //type:'Resource'
                        $expr: {
                            $and: [
                                { $eq: ['$type', 'Resource'] },
                                { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] }
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
                as: 'keyControl.supportingITApplicationObjs'
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    titleIds: '$keyControl.responseOwners',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        // $expr: {
                        // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                        // }
                        $expr: {
                            $and: [{
                                $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                            }, {
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                    fiscalYearOfFirm: '$fiscalYear',
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
                            }, {
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                    titleIds: '$keyControl.controlOperator',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        // $expr: {
                        // $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                        // }
                        $expr: {
                            $and: [{
                                $in: ['$_id', { $cond: { if: { $isArray: '$$titleIds' }, then: '$$titleIds', else: [] } }]
                            }, {
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
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
                let: {
                    fiscalYearOfFirm: '$fiscalYear',
                    keyCtrlTypeEngagements: '$keyControl.typeOfEngagements'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$keyCtrlTypeEngagements"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
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
                    tagIds: '$keyControl.tags',
                    fiscalYearOfFirm: '$fiscalYear',
                },
                pipeline: [{
                    $match: {
                        // $expr: {
                        // $in: [{$toString: '$_id'}, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
                        // }
                        $expr: {
                            $and: [{
                                $in: [{ $toString: '$_id' }, { $cond: { if: { $isArray: '$$tagIds' }, then: '$$tagIds', else: [] } }]
                            }, {
                                $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                            }]
                        }
                    }
                }],
                as: 'keyControl.tags'
            }
        }, {
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
                    fiscalYearOfFirm: '$fiscalYear',
                    firmId: '$keyControl.firmId'
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
                as: 'keyControl.controlOperatorTitleAssignments'
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    fiscalYearOfFirm: '$fiscalYear',
                    relatedQualityRiskObjectives: {
                        $reduce: {
                            input: '$keyControl.relatedQualityRisks.relatedObjectives',
                            initialValue: [],
                            in: { $concatArrays: ['$$value', '$$this'] }
                        }
                    },
                    //isQoOverrideEnabled: '$requirementcontrol.isQoOverrideEnabled',
                    isQoOverrideEnabled: { 
                        $cond: { if: { $and: [{ $ne: ['$keyControl.isQoOverrideEnabled', undefined] },{ $eq: ['$keyControl.isQoOverrideEnabled', true] }]},
                          then: true,
                          else: false}
                    },
                    relatedOverrideObjectives: '$keyControl.relatedObjectives',
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                },
                                {
                                    $in: ["$uniqueId", { $cond: { if: "$$isQoOverrideEnabled", then:'$$relatedOverrideObjectives', else: "$$relatedQualityRiskObjectives"}}]
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
                UniqueControlID: '$keyControl.uniqueId',
                EntityId: '$abbreviation',
                FirmPublishedDate: "$publishedDate",
                LastProcessedDate: new Date().toISOString(),
                NetworkId: {
                    $cond: { if: { $eq: ['$type', "EntityType_Network"] }, then: '$abbreviation', else: '' }
                },
                AreaId: {
                    $cond: { if: { $eq: ['$type', "EntityType_Area"] }, then: '$abbreviation', else: '' }
                },
                RegionId: {
                    $cond: { if: { $eq: ['$type', "EntityType_Region"] }, then: '$abbreviation', else: '' }
                },
                BusinessUnitId: {
                    $cond: { if: { $eq: ['$type', "EntityType_BusinessUnit"] }, then: '$abbreviation', else: '' }
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
                            { case: { $eq: ['$type', "EntityType_BusinessUnit"] }, then: "Business Unit" },
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
                Ref_UniqueId: '$keyControl.uniqueId',
                RelatedPolicyProcedureManualGuidance: { $concat: [' ', '$keyControl.relatedPolicy'] },
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
                EngagementType: {
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
                ControlOwnerTitlesIDs: {
                    $reduce: {
                        input: '$keyControl.responseOwnerTitles',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, { $toString: '$$this._id' }] }
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
                ControlOperatorTitleIDs: {
                    $reduce: {
                        input: '$keyControl.controlOperatorTitles',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, { $toString: '$$this._id' }] }
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
                'FunctionId': {
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
                'ProcessId': '$keyControl.processes._id',
                'ProcessUniqueId': {
                  $reduce: {
                    input: {
                      $cond: {
                        if: { $isArray: '$keyControl.processes' },
                        then: '$keyControl.processes.uniqueId',
                        else: []
                      }
                    },
                    initialValue: '',
                    in: {
                      $concat: [
                        '$$value',
                        { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } },
                        { $toString: '$$this' }
                      ]
                    }
                  }
                },
                ControlObjective: { $concat: [' ', '$keyControl.keyControlObjective'] },
                ControlObjectiveLocal: '$keyControl.keyControlObjectiveAlt',
                ControlCriteria: { $concat: [' ', '$keyControl.controlCriteria'] },
                ControlCriteriaLocal: '$keyControl.controlCriteriaAlt',
                LocalLanguage: {
                    $reduce: {
                        input: '$localLanguage.name',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
                    }
                },
                SupportingEvidence: {
                    $concat: ['', {
                        $reduce: {
                            input: '$keyControl.addControlEvidenceControls',
                            initialValue: '',
                            in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                        }
                    }]
                },
                SupportingEvidenceLocal: {
                    $reduce: {
                        input: '$keyControl.addControlEvidenceControls.nameAlt',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this'] }
                    }
                },
                IEC: {
                    $concat: ['', {
                        $reduce: {
                            input: '$keyControl.addInformationExecutionControls',
                            initialValue: '',
                            in: {
                                $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, {
                                    $cond: {
                                        if: { $eq: ['$keyControl.controlType._id', 'ControlType_ItDependentManual'] }, then: {
                                            $concat: ['IEC-ID-', "$$this.uniqueId", ':']
                                        }, else: ''
                                    }
                                }, '$$this.name']
                            }

                        }
                    }]
                },
                IECLocal: {
                    $reduce: {
                        input: '$keyControl.addInformationExecutionControls.nameAlt',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, 'IEC-ID-', "$$this.uniqueId", ':', '$$this'] }

                    }
                },
                IECReportType: {
                    $reduce: {
                        input: {
                            $map: {
                                input: { $cond: { if: { $isArray: "$keyControl.addInformationExecutionControls" }, then: "$keyControl.addInformationExecutionControls", else: [] } },
                                as: "iecRep",
                                in: {
                                    $reduce: {
                                        input: iecIpeCategoryResources,
                                        initialValue: "",
                                        in: {
                                            $cond: {
                                                if: {
                                                    $eq: ["$$this._id", "$$iecRep.ipeCategory"]
                                                },
                                                then: {
                                                    $concat: [
                                                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: { $concat: ["$$value", ";"] } } },
                                                        'IEC-ID-', "$$iecRep.uniqueId", ':',
                                                        "$$this.name"
                                                    ]
                                                },
                                                else: "$$value"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        initialValue: "",
                        in: {
                            $cond: {
                                if: { $eq: ["$$value", ""] },
                                then: "$$this",
                                else: { $concat: ["$$value", ";", "$$this"] }
                            }
                        }
                    }
                },
                IsControlOverResource: '$keyControl.isControlOverResource',
                KeyControlOperatedByServiceProvider: '$keyControl.isControlOperatedByServiceProvider',
                ResourceType: {
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
                ResourceName: {
                    $reduce: {
                        input: '$keyControl.mitigatedResourceObjs',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

                    }
                },
                SupportingITApplication: {
                    $reduce: {
                        input: '$keyControl.supportingITApplicationObjs',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }

                    }
                },
                ServiceLine: {
                    $cond: {
                        if: { $ne: ['$keyControl.isControlOverResource', true] },
                        then: {
                            $reduce: {
                                input: '$keyControl.serviceLine',
                                initialValue: '',
                                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                            }
                        },
                        else: ''
                    }
                },
                QualityObjectiveUniquesIds: {
                    $reduce: {
                        input: '$associatedQualityObjectives',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.qualityObjectiveId'] }
                    }
                },
                ActiveQualityRiskUniqueIds: {
                    $reduce: {
                        input: '$keyControl.relatedQualityRisks.uniqueId',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
                    }
                },
                ActiveQualitySubRiskUniqueIds: {
                    $reduce: {
                        input: '$keyControl.relatedSubRisks.uniqueId',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this'] }
                    }
                },
                Draft: { $eq: ['$keyControl.status', 'StatusType_Draft'] },
                IECSystems: {
                    $cond: {
                        if: { $eq: ['$keyControl.controlType._id', 'ControlType_ItDependentManual'] }, then: {
                            $reduce: {
                                input: {
                                    $map: {
                                        input: "$keyControl.addInformationExecutionControls",
                                        as: "control",
                                        in: {
                                            $let: {
                                                vars: {
                                                    reducedValue: {
                                                        $reduce: {
                                                            input: {
                                                                $map: {
                                                                    input: "$$control.ipeSystems",
                                                                    as: "ipe",
                                                                    in: {
                                                                        $let: {
                                                                            vars: {
                                                                                matched: {
                                                                                    $filter: {
                                                                                        input: "$ipeSystemsWithId",
                                                                                        as: "sys",
                                                                                        cond: { $eq: ["$$sys.id", "$$ipe"] }
                                                                                    }
                                                                                }
                                                                            },
                                                                            in: {
                                                                                $cond: {
                                                                                    if: { $gt: [{ $size: "$$matched" }, 0] },
                                                                                    then: { $concat: ["IEC-ID-", "$$control.uniqueId", ":", { $arrayElemAt: ["$$matched.name", 0] }] },
                                                                                    else: { $concat: ["IEC-ID-", "$$control.uniqueId", ":N/A"] }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            },
                                                            initialValue: "",
                                                            in: {
                                                                $concat: [
                                                                    "$$value",
                                                                    { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: ";" } },
                                                                    "$$this"
                                                                ]
                                                            }
                                                        }
                                                    }
                                                },
                                                in: {
                                                    $cond: {
                                                        if: { $eq: ["$$reducedValue", ""] },
                                                        then: { $concat: ["IEC-ID-", "$$control.uniqueId", ":N/A"] },
                                                        else: "$$reducedValue"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                },
                                initialValue: "",
                                in: {
                                    $concat: [
                                        "$$value",
                                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: ";" } },
                                        "$$this"
                                    ]
                                }
                            }
                        }, else: ''
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
                IsControlOverResourceNew: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$keyControl.isControlOverResource', true] }, then: "Yes" },
                            { case: { $eq: ['$keyControl.isControlOverResource', false] }, then: "No" },
                        ],
                        default: ''
                    }
                },
                KeyControlOperatedByServiceProviderNew: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$keyControl.isControlOperatedByServiceProvider', true] }, then: "Yes" },
                            { case: { $eq: ['$keyControl.isControlOperatedByServiceProvider', false] }, then: "No" },
                        ],
                        default: ''
                    },
                },
                ArcherPublishedOn: new Date().toISOString(),
                IsThisIECReportOrOther: {
                    $reduce: {
                        input: {
                            $map: {
                                input: { $cond: { if: { $isArray: "$keyControl.addInformationExecutionControls" }, then: "$keyControl.addInformationExecutionControls", else: [] } },
                                as: "iecRepName",
                                in: {
                                    $reduce: {
                                        input: iecReportNameResources,
                                        initialValue: "",
                                        in: {
                                            $cond: {
                                                if: {
                                                    $eq: ["$$this._id", "$$iecRepName.iecReports"]
                                                },
                                                then: {
                                                    $concat: [
                                                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: { $concat: ["$$value", ";"] } } },
                                                        'IEC-ID-', "$$iecRepName.uniqueId", ':',
                                                        "$$this.name"
                                                    ]
                                                },
                                                else: "$$value"
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        initialValue: "",
                        in: {
                            $cond: {
                                if: { $eq: ["$$value", ""] },
                                then: "$$this",
                                else: { $concat: ["$$value", ";", "$$this"] }
                            }
                        }
                    }
                },
                ControlOwnerProceduresOverCompleteness: {
                    $reduce: {
                        input: '$keyControl.addInformationExecutionControls',
                        initialValue: '',
                        in: {
                            $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, {
                                $cond: {
                                    if: { $eq: ['$keyControl.controlType._id', 'ControlType_ItDependentManual'] }, then: {
                                        $concat: ['IEC-ID-', "$$this.uniqueId", ':']
                                    }, else: ''
                                }
                            }, '$$this.iecOwnerProcedures']
                        }
                    }
                },
                ControlOwnerProceduresOverCompletenessLocal: {
                    $reduce: {
                        input: '$keyControl.addInformationExecutionControls',
                        initialValue: '',
                        in: {
                            $concat: [
                                '$$value',
                                {
                                    $cond: {
                                        if: { $eq: ['$$value', ''] },
                                        then: '',
                                        else: '; '
                                    }
                                },
                                {
                                    $cond: {
                                        if: { $eq: ['$keyControl.controlType._id', 'ControlType_ItDependentManual'] },
                                        then: {
                                            $cond: {
                                                if: { $ne: ['$$this.iecOwnerProceduresAlt', ''] },
                                                then: { $concat: ['IEC-ID-', "$$this.uniqueId", ':'] },
                                                else: ''
                                            }
                                        },
                                        else: ''
                                    }
                                },
                                '$$this.iecOwnerProceduresAlt'
                            ]
                        }
                    }
                },
                IECRationale: '$keyControl.iecRationale',
                IECRationaleLocal: '$keyControl.iecRationaleAlt',
                IECIDs: {
                    $reduce: {
                        input: '$keyControl.addInformationExecutionControls.uniqueId',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, { $cond: { if: { $eq: ['$keyControl.controlType._id', 'ControlType_ItDependentManual'] }, then: '$$this', else: '' } }] }

                    }
                },
                ControlsByPeriod: {
                    $cond: {
                        if: {
                            $eq: ["$keyControl.prevId", '']
                        },
                        then: { $concat :[ "Control added in FY", { $substr:['$fiscalYear',2,2] } ] },
                        else: "Rolled forward Control"
                    }
                },
                PCAOB_Configured_Object: {
                $cond: { if: { $eq: ['$keyControl.isPCAOBRegistered', true] }, then: 'Yes', else: 'No' }
                },//Added New column PCAOB_Configured_Object keycontrol
                EntityFiscalYear: '$fiscalYear',
                "FiscalYear": {$concat:['FY',{$substr:['$fiscalYear',2,2]}]},
                IsQoOverrideEnabled: '$keyControl.isQoOverrideEnabled'
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
            $match: {
                //type:'QualityRisk'
                $expr: {
                    $and: [
                        {
                            $eq: ["$type", "QualityRisk"],
                        },
                        {
                            $in: ["$fiscalYear", fiscalYearFilter]
                        }
                    ]
                }
            }
        }, {
            $lookup: {
                from: 'firm',
                //localField: 'firmId',
                //foreignField: 'abbreviation',
                let: {
                    firmIdCk: '$firmId',
                    fiscalYearOfQR: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$abbreviation", "$$firmIdCk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfQR"]
                                },
                                {
                                    $or: [{ $eq: ["$isRollForwardedFromPreFY", true] }, { $eq: ["$isCreatedInCurrentFY", true] }]
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
                    $or:orConditions
                }
            }
        }, {
            $lookup: {
                from: 'documentation',
                //localField: 'relatedObjectives',
                //foreignField: 'uniqueId',
                let: {
                    relatedObjectivesCk: '$relatedObjectives',
                    fiscalYearOfQR: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", "$$relatedObjectivesCk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfQR"]
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
                FirmPublishedDate: '$firm.publishedDate',
                LastProcessedDate: new Date().toISOString(),
                NetworkId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Network"] }, then: '$firmId', else: '' }
                },
                AreaId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Area"] }, then: '$firmId', else: '' }
                },
                BusinessUnitId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_BusinessUnit"] }, then: '$firmId', else: '' }
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
                RegionId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Region"] }, then: '$firmId', else: '' }
                },
                EntityType: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$firm.type', "EntityType_Area"] }, then: "Area" },
                            { case: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: "Cluster" },
                            { case: { $eq: ['$firm.type', "EntityType_Group"] }, then: "Location" },
                            { case: { $eq: ['$firm.type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                            { case: { $eq: ['$firm.type', "EntityType_Network"] }, then: "Network" },
                            { case: { $eq: ['$firm.type', "EntityType_BusinessUnit"] }, then: "Business Unit" },
                            { case: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: "Sub-Cluster" },
                            { case: { $eq: ['$firm.type', "EntityType_Region"] }, then: "Region" }
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
                },
                ArcherPublishedOn: new Date().toISOString(),
                PCAOB_Configured_Object: {
                    $cond: { if: { $eq: ['$isPCAOBRegistered', true] }, then: 'Yes', else: 'No' }
                },//Added New column PCAOB_Configured_Object QR
                EntityFiscalYear: '$fiscalYear',
                "FiscalYear": {$concat:['FY',{$substr:['$fiscalYear',2,2]}]}
            },
        }, {
            $out: 'sqmarcherqualityrisktemp'
        }])


        //Sub-Risk 
        db.documentation.aggregate([{
            $match: {
                //type:'SubRisk'
                $expr: {
                    $and: [
                        {
                            $eq: ["$type", "SubRisk"],
                        },
                        {
                            $in: ["$fiscalYear", fiscalYearFilter]
                        }
                    ]
                }
            }
        }, {
            $lookup: {
                from: 'firm',
                //localField: 'firmId',
                //foreignField: 'abbreviation',
                let: {
                    firmIdCk: '$firmId',
                    fiscalYearOfSR: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$abbreviation", "$$firmIdCk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfSR"]
                                },
                                {
                                    $or: [{ $eq: ["$isRollForwardedFromPreFY", true] }, { $eq: ["$isCreatedInCurrentFY", true] }]
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
                    $or:orConditions
                }    
            }
        }, {
            $project: {
                QualitySubRiskUniqueId: '$uniqueId',
                EntityId: '$firmId',
                FirmPublishedDate: '$firm.publishedDate',
                LastProcessedDate: new Date().toISOString(),
                NetworkId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Network"] }, then: '$firmId', else: '' }
                },
                AreaId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Area"] }, then: '$firmId', else: '' }
                },
                BusinessUnitId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_BusinessUnit"] }, then: '$firmId', else: '' }
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
                RegionId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Region"] }, then: '$firmId', else: '' }
                },
                EntityType: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$firm.type', "EntityType_Area"] }, then: "Area" },
                            { case: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: "Cluster" },
                            { case: { $eq: ['$firm.type', "EntityType_Group"] }, then: "Location" },
                            { case: { $eq: ['$firm.type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                            { case: { $eq: ['$firm.type', "EntityType_Network"] }, then: "Network" },
                            { case: { $eq: ['$firm.type', "EntityType_BusinessUnit"] }, then: "Business Unit" },
                            { case: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: "Sub-Cluster" },
                            { case: { $eq: ['$firm.type', "EntityType_Region"] }, then: "Region" }
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
                },
                ArcherPublishedOn: new Date().toISOString(),
                PCAOB_Configured_Object: {
                    $cond: { if: { $eq: ['$isPCAOBRegistered', true] }, then: 'Yes', else: 'No' }
                },//Added New column PCAOB_Configured_Object QR
                EntityFiscalYear: '$fiscalYear',
                "FiscalYear": {$concat:['FY',{$substr:['$fiscalYear',2,2]}]}
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
            $match: {
                //type:'QualityObjective'
                $expr: {
                    $and: [
                        {
                            $in: ["$fiscalYear", fiscalYearFilter]
                        },
                        {
                            $eq: ['$type', 'QualityObjective']
                        }
                    ]
                }
            }
        }, {
            $lookup: {
                from: 'firm',
                //localField: 'firmId',
                //foreignField: 'abbreviation',
                let: {
                    firmIdCk: '$firmId',
                    fiscalYearOfQO: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$abbreviation", "$$firmIdCk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfQO"]
                                },
                                {
                                    $or: [{ $eq: ["$isRollForwardedFromPreFY", true] }, { $eq: ["$isCreatedInCurrentFY", true] }]
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
                    $or:orConditions
                }
                
            }
        }, {
            $lookup: {
                from: 'component',
                //localField: 'componentId',
                //foreignField: '_id',
                let: {
                    componentIdCk: '$componentId',
                    fiscalYearOfQO: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$_id", "$$componentIdCk"],
                                },
                                {
                                    $in: ["$$fiscalYearOfQO", "$fiscalYear"]
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
                QualityObjectiveUniqueID: '$uniqueId',
                QualityObjectiveID: '$qualityObjectiveId',
                EntityId: '$firmId',
                FirmPublishedDate: '$firm.publishedDate',
                LastProcessedDate: new Date().toISOString(),
                NetworkId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Network"] }, then: '$firmId', else: '' }
                },
                AreaId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Area"] }, then: '$firmId', else: '' }
                },
                BusinessUnitId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_BusinessUnit"] }, then: '$firmId', else: '' }
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
                RegionId: {
                    $cond: { if: { $eq: ['$firm.type', "EntityType_Region"] }, then: '$firmId', else: '' }
                },
                EntityType: {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$firm.type', "EntityType_Area"] }, then: "Area" },
                            { case: { $eq: ['$firm.type', "EntityType_Cluster"] }, then: "Cluster" },
                            { case: { $eq: ['$firm.type', "EntityType_Group"] }, then: "Location" },
                            { case: { $eq: ['$firm.type', "EntityType_MemberFirm"] }, then: "Member Firm" },
                            { case: { $eq: ['$firm.type', "EntityType_Network"] }, then: "Network" },
                            { case: { $eq: ['$firm.type', "EntityType_BusinessUnit"] }, then: "Business Unit" },
                            { case: { $eq: ['$firm.type', "EntityType_SubCluster"] }, then: "Sub-Cluster" },
                            { case: { $eq: ['$firm.type', "EntityType_Region"] }, then: "Region" }
                        ],
                        default: ''
                    }
                },
                QualityObjectiveName: '$name',
                QualityObjectiveDescription: '$description',
                Component: '$component.name',
                ArcherPublishedOn: new Date().toISOString(),
                PCAOB_Configured_Object: {
                    $cond: { if: { $eq: ['$isPCAOBRegistered', true] }, then: 'Yes', else: 'No' }
                },//Added New column PCAOB_Configured_Object QR
                EntityFiscalYear: '$fiscalYear',
                "FiscalYear": {$concat:['FY',{$substr:['$fiscalYear',2,2]}]}
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
                from: 'documentation',
                let: {
                    firmId: '$abbreviation',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [
                    {
                        $match:
                        {
                            $expr:
                            {
                                $and:
                                    [
                                        { $eq: ["$type", "Resource"] },
                                        { $eq: ["$firmId", "$$firmId"] },
                                        { $eq: ["$fiscalYear", "$$fiscalYearOfFirm"] }
                                    ]
                            }
                        }
                    }
                ],
                as: "resourcesCreatedAtPublishedEntity"
            }
        }, {
            $unwind: "$resourcesCreatedAtPublishedEntity"
        }, {
            $addFields: {
                'resourcesCreatedAtPublishedEntity.entityType': '$type',
                'resourcesCreatedAtPublishedEntity.publishedDate': '$publishedDate'
            }
        }, {
            $replaceRoot: {
                newRoot: '$resourcesCreatedAtPublishedEntity'
            }
        }, {
            $lookup: {
                from: 'keycontrolresource',
                //localField: 'engagementType',
                //foreignField: '_id',
                let: {
                    engagementTypeCk: '$engagementType',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$engagementTypeCk"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
                                }
                            ]
                        }
                    }
                }],
                as: 'engagementType'
            }
        }, {
            $lookup: {
                from: 'keycontrolresource',
                //localField: 'serviceLine',
                //foreignField: '_id',
                let: {
                    serviceLineCk: '$serviceLine',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$_id", "$$serviceLineCk"],
                                },
                                {
                                    $in: ["$$fiscalYearOfFirm", "$fiscalYear"]
                                }
                            ]
                        }
                    }
                }],
                as: 'serviceLine'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    functionId: '$relatedFunction',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            //$eq:['$type','Function']
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
                            $eq: ['$$functionId', { $toString: '$_id' }]
                        }
                    }
                }],
                as: 'function/SL'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    functionId: '$techRelatedFunction',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            //$eq:['$type','Function']
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
                            $eq: ['$$functionId', { $toString: '$_id' }]
                        }
                    }
                }],
                as: 'technologyRelatedFunction'
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    functionIds: '$relatedFunctionList',
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
                                    $in: [{ $toString: '$_id' }, { $cond: { if: { $isArray: '$$functionIds' }, then: '$$functionIds', else: [] } }]
                                }
                            ]
                        }
                    }
                }],
                as: 'multiFunction/SL'
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    functionIds: '$techRelatedFunctionList',
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
                                    $in: [{ $toString: '$_id' }, { $cond: { if: { $isArray: '$$functionIds' }, then: '$$functionIds', else: [] } }]
                                }
                            ]
                        }
                    }
                }],
                as: 'multiTechnologyRelatedFunction'
            }
        }, {
            $lookup: {
                from: 'documentation',
                //localField: 'relatedQualityRisks',
                //foreignField: 'uniqueId',
                let: {
                    relatedQualityRisksCk: '$relatedQualityRisks',
                    fiscalYearOfFirm: '$fiscalYear'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$uniqueId", "$$relatedQualityRisksCk"],
                                },
                                {
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                }
                            ]
                        }
                    }
                }],
                as: 'qualityRisks'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    tagId: '$tags',
                    fiscalYearOfFirm: '$fiscalYear'
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
                                    $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                }
                            ]
                        }
                    }
                }, {
                    $match: {
                        $expr: {
                            $in: [{ $toString: '$_id' }, { $cond: { if: { $isArray: '$$tagId' }, then: '$$tagId', else: [] } }]
                        }
                    }
                }],
                as: 'tag'
            }
        }, {
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
                FirmPublishedDate: "$publishedDate",
                LastProcessedDate: new Date().toISOString(),
                'EntityType': {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$entityType', "EntityType_Area"] }, then: "Area" },
                            { case: { $eq: ['$entityType', "EntityType_Cluster"] }, then: "Cluster" },
                            { case: { $eq: ['$entityType', "EntityType_Group"] }, then: "Location" },
                            { case: { $eq: ['$entityType', "EntityType_MemberFirm"] }, then: "Member Firm" },
                            { case: { $eq: ['$entityType', "EntityType_Network"] }, then: "Network" },
                            { case: { $eq: ['$entityType', "EntityType_BusinessUnit"] }, then: "Business Unit" },
                            { case: { $eq: ['$entityType', "EntityType_SubCluster"] }, then: "Sub-Cluster" },
                            { case: { $eq: ['$entityType', "EntityType_Region"] }, then: "Region" }
                        ],
                        default: ''
                    }
                },
                'ResourceCategory': {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$categoryofResource', "ResourceCategory_A"] }, then: "Category A" },
                            { case: { $eq: ['$categoryofResource', "ResourceCategory_B"] }, then: "Category B" },
                            { case: { $eq: ['$categoryofResource', "ResourceCategory_C"] }, then: "Category C" },
                            { case: { $eq: ['$categoryofResource', "ResourceCategory_inScope"] }, then: "In scope for ITGCs" },
                            { case: { $eq: ['$categoryofResource', "ResourceCategory_out_Of_Scope"] }, then: "Out of scope for ITGCs" },
                        ],
                        default: ''
                    }
                },
                'CategoryServiceProvider':  {
                    $switch: {
                        branches: [
                            { case: { $eq: ['$categoryServiceProvider', "ServiceCategory_A"] }, then: "Category A" },
                            { case: { $eq: ['$categoryServiceProvider', "ServiceCategory_B"] }, then: "Category B" },
                            { case: { $eq: ['$categoryServiceProvider', "ServiceCategory_C"] }, then: "Category C" },
                            { case: { $eq: ['$categoryServiceProvider', "ServiceCategory_inScope"] }, then: "In Scope" },                       
                            { case: { $eq: ['$categoryServiceProvider', "ServiceCategory_out_Of_Scope"] }, then: "Out of Scope" },                   
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
                            { case: { $eq: ['$isResourceProvidedByService', true] }, then: "Yes" },
                            { case: { $eq: ['$isResourceProvidedByService', false] }, then: "No" }
                        ],
                        default: ''
                    }
                },
                'NameOfServiceProvider': '$nameOfServiceProvider',
                'RelatedQualityRisks': {
                    $reduce: {
                        input: '$qualityRisks',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.uniqueId', ':', '$$this.name'] }
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
                },
                'ArcherPublishedOn': new Date().toISOString(),
                'MultiFunction/SL': {
                    $reduce: {
                        input: '$multiFunction/SL',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
                    }
                },
                'MultiTechnologyFunction': {
                    $reduce: {
                        input: '$multiTechnologyRelatedFunction',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
                    }
                },
                PCAOB_Configured_Object: {
                        $cond: { if: { $eq: ['$isPCAOBRegistered', true] }, then: 'Yes', else: 'No' }
                    },//Added New column PCAOB_Configured_Object resource
                EntityFiscalYear: '$fiscalYear',
                "FiscalYear": {$concat:['FY',{$substr:['$fiscalYear',2,2]}]}
            }
        }, {
            $out: 'sqmarcherresourcestemp'
        }])


        //function
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
            $graphLookup: {
                from: "firm",
                startWith: "$parentFirmId",
                connectFromField: "parentFirmId",
                connectToField: "abbreviation",
                as: "parentFirm",
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
                parentFirm: {
                    $filter: {
                        input: "$parentFirm",
                        as: 'parentfirm',
                        cond: {
                            $eq: ['$$parentfirm.fiscalYear', '$fiscalYear']
                        }
                    }
                }
            }
        }, {
            $addFields: {
                firmIds: {
                    $concatArrays: [
                        ["$abbreviation"], "$parentFirm.abbreviation"
                    ]
                },
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    "firms": "$firmIds",
                    fiscalYearOfFirm: "$fiscalYear"
                },
                pipeline: [
                    {
                        "$match":
                        {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$type", 'Function'] },
                                    {
                                        $gt: [{ $size: { $setIntersection: ['$$firms', '$hierarchy'] } }, 0]
                                    },
                                    {
                                        $eq: ["$fiscalYear", "$$fiscalYearOfFirm"]
                                    }
                                ]
                            }
                        }
                    },
                    { "$project": { "_id": 1, "name": 1 } }
                ],
                as: 'functions'
            }
        },
        {
            $unwind: '$functions'
        },
        {
            $addFields: {
                "functions.firmIds": "$firmIds",
                "functions.abbreviation": "$abbreviation",
                "functions.parentFirms": "$parentFirm",
                "functions.type": "$type",
                "functions.functionId": { "$toString": "$_id" },
                "functions.publishedDate": "$publishedDate",
                "functions.fiscalYear": "$fiscalYear"
            }
        },
        {
            $replaceRoot: {
                newRoot: '$functions'
            }
        },
        {
            $lookup: {
                from: 'functionowner',
                let: {
                    "functionId": { $toString: "$_id" },
                    firmId: '$abbreviation',
                    fiscalYearOfFirm: "$fiscalYear"
                },
                pipeline: [
                    {
                        "$match":
                        {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$functionId", "$$functionId"] },
                                    { "$eq": ["$firmId", '$$firmId'] },
                                    { "$eq": ["$fiscalYear", "$$fiscalYearOfFirm"] }
                                ]
                            }
                        }
                    },
                    { "$project": { "title": 1, "notApplicable": 1 } }
                ],
                as: 'functionTitle'
            }
        },
        {
            $unwind: {
                path: '$functionTitle',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $match: {
                'functionTitle.notApplicable': {
                    $ne: true
                }
            }
        },
        {
            $lookup: {
                from: 'title',
                let: { "titleId": "$functionTitle.title", fiscalYearOfFirm: "$fiscalYear" },
                pipeline: [
                    {
                        "$match":
                        {
                            "$expr": {
                                "$and": [
                                    //{ "$eq": [ "$$titleId", "$_id" ] },
                                    { "$in": [{ "$toString": "$_id" }, { $cond: { if: { $isArray: "$$titleId" }, then: "$$titleId", else: [] } }] },
                                    { "$eq": ["$fiscalYear", "$$fiscalYearOfFirm"] }
                                ]
                            }
                        }
                    },
                    { "$project": { "name": 1 } }
                ],
                as: 'functionLeaderTitle'
            }
        },
        {
            $unwind: {
                path: '$functionLeaderTitle',
                preserveNullAndEmptyArrays: true
            }
        },
        {
            $lookup: {
                from: 'titleassignment',
                let: {
                    "titleId": "$functionLeaderTitle._id",
                    "firmId": "$abbreviation",
                    fiscalYearOfFirm: "$fiscalYear"
                },
                pipeline: [
                    { "$addFields": { "titleId": { "$toObjectId": "$titleId" } } },
                    {
                        "$match":
                        {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$titleId", "$$titleId"] },
                                    { "$eq": ["$firmId", '$$firmId'] },
                                    { "$eq": ["$fiscalYear", "$$fiscalYearOfFirm"] }
                                ]
                            }
                        }
                    }
                ],
                as: 'functionLeaderTitleAssignment'
            }
        },
        {
            $unwind: {
                path: '$functionLeaderTitleAssignment',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $unwind: {
                path: '$functionLeaderTitleAssignment.assignments',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $project: {
                UniqueRowId: {
                    $concat: [{ $toString: "$_id" }, '$abbreviation', {
                        $cond: {
                            if: '$functionLeaderTitle',
                            then: { $toString: '$functionLeaderTitle._id' },
                            else: ''
                        }
                    }, {
                        $cond: {
                            if: '$functionLeaderTitleAssignment.assignments.email',
                            then: '$functionLeaderTitleAssignment.assignments.email',
                            else: ''
                        }
                    }]
                },
                FunctionId: { $toString: "$_id" },
                EntityFunctionId: { $concat: [{ $toString: "$_id" }, '$abbreviation'] },
                EntityId: '$abbreviation',
                FirmPublishedDate: "$publishedDate",
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
                BusinessUnitId: {
                    $cond: { if: { $eq: ['$type', "EntityType_BusinessUnit"] }, then: '$abbreviation', else: '' }
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
                            { case: { $eq: ['$type', "EntityType_SubCluster"] }, then: "Sub-Cluster" },
                            { case: { $eq: ['$type', "EntityType_BusinessUnit"] }, then: "Business Unit" }
                        ],
                        default: ''
                    }
                },
                FunctionName: '$name',
                FunctionLeaderTitle: {
                    $cond: {
                        if: '$functionLeaderTitle',
                        then: '$functionLeaderTitle.name',
                        else: ''
                    }
                },
                FunctionLeaderTitleId: {
                    $cond: {
                        if: '$functionLeaderTitle',
                        then: '$functionLeaderTitle._id',
                        else: ''
                    }
                },
                FunctionLeaderTitleAssignment: '$functionLeaderTitleAssignment.assignments.email',
                FunctionLeaderTitleAssignmentDisplayName: '$functionLeaderTitleAssignment.assignments.displayName',
                _id: 0,
                ArcherPublishedOn: new Date().toISOString(),
                EntityFiscalYear: '$fiscalYear',
                "FiscalYear": {$concat:['FY',{$substr:['$fiscalYear',2,2]}]}
            },

        },
        {
            $out: 'sqmarcherfunctiontemp'
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

        publishedEntityIds.forEach(function(f){
            db.sqmarchersubrisk.remove({ EntityId: { $eq:f.abbreviation },EntityFiscalYear:f.fiscalYear });
            db.sqmarcherkeycontrol.remove({ EntityId: { $eq:f.abbreviation },EntityFiscalYear:f.fiscalYear });
            db.sqmarcherqualityrisk.remove({ EntityId: { $eq:f.abbreviation } ,EntityFiscalYear:f.fiscalYear});
            db.sqmarcherqualityobjective.remove({ EntityId: { $eq:f.abbreviation },EntityFiscalYear:f.fiscalYear });
            db.sqmarchertitleassignments.remove({ EntityId: { $eq:f.abbreviation },EntityFiscalYear:f.fiscalYear });
            db.sqmarcherresources.remove({ EntityId: { $eq:f.abbreviation },EntityFiscalYear:f.fiscalYear });
            db.sqmarcherfunction.remove({ EntityId: { $eq:f.abbreviation },EntityFiscalYear:f.fiscalYear });
        })


        //db.sqmarcherqualitysubrisktemp.copyTo('sqmarchersubrisk');
        //db.sqmarcherkeycontroltemp.copyTo('sqmarcherkeycontrol');
        // db.sqmarcherrequirementcontroltemp.copyTo('sqmarcherkeycontrol');
        // db.sqmarcherqualityrisktemp.copyTo('sqmarcherqualityrisk');
        // db.sqmarcherqualityobjectivetemp.copyTo('sqmarcherqualityobjective');
        // db.sqmarcherresourcestemp.copyTo('sqmarcherresources');
        // db.sqmarcherfunctiontemp.copyTo('sqmarcherfunction');

        db.sqmarcherqualitysubrisktemp.aggregate([
            { $merge: { into: "sqmarchersubrisk" } }
        ])
        db.sqmarcherkeycontroltemp.aggregate([
            { $merge: { into: "sqmarcherkeycontrol" } }
        ])
        db.sqmarcherrequirementcontroltemp.aggregate([
            { $merge: { into: "sqmarcherkeycontrol" } }
        ])
        db.sqmarcherqualityrisktemp.aggregate([
            { $merge: { into: "sqmarcherqualityrisk" } }
        ])
        db.sqmarcherqualityobjectivetemp.aggregate([
            { $merge: { into: "sqmarcherqualityobjective" } }
        ])
        db.sqmarcherresourcestemp.aggregate([
            { $merge: { into: "sqmarcherresources" } }
        ])
        db.sqmarcherfunctiontemp.aggregate([
            { $merge: { into: "sqmarcherfunction" } }
        ])

        var calc7Days = 7 * 24 * 60 * 60 * 1000;

        //QualityObjective,QualityRisk,SubRisk
        publishedEntityIds.forEach(function(f){
                db.sqmarcherqualityobjective.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear }).forEach(function (kc) {
                var existingQO = db.sqmarcherqualityobjectivemaster.findOne({ EntityId: kc.EntityId, QualityObjectiveUniqueID: kc.QualityObjectiveUniqueID, FiscalYear: kc.FiscalYear });
                var updatedEvent = existingQO && db.event.find({ publisher: kc.EntityId, actor: kc.QualityObjectiveUniqueID, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Update', modifiedOn: { $gt: existingQO.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var isQOUpdate = existingQO && updatedEvent.length > 0;
                if (!(existingQO)) {
                    var getNewEventDateQO = db.event.find({ publisher: kc.EntityId, actor: kc.QualityObjectiveUniqueID, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Add' }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                    db.sqmarcherqualityobjective.updateOne({ _id: kc._id }, { $set: { EventType: 'New', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: getNewEventDateQO.length > 0 ? getNewEventDateQO[0].modifiedOn : '' } })
                }
                else if (isQOUpdate) {
                    db.sqmarcherqualityobjective.updateOne({ _id: kc._id }, { $set: { EventType: 'Updated', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: updatedEvent[0].modifiedOn } })
                }
                else {
                    db.sqmarcherqualityobjective.updateOne({ _id: kc._id }, { $set: { EventType: existingQO.EventType, EventAction: existingQO.EventAction, LastPublishedRecordStatus: existingQO.LastPublishedRecordStatus, EventDate: existingQO.EventDate } })
                }
            })
            db.sqmarcherqualityobjectivemaster.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear  }).forEach(function (kc) {
                var existingQO = db.sqmarcherqualityobjective.findOne({ EntityId: kc.EntityId, QualityObjectiveUniqueID: kc.QualityObjectiveUniqueID, FiscalYear: kc.FiscalYear });
                var isQODelete = db.event.find({ publisher: kc.EntityId, actor: kc.QualityObjectiveUniqueID, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Delete', modifiedOn: { $gt: kc.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var frimInfo = db.firm.findOne({ abbreviation: kc.EntityId, fiscalYear: kc.EntityFiscalYear });
                if (!existingQO && isQODelete.length > 0) {
                    kc.EventType = 'Deleted';
                    kc.EventAction = 'Update';
                    kc.LastPublishedRecordStatus = 'Inactive';
                    kc.FirmPublishedDate = frimInfo.publishedDate;
                    kc.EventDate = isQODelete[0].modifiedOn;
                    db.sqmarcherqualityobjective.insertOne(kc);
                }
                else {
                    db.sqmarcherqualityobjective.updateOne({ _id: kc._id }, { $set: { EventType: existingQO.EventType, EventAction: existingQO.EventAction, LastPublishedRecordStatus: existingQO.LastPublishedRecordStatus, FirmPublishedDate: frimInfo.publishedDate, EventDate: existingQO.EventDate } });
                }
            })
            
            //QualityRisk
            db.sqmarcherqualityrisk.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear  }).forEach(function (kc) {
                var existingQR = db.sqmarcherqualityriskmaster.findOne({ EntityId: kc.EntityId, QualityRiskUniqueId: kc.QualityRiskUniqueId, FiscalYear: kc.FiscalYear });
                var updatedEventQR = existingQR && db.event.find({ publisher: kc.EntityId, actor: kc.QualityRiskUniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Update', modifiedOn: { $gt: existingQR.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var isQRUpdate = existingQR && updatedEventQR.length > 0;
                if (!(existingQR)) {
                    var getNewEventDateQR = db.event.find({ publisher: kc.EntityId, fiscalYear: kc.EntityFiscalYear, actor: kc.QualityRiskUniqueId, message: 'ActionType_Add' }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                    db.sqmarcherqualityrisk.updateOne({ _id: kc._id }, { $set: { EventType: 'New', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: getNewEventDateQR.length > 0 ? getNewEventDateQR[0].modifiedOn : '' } });
                }
                else if (isQRUpdate) {
                    db.sqmarcherqualityrisk.updateOne({ _id: kc._id }, { $set: { EventType: 'Updated', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: updatedEventQR[0].modifiedOn } })
                }
                else {
                    db.sqmarcherqualityrisk.updateOne({ _id: kc._id }, { $set: { EventType: existingQR.EventType, EventAction: existingQR.EventAction, LastPublishedRecordStatus: existingQR.LastPublishedRecordStatus, EventDate: existingQR.EventDate } })
                }
            })
            db.sqmarcherqualityriskmaster.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear  }).forEach(function (kc) {
                var existingQR = db.sqmarcherqualityrisk.findOne({ EntityId: kc.EntityId, QualityRiskUniqueId: kc.QualityRiskUniqueId, FiscalYear: kc.FiscalYear });
                var isQRDelete = db.event.find({ publisher: kc.EntityId, actor: kc.QualityRiskUniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Delete', modifiedOn: { $gt: kc.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var frimInfo = db.firm.findOne({ abbreviation: kc.EntityId, fiscalYear: kc.EntityFiscalYear });
                if (!existingQR && isQRDelete.length > 0) {
                    kc.EventType = 'Deleted';
                    kc.EventAction = 'Update';
                    kc.LastPublishedRecordStatus = 'Inactive';
                    kc.FirmPublishedDate = frimInfo.publishedDate;
                    kc.EventDate = isQRDelete[0].modifiedOn;
                    db.sqmarcherqualityrisk.insertOne(kc);
                }
                else {
                    db.sqmarcherqualityrisk.updateOne({ _id: kc._id }, { $set: { EventType: existingQR.EventType, EventAction: existingQR.EventAction, LastPublishedRecordStatus: existingQR.LastPublishedRecordStatus, FirmPublishedDate: frimInfo.publishedDate, EventDate: existingQR.EventDate } });
                }
            })

            //Quality Subrisk
            db.sqmarchersubrisk.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear  }).forEach(function (kc) {
                var existingSR = db.sqmarchersubriskmaster.findOne({ EntityId: kc.EntityId, QualitySubRiskUniqueId: kc.QualitySubRiskUniqueId, FiscalYear: kc.FiscalYear });
                var updatedEventSR = existingSR && db.event.find({ publisher: kc.EntityId, actor: kc.QualitySubRiskUniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Update', modifiedOn: { $gt: existingSR.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var isSRUpdate = existingSR && updatedEventSR.length > 0;
                if (!(existingSR)) {
                    var getNewEventDateSR = db.event.find({ publisher: kc.EntityId, actor: kc.QualitySubRiskUniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Add' }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                    db.sqmarchersubrisk.updateOne({ _id: kc._id }, { $set: { EventType: 'New', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: getNewEventDateSR.length > 0 ? getNewEventDateSR[0].modifiedOn : '' } });
                }
                else if (isSRUpdate) {
                    db.sqmarchersubrisk.updateOne({ _id: kc._id }, { $set: { EventType: 'Updated', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: updatedEventSR[0].modifiedOn } });
                }
                else {
                    db.sqmarchersubrisk.updateOne({ _id: kc._id }, { $set: { EventType: existingSR.EventType, EventAction: existingSR.EventAction, LastPublishedRecordStatus: existingSR.LastPublishedRecordStatus, EventDate: existingSR.EventDate } });
                }
            })
            db.sqmarchersubriskmaster.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear  }).forEach(function (kc) {
                var existingSR = db.sqmarchersubrisk.findOne({ EntityId: kc.EntityId, QualitySubRiskUniqueId: kc.QualitySubRiskUniqueId, FiscalYear: kc.FiscalYear });
                var isSRDelete = db.event.find({ publisher: kc.EntityId, actor: kc.QualitySubRiskUniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Delete', modifiedOn: { $gt: kc.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var frimInfo = db.firm.findOne({ abbreviation: kc.EntityId, fiscalYear: kc.EntityFiscalYear });
                if (!existingSR && isSRDelete.length > 0) {
                    kc.EventType = 'Deleted';
                    kc.EventAction = 'Update';
                    kc.LastPublishedRecordStatus = 'Inactive';
                    kc.FirmPublishedDate = frimInfo.publishedDate;
                    kc.EventDate = isSRDelete[0].modifiedOn;
                    db.sqmarchersubrisk.insertOne(kc);
                }
                else {
                    db.sqmarchersubrisk.updateOne({ _id: kc._id }, { $set: { EventType: existingSR.EventType, EventAction: existingSR.EventAction, LastPublishedRecordStatus: existingSR.LastPublishedRecordStatus, FirmPublishedDate: frimInfo.publishedDate, EventDate: existingSR.EventDate } });
                }
            })
            
        });

        //QO
        db.sqmarcherqualityobjective.updateMany({ FirmPublishedDate: { $lte: new Date(ISODate().getTime() - calc7Days).toISOString() } }, { $set: { EventAction: '' } });
        db.sqmarcherqualityobjectivemaster.drop();
        db.sqmarcherqualityobjective.aggregate([
            { $merge: { into: "sqmarcherqualityobjectivemaster" } }
        ]);

        //QR
        db.sqmarcherqualityrisk.updateMany({ FirmPublishedDate: { $lte: new Date(ISODate().getTime() - calc7Days).toISOString() } }, { $set: { EventAction: '' } });
        db.sqmarcherqualityriskmaster.drop();
        db.sqmarcherqualityrisk.aggregate([
            { $merge: { into: "sqmarcherqualityriskmaster" } }
        ]);

        //QSR
        db.sqmarchersubrisk.updateMany({ FirmPublishedDate: { $lte: new Date(ISODate().getTime() - calc7Days).toISOString() } }, { $set: { EventAction: '' } });
        db.sqmarchersubriskmaster.drop();
        db.sqmarchersubrisk.aggregate([
            { $merge: { into: "sqmarchersubriskmaster" } }
        ]);

        // Controls 
        publishedEntityIds.forEach(function(f){
                db.sqmarcherkeycontrol.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear }).forEach(function (kc) {
                var functionIds = kc.FunctionId ? kc.FunctionId.split(";") : [];

                var functionNamesInfo = db.sqmarcherfunction.find({ FunctionId: { $in: functionIds }, EntityId: kc.EntityId, FiscalYear: kc.FiscalYear }, { FunctionName: 1 });
                var functionNames = functionNamesInfo.toArray().map(f => f.FunctionName).filter(Boolean).join(';');
                var functionLeaderTitlesInfo = db.sqmarcherfunction.find({ FunctionId: { $in: functionIds }, EntityId: kc.EntityId, FiscalYear: kc.FiscalYear }, { FunctionLeaderTitle: 1, FunctionLeaderTitleId: 1 }).toArray();
                var functionLeaderTitle = functionLeaderTitlesInfo.map(f => f.FunctionLeaderTitle).filter(Boolean).join(';');
                var functionTitleAssignmentsInfo = db.sqmarcherfunction.find({ FunctionId: { $in: functionIds }, EntityId: kc.EntityId, FiscalYear: kc.FiscalYear }, { FunctionLeaderTitleAssignment: 1 });
                var functionLeaderTitleAssignment = functionTitleAssignmentsInfo.toArray().map(f => f.FunctionLeaderTitleAssignment).filter(Boolean).join(';');
                var functionLeaderTitleId = functionLeaderTitlesInfo.map(f => f.FunctionLeaderTitleId).filter(Boolean);

                var frimInfo = db.firm.findOne({ abbreviation: kc.EntityId, fiscalYear: kc.EntityFiscalYear });

                var getParents = db.firm.aggregate([{
                    $match: {
                        $and: [{
                            abbreviation: kc.EntityId,
                            fiscalYear: kc.EntityFiscalYear
                        }]
                    }
                }, {
                    $graphLookup: {
                        from: "firm",
                        startWith: "$parentFirmId",
                        connectFromField: "parentFirmId",
                        connectToField: "abbreviation",
                        as: "parentFirms",
                        restrictSearchWithMatch: {
                            "isPartOfGroup": "IsPartOfGroupType_No",
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
                    $project: {
                        _id: 0,
                        parentFirms: 1,
                        parentsFirmAbbreviations: 1
                    }
                }, {
                    $addFields: {
                        _id: '$abbreviation',
                        fiscalYear: '$fiscalYear',
                        parentsFirmAbbreviations: {
                            $map: {
                                input: '$parentFirms',
                                as: 'p',
                                in: '$$p.abbreviation'
                            }
                        }
                    }
                }]).toArray(f => f.parentsFirmAbbreviations)

                var parents = getParents.length ? getParents.find(x => x !== undefined).parentsFirmAbbreviations : [];
                var assignmentData = kc.qualityRiskUniqueIdArray;
                var rejectedQRs = db.action.find({ fiscalYear: kc.EntityFiscalYear, objectId: { $in: assignmentData }, "$or": [{ firmId: { $in: parents } }, { firmId: kc.EntityId }] }, { objectId: 1 }).toArray().map(f => f.objectId).filter(Boolean).join(';');;
                var qualityRisksAssociated = "";

                kc.qualityRiskUniqueIdArray.forEach(function (rc) {
                    if (rejectedQRs.search(rc) == -1) {
                        qualityRisksAssociated = qualityRisksAssociated + (qualityRisksAssociated.length ? ';' : '') + rc;
                    }
                })
                kc.RejectedQualityRiskUniqueIds = rejectedQRs;
                kc.ActiveQualityRiskUniqueIds = qualityRisksAssociated;

                if (kc.ActiveQualitySubRiskUniqueIds != "") {
                    var subRisksAssociated = "";
                    var subRisksRejected = "";
                    kc.ActiveQualitySubRiskUniqueIds.split(';').forEach(function (sr) {
                        var getSrsParents = db.documentation.find({ fiscalYear: kc.EntityFiscalYear, uniqueId: sr }, { relatedQualityRisks: 1, _id: 0 }).toArray().map(f => f.relatedQualityRisks)[0];;

                        if (getSrsParents.every((element) => rejectedQRs.includes(element))) {
                            subRisksRejected = subRisksRejected + (subRisksRejected.length ? ';' : '') + sr;
                        }
                        else {
                            subRisksAssociated = subRisksAssociated + (subRisksAssociated.length ? ';' : '') + sr;
                        }
                    });

                    kc.ActiveQualitySubRiskUniqueIds = subRisksAssociated;
                    kc.RejectedSubQualityRiskUniqueIds = subRisksRejected;
                }
                db.sqmarcherkeycontrol.updateOne({ _id: kc._id }, { $set: { FunctionName: functionNames, FunctionOwnerTitle: functionLeaderTitle, FunctionOwnerTitleId: functionLeaderTitleId, FunctionOwnerAssignments: functionLeaderTitleAssignment, ActiveQualityRiskUniqueIds: kc.ActiveQualityRiskUniqueIds, RejectedQualityRiskUniqueIds: kc.RejectedQualityRiskUniqueIds, ActiveQualitySubRiskUniqueIds: kc.ActiveQualitySubRiskUniqueIds, RejectedSubQualityRiskUniqueIds: kc.RejectedSubQualityRiskUniqueIds } })

                var existingControl = db.sqmarcherkeycontrolmaster.findOne({ EntityId: kc.EntityId, Ref_UniqueId: kc.Ref_UniqueId, FiscalYear: kc.FiscalYear });
                var eventUpdate = existingControl && db.event.find({ publisher: kc.EntityId, actor: kc.Ref_UniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Update', modifiedOn: { $gt: existingControl.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var isControlUpdate = existingControl && eventUpdate.length > 0;

                var eventUnreject = existingControl && db.event.find({ publisher: kc.EntityId, actor: kc.Ref_UniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_ManualUnreject', modifiedOn: { $gt: existingControl.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var isControlUnReject = existingControl && eventUnreject.length > 0;
                if (!(existingControl)) {
                    var getNewEventDate = db.event.find({ publisher: kc.EntityId, actor: kc.Ref_UniqueId, fiscalYear: kc.EntityFiscalYear, message: 'ActionType_Add' }).sort({ 'modifiedOn': -1 }).limit(1).toArray();

                    db.sqmarcherkeycontrol.updateOne({ _id: kc._id }, { $set: { EventType: 'New', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: getNewEventDate.length > 0 ? getNewEventDate[0].modifiedOn : '' } });
                }
                else if (isControlUnReject) {
                    db.sqmarcherkeycontrol.updateOne({ _id: kc._id }, { $set: { EventType: 'Unrejected', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: eventUnreject[0].modifiedOn } });
                }
                else if (isControlUpdate) {
                    var eventName = existingControl.EventType === 'Rejected' ? 'Unrejected' : 'Updated';
                    db.sqmarcherkeycontrol.updateOne({ _id: kc._id }, { $set: { EventType: eventName, EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: eventUpdate[0].modifiedOn } });
                }
                else {
                    if (existingControl.LastPublishedRecordStatus === 'Inactive') {
                        var events = {
                            "ActionType_Update": "Updated",
                            "ActionType_ManualUnreject": "Unrejected",
                            "ActionType_Add": "New",
                        }
                        var event = db.event.find({ publisher: kc.EntityId, actor: kc.Ref_UniqueId, fiscalYear: kc.EntityFiscalYear, message: { $in: ['ActionType_ManualUnreject', 'ActionType_Update', 'ActionType_Add'] } }).sort({ 'modifiedOn': -1 }).limit(1).count() > 0 ? db.event.find({ publisher: kc.EntityId, actor: kc.Ref_UniqueId, fiscalYear: kc.EntityFiscalYear, message: { $in: ['ActionType_ManualUnreject', 'ActionType_Update', 'ActionType_Add'] } }).sort({ 'modifiedOn': -1 }).limit(1).toArray() : [];

                        var eventName = event.length > 0 ? events[event[0].message] : existingControl.EventType === 'Rejected' ? 'Unrejected' : existingControl.EventType;

                        eventName = eventName === 'Deleted' ? 'Updated' : eventName;

                        existingControl.LastPublishedRecordStatus = 'Active';
                        existingControl.EventAction = 'Update';
                        existingControl.EventDate = event.length > 0 ? event[0].modifiedOn : '';
                        //existingControl.AutoCorrect = true;

                        db.sqmarcherkeycontrol.updateOne({ _id: kc._id }, { $set: { EventType: eventName, EventAction: existingControl.EventAction, LastPublishedRecordStatus: existingControl.LastPublishedRecordStatus, EventDate: existingControl.EventDate } });
                    }
                    else {
                        var distinct_current_function = [...new Set(functionNames.split(';'))].join(';');
                        var distinct_existing_function = [...new Set(existingControl.FunctionName.split(';'))].join(';');
                        if (kc.ControlOperatorTitleAssignments !== existingControl.ControlOperatorTitleAssignments || kc.ControlOwnerTitleAssignments !== existingControl.ControlOwnerTitleAssignments
                            || functionLeaderTitle !== existingControl.FunctionOwnerTitle || functionLeaderTitleAssignment !== existingControl.FunctionOwnerAssignments
                            || kc.ProcessOwnerTitles !== existingControl.ProcessOwnerTitles || kc.ProcessOwnerAssignments !== existingControl.ProcessOwnerAssignments
                            || kc.RejectedQualityRiskUniqueIds !== existingControl.RejectedQualityRiskUniqueIds || kc.RejectedSubQualityRiskUniqueIds !== existingControl.RejectedSubQualityRiskUniqueIds
                            || distinct_current_function !== distinct_existing_function || kc.ProcessName !== existingControl.ProcessName || kc.ProcessDescription !== existingControl.ProcessDescription) {
                            var isUpdateFromOutside = [];

                            // TO FIND UPDATES IN FUNCTIONS TITLEs
                            var all_FunctionIds = kc.FunctionId.split(";").filter(Boolean);

                            var function_process_ids = all_FunctionIds.map(funcId => ObjectId(funcId));

                            if (kc.ProcessId) {
                                function_process_ids.push(kc.ProcessId);
                            }

                            isUpdateFromOutside = db.event.find({
                                publisher: kc.EntityId, fiscalYear: kc.EntityFiscalYear, "actor": { $in: function_process_ids }, actorType: { $in: ['Function', 'Process'] }
                                , message: {
                                    $in: ['ActionType_Function_Title_Add', 'ActionType_Function_Title_Update', 'ActionType_Function_Title_Remove', 'ActionType_Function_NotApplicable'
                                        , 'ActionType_Process_Title_Add', 'ActionType_Process_Title_Update', 'ActionType_Process_Title_Remove', 'ActionType_Process_NotApplicable']
                                }
                                , modifiedOn: { $gt: existingControl.ArcherPublishedOn }
                            })
                                .sort({ 'modifiedOn': -1 }).limit(1).toArray();

                            //Find the updates in the Quality-Objective
                            if(kc.EntityId == autoQoNotReqFirms){
                                var current_QOs = kc.qualityObjectiveUniqueIdArray;
                                var existing_QOs = existingControl.qualityObjectiveUniqueIdArray;

                                var differenceInQOs = [
                                    ...current_QOs.filter(qo => ! existing_QOs.includes(qo)),
                                    ...existing_QOs.filter(qo => !current_QOs.includes(qo))
                                ]
                                if(differenceInQOs.length > 0){
                                    var isQOUpdated = db.event.find({
                                        fiscalYear: kc.EntityFiscalYear,
                                        actor: {$in: differenceInQOs}, actorType: 'QualityObjective',
                                        message: {$in: ['ActionType_Delete','ActionType_Add']},
                                        modifiedOn: { $gt: existingControl.ArcherPublishedOn },
                                        eventType: 'EventType_EXC_GLOBAL_LOC',
                                    }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                                    if (isQOUpdated.length > 0) {
                                        isUpdateFromOutside.push(isQOUpdated[0]);
                                    }
                                }
                             }                           
                            // Find Updates in Control Owner/Operator Titles
                            var current_ControlOperatorTitleIDs = kc.ControlOperatorTitleIDs ? kc.ControlOperatorTitleIDs.split(';') : [];
                            var existing_ControlOperatorTitleIDs = existingControl.ControlOperatorTitleIDs ? existingControl.ControlOperatorTitleIDs.split(';') : [];
                            var current_ControlOwnerTitlesIDs = kc.ControlOwnerTitlesIDs ? kc.ControlOwnerTitlesIDs.split(';') : [];
                            var existing_ControlOwnerTitlesIDs = existingControl.ControlOwnerTitlesIDs ? existingControl.ControlOwnerTitlesIDs.split(';') : [];
                            var processOwnerTitlesIds = kc.ProcessOwnerTitlesId ? kc.ProcessOwnerTitlesId.split(';').filter(Boolean).map(titleId => ObjectId(titleId)) : [];

                            var all_TitleIds = current_ControlOperatorTitleIDs.concat(existing_ControlOperatorTitleIDs, current_ControlOwnerTitlesIDs, existing_ControlOwnerTitlesIDs, functionLeaderTitleId, processOwnerTitlesIds);

                            var all_TitleIds_In_ObjectIds = all_TitleIds.filter(Boolean).map(titleId => ObjectId(titleId));

                            var isTitleUpdateded = db.event.find({
                                publisher: kc.EntityId, fiscalYear: kc.EntityFiscalYear, "actor": { $in: all_TitleIds_In_ObjectIds }, actorType: { $in: ['GlobalTitle', 'CustomTitle'] }
                                , message: { $in: ['ActionType_Title_Users_Add', 'ActionType_Title_Users_Update', 'ActionType_Title_Users_Remove'] }
                                , modifiedOn: { $gt: existingControl.ArcherPublishedOn }
                            })
                                .sort({ 'modifiedOn': -1 }).limit(1).toArray();

                            if (isTitleUpdateded.length) {
                                isUpdateFromOutside.push(isTitleUpdateded[0]);
                            }

                            if (!isUpdateFromOutside.length > 0) {
                                var rejectedQRs = db.action.find({ fiscalYear: kc.EntityFiscalYear, objectId: { $in: kc.qualityRiskUniqueIdArray }, firmId: kc.EntityId }, { objectId: 1 }).toArray();
                                var isUpdateFromOutsideForQRs = db.event.aggregate([{
                                    $match: {
                                        $expr: {
                                            $and: [
                                                { $eq: ['$fiscalYear', kc.EntityFiscalYear] },
                                                {
                                                    $or: [
                                                        { $in: ['$actor', kc.qualityRiskUniqueIdArray] },
                                                        { $in: ['$actor', rejectedQRs] }
                                                    ]
                                                },
                                                { $eq: ['$publisher', kc.EntityId] },
                                                { $gt: ['$modifiedOn', existingControl.ArcherPublishedOn] },
                                                { $in: ['$message', ['ActionType_ManualReject', 'ActionType_ManualUnreject']] }
                                            ]
                                        }
                                    }
                                },
                                {
                                    $project: {
                                        actor: 1,
                                        _id: 0
                                    }
                                }
                                ]).toArray();

                                if (isUpdateFromOutsideForQRs.length) {
                                    isUpdateFromOutside.push(isUpdateFromOutsideForQRs[0]);
                                }
                            }

                            if (distinct_current_function !== distinct_existing_function || kc.ProcessName !== existingControl.ProcessName || kc.ProcessDescription !== existingControl.ProcessDescription) {
                                var isObjectUpdated = db.event.find({
                                    publisher: 'global', "actor": { $in: function_process_ids }, actorType: { $in: ['Function', 'Process'] }
                                    , message: { $in: ['ActionType_Update'] }, fiscalYear: kc.EntityFiscalYear
                                    , modifiedOn: { $gt: existingControl.ArcherPublishedOn }
                                })
                                    .sort({ 'modifiedOn': -1 }).limit(1).toArray();

                                if (isObjectUpdated.length) {
                                    isUpdateFromOutside.push(isObjectUpdated[0]);
                                }
                            }

                            if (isUpdateFromOutside.length > 0) {
                                var maxModified = isUpdateFromOutside.reduce((max, obj) => max.modifiedOn > obj.modifiedOn ? max : obj);
                                existingControl.EventType = 'Updated';
                                existingControl.LastPublishedRecordStatus = 'Active';
                                existingControl.EventAction = 'Update';
                                existingControl.EventDate = maxModified.modifiedOn;
                            }
                        }
                        db.sqmarcherkeycontrol.updateOne({ _id: kc._id }, { $set: { EventType: existingControl.EventType, EventAction: existingControl.EventAction, LastPublishedRecordStatus: existingControl.LastPublishedRecordStatus, EventDate: existingControl.EventDate } });
                    }
                }
                    }); 

                db.sqmarcherkeycontrolmaster.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear }).forEach(function (control) {
                    var existingControl = db.sqmarcherkeycontrol.findOne({ EntityId: control.EntityId, UniqueControlID: control.UniqueControlID, FiscalYear: control.FiscalYear });

                    var frimInfo = db.firm.findOne({ abbreviation: control.EntityId, fiscalYear: control.EntityFiscalYear });

                    if (!(existingControl)) {
                        var eventDelete = db.event.find({ publisher: control.EntityId, actor: control.Ref_UniqueId, fiscalYear: control.EntityFiscalYear, message: 'ActionType_Delete', modifiedOn: { $gt: control.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                        var isControlDelete = eventDelete.length > 0;
                        var eventRejected = db.event.find({ publisher: control.EntityId, actor: control.Ref_UniqueId, fiscalYear: control.EntityFiscalYear, message: 'ActionType_ManualReject', modifiedOn: { $gt: control.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                        var isControlRejected = eventRejected.length > 0;

                        control.FirmPublishedDate = frimInfo.publishedDate;

                        if (isControlDelete) {
                            control.EventType = 'Deleted';
                            control.EventAction = 'Update';
                            control.LastPublishedRecordStatus = 'Inactive';
                            control.EventDate = eventDelete[0].modifiedOn;
                            control.LastProcessedDate = new Date().toISOString();
                        }
                        else if (isControlRejected) {
                            control.EventType = 'Rejected';
                            control.EventAction = 'Update';
                            control.LastPublishedRecordStatus = 'Inactive';
                            control.EventDate = eventRejected[0].modifiedOn;
                            control.LastProcessedDate = new Date().toISOString();
                        }
                        else {
                            if (control.EventType === 'Rejected') {
                                control.EventType = 'Rejected';
                                control.EventAction = 'Update';
                                control.LastPublishedRecordStatus = 'Inactive';
                                control.EventDate = '';
                                control.LastProcessedDate = new Date().toISOString();
                                //existingControl.AutoCorrect = true;
                            }
                            else {
                                var getParents = db.firm.aggregate([{
                                    $match: {
                                        $and: [{
                                            abbreviation: control.EntityId,
                                            fiscalYear: control.EntityFiscalYear
                                        }]
                                    }
                                }, {
                                    $graphLookup: {
                                        from: "firm",
                                        startWith: "$parentFirmId",
                                        connectFromField: "parentFirmId",
                                        connectToField: "abbreviation",
                                        as: "parentFirms",
                                        restrictSearchWithMatch: {
                                            "isPartOfGroup": "IsPartOfGroupType_No",
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
                                    $project: {
                                        _id: 0,
                                        parentFirms: 1,
                                        parentsFirmAbbreviations: 1
                                    }
                                }, {
                                    $addFields: {
                                        _id: '$abbreviation',
                                        fiscalYear: '$fiscalYear',
                                        parentsFirmAbbreviations: {
                                            $map: {
                                                input: '$parentFirms',
                                                as: 'p',
                                                in: '$$p.abbreviation'
                                            }
                                        }
                                    }
                                }]).toArray(f => f.parentsFirmAbbreviations)
                                var parents = getParents.length ? getParents.find(x => x !== undefined).parentsFirmAbbreviations : [];
                                var isRejected = false;
                                var getAssignment = db.documentation.find({ firmId: control.EntityId, uniqueId: control.Ref_UniqueId, fiscalYear: control.EntityFiscalYear }, { mitigatedResources: 1, supportingITApplication: 1, relatedQualityRisks: 1 }).toArray();
                                if (getAssignment.length) {

                                    if (control.qualityRiskUniqueIdArray.length) {
                                        //isRejected = db.action.find({fiscalYear:fiscalYearFilter,objectId:{$in:control.qualityRiskUniqueIdArray}, "$or": [{firmId : {$in:parents}},{firmId:control.EntityId}]}).count() > 0;

                                        var rejectedQRs = db.action.aggregate([{
                                            $match: {
                                                $expr: {
                                                    $and: [
                                                        { $eq: ['$fiscalYear', control.EntityFiscalYear] },
                                                        { $in: ['$objectId', control.qualityRiskUniqueIdArray] },
                                                        {
                                                            $or: [
                                                                { $in: ['$firmId', parents] },
                                                                { $eq: ['$firmId', control.EntityId] }
                                                            ]
                                                        }
                                                    ]
                                                }
                                            }
                                        },
                                        {
                                            $project: {
                                                abbreviation: 1,
                                                _id: 0
                                            }
                                        }
                                        ]).toArray();
                                    }

                                    var isRejected = rejectedQRs.length > 0;

                                    var assignmentData = getAssignment.find(x => x !== undefined);

                                    if (!isRejected && ((assignmentData.mitigatedResources && assignmentData.mitigatedResources.length) || (assignmentData.supportingITApplication && assignmentData.supportingITApplication.length))) {
                                        var mitigatedResourcesArray = assignmentData.mitigatedResources ? assignmentData.mitigatedResources : [];
                                        var supportingITApplicationArray = assignmentData.supportingITApplication ? assignmentData.supportingITApplication : [];
                                        var uniqueResources = mitigatedResourcesArray.concat(supportingITApplicationArray.filter((item) => mitigatedResourcesArray.indexOf(item) < 0));
                                        isRejected = db.action.find({ fiscalYear: control.EntityFiscalYear, objectId: { $in: uniqueResources }, "$or": [{ firmId: { $in: parents } }, { firmId: control.EntityId }] }).count() > 0;
                                    }
                                }

                                if (isRejected) {
                                    control.EventType = 'Rejected';
                                    control.EventAction = 'Update';
                                    control.LastPublishedRecordStatus = 'Inactive';
                                    control.EventDate = rejectedQRs[0].modifiedOn;
                                    control.LastProcessedDate = new Date().toISOString();
                                }
                                else {
                                    var deletedByParent = db.event.find({ actor: control.Ref_UniqueId, message: 'ActionType_Delete', fiscalYear: control.EntityFiscalYear, modifiedOn: { $gt: control.ArcherPublishedOn }, "$or": [{ publisher: { $in: parents } }, { publisher: control.EntityId }] }).sort({ 'modifiedOn': -1 }).toArray();
                                    var isDeletedByParent = deletedByParent.length > 0;
                                    if (isDeletedByParent) {
                                        control.EventType = 'Deleted';
                                        control.EventAction = 'Update';
                                        control.LastPublishedRecordStatus = 'Inactive';
                                        control.EventDate = deletedByParent[0].modifiedOn;
                                        control.LastProcessedDate = new Date().toISOString();
                                    }
                                }
                            }
                        }
                        db.sqmarcherkeycontrol.insertOne(control);
                    }
                    else {
                        db.sqmarcherkeycontrol.updateOne({ EntityId: control.EntityId, UniqueControlID: control.UniqueControlID, EntityFiscalYear: control.EntityFiscalYear }, { $set: { EventType: existingControl.EventType, EventAction: existingControl.EventAction, LastPublishedRecordStatus: existingControl.LastPublishedRecordStatus, FirmPublishedDate: frimInfo.publishedDate, EventDate: existingControl.EventDate, LastProcessedDate: existingControl.LastProcessedDate } });
                    }
                });
        })

        db.sqmarcherkeycontrol.updateMany({ FirmPublishedDate: { $lte: new Date(ISODate().getTime() - calc7Days).toISOString() } }, { $set: { EventAction: '' } });
        db.sqmarcherkeycontrolmaster.drop();
        db.sqmarcherkeycontrol.aggregate([
            { $merge: { into: "sqmarcherkeycontrolmaster" } }
        ])

        var publishedAbbreviations = publishedEntityIds.map(function(entity) {
            return entity.abbreviation;
        });

        //Resources
        db.sqmarcherresources.aggregate([{
            $lookup: {
                from: 'sqmarcherresourcesmaster',
                let: {
                    resEntityId: '$EntityId',
                    resUniqueId: '$UniqueId'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$EntityId", '$$resEntityId']
                                },
                                {
                                    $eq: ["$UniqueId", '$$resUniqueId'],
                                },
                                {
                                    $not: { $in: ['$EntityId', publishedAbbreviations] }
                                }
                            ]
                        }
                    }
                }],
                as: 'master_resources'
            }
        },
        {
            $unwind: {
                path: '$master_resources',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $addFields: {
                EventAction: '$master_resources.EventAction',
                EventType: '$master_resources.EventType',
                LastPublishedRecordStatus: '$master_resources.LastPublishedRecordStatus',
            }
        }, {
            $unset: "master_resources"
        }, {
            $out: "sqmarcherresources"
        }])

        publishedEntityIds.forEach(function(f){
            db.sqmarcherresources.find({ EntityId:f.abbreviation ,EntityFiscalYear:f.fiscalYear }).forEach(function (res) {

            var existingRes = db.sqmarcherresourcesmaster.findOne({ EntityId: res.EntityId, UniqueId: res.UniqueId, FiscalYear: res.FiscalYear });
            var updatedEventRes = existingRes && db.event.find({ publisher: res.EntityId, actor: ObjectId(res.UniqueId), fiscalYear: res.EntityFiscalYear, message: 'ActionType_Update', modifiedOn: { $gt: existingRes.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
            var isResUpdate = existingRes && updatedEventRes.length > 0;
            if (!(existingRes)) {
                var getNewEventDateRes = db.event.find({ publisher: res.EntityId, actor: ObjectId(res.UniqueId), fiscalYear: res.EntityFiscalYear, message: 'ActionType_Add' }).sort({ 'modifiedOn': -1 }).limit(1).toArray();

                db.sqmarcherresources.updateOne({ _id: res._id }, { $set: { EventType: 'New', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: getNewEventDateRes.length > 0 ? getNewEventDateRes[0].modifiedOn : '' } });
            }
            else if (isResUpdate) {
                db.sqmarcherresources.updateOne({ _id: res._id }, { $set: { EventType: 'Updated', EventAction: 'Update', LastPublishedRecordStatus: 'Active', EventDate: updatedEventRes[0].modifiedOn } });
            }
            else {
                db.sqmarcherresources.updateOne({ _id: res._id }, { $set: { EventType: existingRes.EventType, EventAction: existingRes.EventAction, LastPublishedRecordStatus: existingRes.LastPublishedRecordStatus, FirmPublishedDate: existingRes.FirmPublishedDate, EventDate: existingRes.EventDate } });
            }
            })

            db.sqmarcherresourcesmaster.find({ EntityId: f.abbreviation ,EntityFiscalYear:f.fiscalYear }).forEach(function (res) {

                var existingRes = db.sqmarcherresources.findOne({ EntityId: res.EntityId, UniqueId: res.UniqueId, FiscalYear: res.FiscalYear });
                var isResDelete = db.event.find({ publisher: res.EntityId, actor: ObjectId(res.UniqueId), fiscalYear: res.EntityFiscalYear, message: 'ActionType_Delete', modifiedOn: { $gt: res.ArcherPublishedOn } }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                if (!existingRes && isResDelete.length > 0) {
                    var frimInfo = db.firm.findOne({ abbreviation: res.EntityId, fiscalYear: res.EntityFiscalYear });
                    res.EventType = 'Deleted';
                    res.EventAction = 'Update';
                    res.LastPublishedRecordStatus = 'Inactive';
                    res.FirmPublishedDate = frimInfo.publishedDate;
                    res.EventDate = isResDelete[0].modifiedOn;
                    db.sqmarcherresources.insertOne(res);
                }
            })
        })

        db.sqmarcherresources.updateMany({ FirmPublishedDate: { $lte: new Date(ISODate().getTime() - calc7Days).toISOString() } }, { $set: { EventAction: '' } });
        db.sqmarcherresourcesmaster.drop();
        db.sqmarcherresources.aggregate([
            { $merge: { into: "sqmarcherresourcesmaster" } }
        ]);

        // Functions
        publishedEntityIds.forEach(function(f){
            //Functions Event Log
            db.sqmarcherfunction.find({ EntityId: f.abbreviation, EntityFiscalYear:f.fiscalYear }).forEach(function (func) {
                var existingFunc = db.sqmarcherfunctionmaster.findOne({ UniqueRowId: func.UniqueRowId });

                //UPDATE
                var updatedFunction = existingFunc && db.event.find({
                    publisher: 'global', actor: ObjectId(func.FunctionId), fiscalYear: func.EntityFiscalYear, message: 'ActionType_Update'
                    , modifiedOn: { $gt: existingFunc.ArcherPublishedOn }
                }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                var isUpdatedFunction = updatedFunction && updatedFunction.length > 0;

                if (!(existingFunc)) {
                    // NEW FUNCTIONS
                    var ids = [ObjectId(func.FunctionId)];
                    if (func.FunctionLeaderTitleId) {
                        ids.push(ObjectId(func.FunctionLeaderTitleId));
                    }
                    var getNewEventDate = db.event.find({
                        publisher: { $in: [func.EntityId, 'global'] }, actor: { $in: ids }, fiscalYear: func.EntityFiscalYear, message: {
                            $in: ['ActionType_Function_Title_Add'
                                , 'ActionType_Title_Users_Add', 'ActionType_Add', 'ActionType_Function_Entity_Assigned']
                        }
                    }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
                    db.sqmarcherfunction.updateOne({ _id: func._id }, {
                        $set: {
                            EventType: 'New', EventAction: 'Update', LastPublishedRecordStatus: 'Active'
                            , LastProcessedDate: new Date().toISOString(), EventDate: getNewEventDate.length > 0 ? getNewEventDate[0].modifiedOn : ''
                        }
                    });
                }
                else if (isUpdatedFunction) {
                    // UPDATE FUNCTIONS CHANGES
                    db.sqmarcherfunction.updateOne({ _id: func._id }, {
                        $set: {
                            EventType: 'Updated', EventAction: 'Update', LastPublishedRecordStatus: 'Active', LastProcessedDate: new Date().toISOString()
                            , EventDate: updatedFunction[0].modifiedOn
                        }
                    });
                }
                else {
                    var isUpdateFromOutside = [];
                    // TO FIND UPDATES IN FUNCTIONS TITLEs            
                    var ids = [ObjectId(func.FunctionId)];
                    if (func.FunctionLeaderTitleId) {
                        ids.push(ObjectId(func.FunctionLeaderTitleId));
                    }
                    isUpdateFromOutside = db.event.find({
                        publisher: { $in: [func.EntityId, 'global'] }, fiscalYear: func.EntityFiscalYear, "actor": { $in: ids }, actorType: { $in: ['Function', 'GlobalTitle', 'CustomTitle'] }
                        , message: {
                            $in: ['ActionType_Function_Title_Add', 'ActionType_Title_Users_Add', 'ActionType_Function_Title_Update', 'ActionType_Function_Title_Remove', 'ActionType_Title_Users_Update'
                                , 'ActionType_Title_Users_Remove', 'ActionType_Update']
                        }, modifiedOn: { $gt: existingFunc.ArcherPublishedOn }
                    })
                        .sort({ 'modifiedOn': -1 }).limit(1).toArray();
                    var updatedFromOutside = isUpdateFromOutside && isUpdateFromOutside.length > 0;
                    if (updatedFromOutside) {
                        existingFunc.EventType = 'Updated';
                        existingFunc.LastPublishedRecordStatus = 'Active';
                        existingFunc.EventAction = 'Update';
                        existingFunc.EventDate = isUpdateFromOutside[0].modifiedOn;
                    }
                    db.sqmarcherfunction.updateOne({ _id: func._id }, {
                        $set: {
                            EventType: existingFunc.EventType, EventAction: existingFunc.EventAction, LastPublishedRecordStatus: existingFunc.LastPublishedRecordStatus, LastProcessedDate: new Date().toISOString()
                            , EventDate: existingFunc.EventDate
                        }
                    });
                }
            })

            //DELETE FUNCTION
            db.sqmarcherfunctionmaster.find({ EntityId: f.abbreviation,EntityFiscalYear:f.fiscalYear }).forEach(function (func) {
                var existingFunction = db.sqmarcherfunction.findOne({ UniqueRowId: func.UniqueRowId, FiscalYear: func.FiscalYear });

                // TO FIND UPDATES IN FUNCTIONS TITLEs

                var ids = [ObjectId(func.FunctionId)];
                if (func.FunctionLeaderTitleId) {
                    ids.push(ObjectId(func.FunctionLeaderTitleId));
                }

                var funcDeleteData = db.event.find({
                    publisher: { $in: [func.EntityId, 'global'] }, actor: { $in: ids }, fiscalYear: func.EntityFiscalYear, message: {
                        $in: ['ActionType_Function_Title_Update', 'ActionType_Function_Title_Remove'
                            , 'ActionType_Title_Users_Update', 'ActionType_Title_Users_Remove', 'ActionType_Delete']
                    }
                }).sort({ 'modifiedOn': -1 }).limit(1).toArray();

                var funcNAData = db.event.find({ publisher: func.EntityId, actor: ObjectId(func.FunctionId), fiscalYear: func.EntityFiscalYear, message: 'ActionType_Function_NotApplicable' }).sort({ 'modifiedOn': -1 }).limit(1).toArray();

                var isFuncDelete = funcDeleteData.length > 0;
                var isFuncNA = funcNAData.length > 0;

                if (!(existingFunction) && (isFuncDelete || isFuncNA)) {
                    var firm = db.firm.findOne({ abbreviation: func.EntityId, fiscalYear: func.EntityFiscalYear });
                    func.EventType = 'Deleted';
                    func.EventAction = 'Update';
                    func.LastPublishedRecordStatus = 'Inactive';
                    func.EventDate = isFuncNA ? funcNAData[0].modifiedOn : funcDeleteData[0].modifiedOn;
                    func.LastProcessedDate = new Date().toISOString();
                    func.FirmPublishedDate = firm.publishedDate;
                    db.sqmarcherfunction.insertOne(func);
                }
            });

        })
        db.sqmarcherfunction.updateMany({ FirmPublishedDate: { $lte: new Date(ISODate().getTime() - calc7Days).toISOString() } }, { $set: { EventAction: '' } });
        db.sqmarcherfunctionmaster.drop();
        db.sqmarcherfunction.aggregate([
            { $merge: { into: "sqmarcherfunctionmaster" } }
        ]);

        //Drop temp collections
        db.sqmarcherkeycontroltemp.drop();
        db.sqmarcherqualityrisktemp.drop();
        db.sqmarcherqualityobjectivetemp.drop();
        db.sqmarcherresourcestemp.drop();
        db.sqmarcherrequirementcontroltemp.drop();
        db.sqmarcherfunctiontemp.drop();
        db.sqmarcherqualitysubrisktemp.drop();
        //Update isPublishQueryRun property
        //db.firm.updateMany({ abbreviation: { $in: publishedEntityIds }, { $or: [{ $eq: ['$isPublishQueryRun', false] }, { $and: [{ $eq: ['$isAutoPublished', true] },{ $eq: ['$isReadOnly', false] }]}]} }, { $set: { isPublishQueryRun: true, isAutoPublished: false } }, { multi: true })
        publishedEntityIds.forEach(function(f){
            db.firm.aggregate([
                {
                    $match: {
                        abbreviation: { $eq: f.abbreviation},
                        fiscalYear: { $eq :f.fiscalYear },
                        $expr: {
                            $and: [
                                { $eq: ['$isPublishQueryRun', false] },
                                {
                                    $or: [
                                        { $eq: ['$isAutoPublished', true] },
                                        { $eq: ['$isReadOnly', false] }
                                    ]
                                }
                            ]
                        }
                    }
                }
            ]).forEach(function (f) {
                var updateFields = { isPublishQueryRun: true };
                if (f.isAutoPublished) {
                    updateFields.isAutoPublished = false;
                }
                db.firm.updateOne({ _id: f._id }, { $set: updateFields });
            });
        })
    }
} catch (error) {
    db.sqmarcherkeycontroltemp.drop();
    db.sqmarcherqualityrisktemp.drop();
    db.sqmarcherqualityobjectivetemp.drop();
    db.sqmarcherresourcestemp.drop();
    db.sqmarcherrequirementcontroltemp.drop();
    db.sqmarcherfunctiontemp.drop();
    db.sqmarcherqualitysubrisktemp.drop();
    print("SYSTEM:Archer Error :: Error at Documentation  Query ", error);
    throw (error);
}
