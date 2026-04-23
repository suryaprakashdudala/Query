try{
    db = db.getSiblingDB('isqc');

    var fiscalYearFilter = process.env.FiscalYears;

    // Convert to integers if needed
    fiscalYearFilter = fiscalYearFilter.split(",").map(function(year) {
        return parseInt(year, 10);
    });

    // Fetch publishedEntityIds with fiscalYear for orConditions
    var publishedEntityIds = db.firm.aggregate([
        {
            $match: {
                $expr: {
                    $and: [
                        '$publishedDate',
                        { $ne: ['$publishedDate', ''] },
                        { $and: [
                            { $eq: ['$isPublishQueryRun', false] },
                            { $or: [
                                { $eq: ['$isAutoPublished', true] },
                                { $eq: ['$isReadOnly', false] }
                            ]}
                        ]},
                        { $in: ["$fiscalYear", fiscalYearFilter]},
                        { $or: [
                            { $eq: ["$isRollForwardedFromPreFY" , true]},
                            { $eq: ["$isCreatedInCurrentFY" , true]}
                        ]}
                    ]
                }
            }
        },
        { $project:{
            abbreviation:1,
            _id: 0,
            fiscalYear:1
        }}
    ]).toArray();

    if (publishedEntityIds.length > 0) {
         // Build orConditions for (firmId, fiscalYear) pairs
    var orConditions = publishedEntityIds.map(entity => ({
        firmId: entity.abbreviation,
        fiscalYear: entity.fiscalYear
    }));

    db.documentation.aggregate([
        {
            $match: {
                type: "Resource",
                fiscalYear: { $in: fiscalYearFilter } 
            }
        },
        {
            $project: {
                _id: 0,
                resource_id: { $toString: "$_id" },
                resource_name: "$name"
            }
        },
        {
            $out: "documentation_resource"
        }
    ]);

    db.documentation.aggregate([
        {
            $match: {
                $and: [
                    { type: { $in: ["KeyControl", "RequirementControlAssignment"] } },
                    { $or: orConditions }
                ]
            }
        },
        { $unwind: { path: "$addInformationExecutionControls", preserveNullAndEmptyArrays: true } },
        { $unwind: { path: "$addInformationExecutionControls.id", preserveNullAndEmptyArrays: true } },
        {
            $lookup: {
                from: 'keycontrolresource',
                let: {
                reqCtrlAddIpeCategory: '$addInformationExecutionControls.ipeCategory'
                },
                pipeline: [
                {
                    $match: {
                    $expr: {
                        $and: [
                        {
                            $in: [
                            { $toString: '$_id' },
                            {
                                $cond: {
                                if: { $isArray: '$$reqCtrlAddIpeCategory' },
                                then: '$$reqCtrlAddIpeCategory',
                                else: ['$$reqCtrlAddIpeCategory']
                                }
                            }
                            ]
                        },
                        {
                            $gt: [
                            {
                                $size: {
                                    $setIntersection: ['$fiscalYear', fiscalYearFilter]
                                }
                            },
                            0
                            ]
                        }
                        ]
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
                as: 'ipeCategory'
            }
        },
        {
            $lookup: {
                from: 'keycontrolresource',
                let: {
                    reqCtrlAddIecReports: '$addInformationExecutionControls.iecReports' 
                },
                pipeline: [
                {
                    $match: {
                    $expr: {
                        $and: [
                        {
                            $in: [
                            { $toString: '$_id' },
                            {
                                $cond: {
                                if: { $isArray: '$$reqCtrlAddIecReports' },
                                then: '$$reqCtrlAddIecReports',
                                else: ['$$reqCtrlAddIecReports']
                                }
                            }
                            ]
                        },
                        {
                            $gt: [
                            {
                                $size: {
                                    $setIntersection: ['$fiscalYear', fiscalYearFilter]
                                }
                            },
                            0
                            ]
                        }
                        ]
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
                as: 'iecReportsOrOther'
            }
        },
        {
            $lookup: {
                from: 'documentation_resource',
                let: {
                    reqCtrlIECSystems: '$addInformationExecutionControls.ipeSystems' 
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $in: ["$resource_id", "$$reqCtrlIECSystems"] }
                            ]
                        }
                    }
                }],
                as: 'iecSystemsResource'
            }
        },
        {
            $project: {
                _id: 0,
                UniqueControlId:{ $cond: { if: { $eq: ['$type','KeyControl'] }, then: '$uniqueId', else: { $concat: ['$firmId', '-', '$uniqueId'] } } },
                EntityId:'$firmId',
                ControlName:'$controlName',
                FiscalYear: { $concat: ["FY", { $substr: ["$fiscalYear", 2, 3] }] },
                IECId: { $concat: ['"', "$addInformationExecutionControls.id", '"']},
                IEC: "$addInformationExecutionControls.name",
                IECLocal: "$addInformationExecutionControls.nameAlt",
                IpeSystemsApplicable: "$addInformationExecutionControls.ipeSystemsApplicable",
                iecReports: "$addInformationExecutionControls.iecReports",
                IsThisIECReportOrOther: {
                    $reduce: {
                        input: '$iecReportsOrOther',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                    }
                },
                ipeCategory: "$addInformationExecutionControls.ipeCategory",
                IECReportType: {
                    $reduce: {
                        input: '$ipeCategory',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
                    }
                },
                ControlOwnerProceduresOverCompleteness: "$addInformationExecutionControls.iecOwnerProcedures",
                ControlOwnerProceduresOverCompletenessLocal: "$addInformationExecutionControls.iecOwnerProceduresAlt",
                IECRationale: "$iecRationale",
                IECRationaleLocal: "$iecRationaleAlt",
                ipeSystems: "$addInformationExecutionControls.ipeSystems",
                IECSystems: { $cond: { if: { $eq: ['$controlType','ControlType_ItDependentManual'] }, then: {
                    $reduce: {
                        input: {
                            $cond: {
                                if: { $gt: [{ $size: { $ifNull: ['$addInformationExecutionControls.ipeSystems', []] } }, 0] },
                                then: '$iecSystemsResource',
                                else: [{ resource_name: 'N/A' }]
                            }
                        },
                        initialValue: '',
                        in: { $concat: [ '$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.resource_name' ] }
                    }
                }, else: '' } },
                firmId: 1,
                uniqueId: 1,
                type: 1,
                EntityFiscalYear: "$fiscalYear",
                id: { $toString: "$_id" }
            }
        },
        { $out: "sqmarcherkeycontrol_iec" }
    ]);
    db.documentation_resource.drop();
    }
    else {
         print("No publishedEntityIds found for the given fiscal year filter.");
    }
} catch (error) {
    print("SYSTEM:Archer Error :: Error at IEC Query ", error);        
    db.documentation_resource.drop();
    throw (error);
}