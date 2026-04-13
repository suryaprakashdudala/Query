try{
    // db = db.getSiblingDB('isqc');

    var fiscalYearFilter = "2026";

    // Convert to integers if needed
    fiscalYearFilter = fiscalYearFilter.split(",").map(function(year) {
        return parseInt(year, 10);
    });

    // Fetch publishedEntityIds with fiscalYear for orConditions
        var publishedEntityIds = [ { abbreviation: 'NTW', fiscalYear: 2026 },{ abbreviation: 'USA', fiscalYear: 2026 },{ abbreviation: 'USLI', fiscalYear: 2026 } ]


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
//
// 2026-04-13T13:19:46.009+0000    exported 363 records
// SYSTEM:Archer Error :: Error at IEC Query  MongoServerError: PlanExecutor error during aggregation :: caused by :: $in requires an array as a second argument, found: missing
//     at Connection.sendCommand (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5630942)
//     at process.processTicksAndRejections (node:internal/process/task_queues:104:5)
//     at async Connection.command (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5631610)
//     at async Server.command (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5884443)
//     at async topology (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5834647)
//     at async t.executeOperation (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5832989)
//     at async AggregationCursor._initialize (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5718773)
//     at async AggregationCursor.cursorInit (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5713502)
//     at async AggregationCursor.fetchBatch (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5713962)
//     at async AggregationCursor.hasNext (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:13:5708781)
//     at async t.hasNext (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:158618)
//     at async t.hasNext (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:703202)
//     at async t.eval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:705699)
//     at async Proxy.aggregate (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:607359)
//     at async Proxy.aggregate (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:703202)
//     at async Proxy.eval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:705308)
//     at async Proxy.eval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:705699)
//     at async /publish/scripts/Sqm_Archer_KeyControl_IECs.js:575:30
//     at async ShellEvaluator.innerEval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:877974)
//     at async ShellEvaluator.customEval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:878162)
//     at async MongoshNodeRepl.eval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:956975)
//     at async MongoshNodeRepl.loadExternalCode (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:958408)
//     at async evaluate (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:957889)
//     at async n.load (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:835234)
//     at async n.load (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:703202)
//     at async n.eval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:705699)
//     at async MongoshNodeRepl.loadExternalFile (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:957984)
//     at async CliRepl.loadCommandLineFilesAndEval (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:913909)
//     at async CliRepl._start (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:910449)
//     at async CliRepl.start (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:906196)
//     at async w (eval at <anonymous> (node:lib-boxednode/mongosh:103:20), <anonymous>:145:972933) {
//   errorLabelSet: Set(0) {},
//   errorResponse: {
//     ok: 0,
//     errmsg: 'PlanExecutor error during aggregation :: caused by :: $in requires an array as a second argument, found: missing',
//     code: 40081,
//     codeName: 'Location40081',
//     '$clusterTime': {
//       clusterTime: Timestamp({ t: 1776086388, i: 17 }),
//       signature: {
//         hash: Binary.createFromBase64('8RnuXdk9fCBvSuI4PsttASxpcNE=', 0),
//         keyId: Long('7595641476768333834')
//       }
//     },
//     operationTime: Timestamp({ t: 1776086387, i: 1712 })
//   },
//   ok: 0,
//   code: 40081,
//   codeName: 'Location40081',
//   '$clusterTime': {
//     clusterTime: Timestamp({ t: 1776086388, i: 17 }),
//     signature: {
//       hash: Binary.createFromBase64('8RnuXdk9fCBvSuI4PsttASxpcNE=', 0),
//       keyId: Long('7595641476768333834')
//     }
//   },
//   operationTime: Timestamp({ t: 1776086387, i: 1712 })
// }
// MongoServerError: PlanExecutor error during aggregation :: caused by :: $in requires an array as a second argument, found: missing