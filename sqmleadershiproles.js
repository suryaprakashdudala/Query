try{
    // var db = db.getSiblingDB('isqc');

    var startTime = new Date();
    print("SYSTEM:Power-BI-Info:: Starting SQM Leadership Roles script at ", startTime.toISOString());
    db.sqmleadership.drop();

    var fiscalYearFilter = 2026;
    db.title.aggregate([
        { $match: { fiscalYear: fiscalYearFilter } },
        { $addFields: { id_str: { $toString: "$_id" } } },
        {
            $lookup: {
                from: 'titleassignment',
                localField: 'id_str',
                foreignField: 'titleId',
                as: 'tas'
            }
        },
        {
            $project: {
                _id: 0,
                titleId: "$id_str",
                fiscalYear: 1,
                titleName: "$name",
                firmId: "$tas.firmId",
                firmAssignments: {
                    $map: {
                        input: "$tas",
                        as: "t",
                        in: {
                            firmId: "$$t.firmId",
                            assignmentString: {
                                $reduce: {
                                    input: {
                                        $map: {
                                            input: { $ifNull: ["$$t.assignments", []] },
                                            as: "a",
                                            in: { $concat: ["$$a.displayName", "(", "$$a.email", ")"] }
                                        }
                                    },
                                    initialValue: "",
                                    in: {
                                        $cond: [
                                            { $eq: ["$$value", ""] },
                                            "$$this",
                                            { $concat: ["$$value", "; ", "$$this"] }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        { $out: "titleAndAssignments" }
    ]);

    db.titleAndAssignments.aggregate([
      { $unwind: "$firmAssignments" },
      {
        $project: {
          _id: 0,
          titleId: 1,
          fiscalYear: 1,
          titleName: 1,
          firmId: "$firmAssignments.firmId",
          assignmentString: "$firmAssignments.assignmentString"
        }
      },
      { $out: "titleFirmAssignments" }
    ]);

    db.titleAndAssignments.createIndex({ titleId: 1, fiscalYear: 1 });

    db.firm.aggregate([
        {
            $match: {
                fiscalYear: fiscalYearFilter,
                type: 'EntityType_MemberFirm'
            }
        },
        {
            $addFields: {
                lookupIds: {
                    $setUnion: [
                        { $ifNull: ["$ultimateResponsibility", []] },
                        { $ifNull: ["$operationalResponsibilitySqm", []] },
                        { $cond: [{ $ne: ["$orIndependenceRequirement", null] }, [{$toString: "$orIndependenceRequirement"}], []] },
                        { $cond: [{ $ne: ["$orMonitoringRemediation", null] }, [{$toString: "$orMonitoringRemediation"}], []] }
                    ]
                }
            }
        },
        {
            $lookup: {
                from: 'titleFirmAssignments',
                let: { ids: "$lookupIds", fy: "$fiscalYear" },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $in: ["$titleId", "$$ids"] },
                                    { $eq: ["$fiscalYear", "$$fy"] }
                                ]
                            }
                        }
                    }
                ],
                as: 'leadership'
            }
        },
        {
            $addFields: {
                ultimateResponsibility: {
                    $map: {
                        input: { $ifNull: ["$ultimateResponsibility", []] },
                        as: "tId",
                        in: {
                            $let: {
                                vars: {
                                    strId: { $toString: "$$tId" },
                                    matchingRecs: {
                                        $filter: { input: "$leadership", as: "l", cond: { $eq: ["$$l.titleId", { $toString: "$$tId" }] } }
                                    }
                                },
                                in: {
                                    titleId: "$$strId",
                                    titleName: {
                                        $let: { vars: { firstMatch: { $arrayElemAt: ["$$matchingRecs", 0] } }, in: { $ifNull: ["$$firstMatch.titleName", ""] } }
                                    },
                                    assignmentString: {
                                        $let: { vars: { firmMatch: { $arrayElemAt: [ { $filter: { input: "$$matchingRecs", as: "m", cond: { $eq: ["$$m.firmId", "$firmGroupId"] } } }, 0 ] } }, in: { $ifNull: ["$$firmMatch.assignmentString", ""] } }
                                    }
                                }
                            }
                        }
                    }
                },
                operationalResponsibilitySqm: {
                    $map: {
                        input: { $ifNull: ["$operationalResponsibilitySqm", []] },
                        as: "tId",
                        in: {
                            $let: {
                                vars: {
                                    strId: { $toString: "$$tId" },
                                    matchingRecs: {
                                        $filter: { input: "$leadership", as: "l", cond: { $eq: ["$$l.titleId", { $toString: "$$tId" }] } }
                                    }
                                },
                                in: {
                                    titleId: "$$strId",
                                    titleName: {
                                        $let: { vars: { firstMatch: { $arrayElemAt: ["$$matchingRecs", 0] } }, in: { $ifNull: ["$$firstMatch.titleName", ""] } }
                                    },
                                    assignmentString: {
                                        $let: { vars: { firmMatch: { $arrayElemAt: [ { $filter: { input: "$$matchingRecs", as: "m", cond: { $eq: ["$$m.firmId", "$firmGroupId"] } } }, 0 ] } }, in: { $ifNull: ["$$firmMatch.assignmentString", ""] } }
                                    }
                                }
                            }
                        }
                    }
                },
                orIndependenceRequirement: {
                    $map: {
                        input: { $cond: [{ $ne: ["$orIndependenceRequirement", null] }, ["$orIndependenceRequirement"], []] },
                        as: "tId",
                        in: {
                            $let: {
                                vars: {
                                    strId: { $toString: "$$tId" },
                                    matchingRecs: {
                                        $filter: { input: "$leadership", as: "l", cond: { $eq: ["$$l.titleId", { $toString: "$$tId" }] } }
                                    }
                                },
                                in: {
                                    titleId: "$$strId",
                                    titleName: {
                                        $let: { vars: { firstMatch: { $arrayElemAt: ["$$matchingRecs", 0] } }, in: { $ifNull: ["$$firstMatch.titleName", ""] } }
                                    },
                                    assignmentString: {
                                        $let: { vars: { firmMatch: { $arrayElemAt: [ { $filter: { input: "$$matchingRecs", as: "m", cond: { $eq: ["$$m.firmId", "$firmGroupId"] } } }, 0 ] } }, in: { $ifNull: ["$$firmMatch.assignmentString", ""] } }
                                    }
                                }
                            }
                        }
                    }
                },
                orMonitoringRemediation: {
                    $map: {
                        input: { $cond: [{ $ne: ["$orMonitoringRemediation", null] }, ["$orMonitoringRemediation"], []] },
                        as: "tId",
                        in: {
                            $let: {
                                vars: {
                                    strId: { $toString: "$$tId" },
                                    matchingRecs: {
                                        $filter: { input: "$leadership", as: "l", cond: { $eq: ["$$l.titleId", { $toString: "$$tId" }] } }
                                    }
                                },
                                in: {
                                    titleId: "$$strId",
                                    titleName: {
                                        $let: { vars: { firstMatch: { $arrayElemAt: ["$$matchingRecs", 0] } }, in: { $ifNull: ["$$firstMatch.titleName", ""] } }
                                    },
                                    assignmentString: {
                                        $let: { vars: { firmMatch: { $arrayElemAt: [ { $filter: { input: "$$matchingRecs", as: "m", cond: { $eq: ["$$m.firmId", "$firmGroupId"] } } }, 0 ] } }, in: { $ifNull: ["$$firmMatch.assignmentString", ""] } }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        },
        {
            $addFields: {
                combined: {
                    $concatArrays: [
                        {
                            $cond: [
                                { $gt: [{ $size: "$ultimateResponsibility" }, 0] },
                                { 
                                    $map: {
                                        input: "$ultimateResponsibility",
                                        as: "r",
                                        in: {
                                            role: "ultimateResponsibility",
                                            title: "$$r.titleName",
                                            assignment: "$$r.assignmentString",
                                            fiscalYear: "$fiscalYear",
                                            memberFirmId: "$memberFirmId",
                                            country: "$country",
                                            firmGroupId: "$firmGroupId",
                                            name: "$name",
                                            type: "$type",
                                            leadershipId: { $toObjectId: "$$r.titleId" }
                                        }
                                    } 
                                },
                                [{ role: "ultimateResponsibility", title: "", assignment: "", fiscalYear: "$fiscalYear", memberFirmId: "$memberFirmId", country: "$country", firmGroupId: "$firmGroupId", name: "$name", type: "$type" }]
                            ]
                        },
                        {
                            $cond: [
                                { $gt: [{ $size: "$operationalResponsibilitySqm" }, 0] },
                                { 
                                    $map: {
                                        input: "$operationalResponsibilitySqm",
                                        as: "r",
                                        in: {
                                            role: "operationalResponsibilitySqm",
                                            title: "$$r.titleName",
                                            assignment: "$$r.assignmentString",
                                            fiscalYear: "$fiscalYear",
                                            memberFirmId: "$memberFirmId",
                                            country: "$country",
                                            firmGroupId: "$firmGroupId",
                                            name: "$name",
                                            type: "$type",
                                            leadershipId: { $toObjectId: "$$r.titleId" }
                                        }
                                    } 
                                },
                                [{ role: "operationalResponsibilitySqm", title: "", assignment: "", fiscalYear: "$fiscalYear", memberFirmId: "$memberFirmId", country: "$country", firmGroupId: "$firmGroupId", name: "$name", type: "$type" }]
                            ]
                        },
                        { 
                            $cond: [
                                { $gt: [{ $size: "$orIndependenceRequirement" }, 0] },
                                {
                                    $map: {
                                        input: "$orIndependenceRequirement",
                                        as: "r",
                                        in: {
                                            role: "orIndependenceRequirement",
                                            title: "$$r.titleName",
                                            assignment: "$$r.assignmentString",
                                            fiscalYear: "$fiscalYear",
                                            memberFirmId: "$memberFirmId",
                                            country: "$country",
                                            firmGroupId: "$firmGroupId",
                                            name: "$name",
                                            type: "$type",
                                            leadershipId: { $toObjectId: "$$r.titleId" }
                                        }
                                    } 
                                },
                                [{ role: "orIndependenceRequirement", title: "", assignment: "", fiscalYear: "$fiscalYear", memberFirmId: "$memberFirmId", country: "$country", firmGroupId: "$firmGroupId", name: "$name", type: "$type" }]
                            ]
                        },
                        { 
                            $cond: [
                                { $gt: [{ $size: "$orMonitoringRemediation" }, 0] },
                                {    
                                    $map: {
                                        input: "$orMonitoringRemediation",
                                        as: "r",
                                        in: {
                                            role: "orMonitoringRemediation",
                                            title: "$$r.titleName",
                                            assignment: "$$r.assignmentString",
                                            fiscalYear: "$fiscalYear",
                                            memberFirmId: "$memberFirmId",
                                            country: "$country",
                                            firmGroupId: "$firmGroupId",
                                            name: "$name",
                                            type: "$type",
                                            leadershipId: { $toObjectId: "$$r.titleId" }
                                        }
                                    } 
                                },
                                [{ role: "orMonitoringRemediation", title: "", assignment: "", fiscalYear: "$fiscalYear", memberFirmId: "$memberFirmId", country: "$country", firmGroupId: "$firmGroupId", name: "$name", type: "$type" }]
                            ]
                        }
                    ]
                }
            }
        },
        { $unwind: "$combined" },
        { $replaceRoot: { newRoot: "$combined" } },
        { $sort: { fiscalYear: 1, memberFirmId: 1 } },
        { $out: 'sqmleadership' }
    ], { allowDiskUse: true });
    
    // db.titleAndAssignments.drop();

    var endTime = new Date();
    print("SYSTEM:Power-BI-Info:: Completed SQM Leadership Roles script at ", endTime.toISOString(), " Total execution time (minutes): ", (endTime.getTime() - startTime.getTime())/60000);
}
catch(error){
    print("SYSTEM:Power-BI-Error:: Error in SQM_Leadership_Roles ", error);
    throw (error);
}
