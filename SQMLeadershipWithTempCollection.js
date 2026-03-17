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
                from: 'titleAndAssignments',
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
                    $filter: {
                        input: "$leadership",
                        as: "l",
                        cond: { $in: ["$$l.titleId", { $map: { input: { $ifNull: ["$ultimateResponsibility", []] }, as: "id", in: { $toString: "$$id" } } }] }
                    }
                },
                operationalResponsibilitySqm: {
                    $filter: {
                        input: "$leadership",
                        as: "l",
                        cond: { $in: ["$$l.titleId", { $map: { input: { $ifNull: ["$operationalResponsibilitySqm", []] }, as: "id", in: { $toString: "$$id" } } }] }
                    }
                },
                orIndependenceRequirement: {
                    $filter: {
                        input: "$leadership",
                        as: "l",
                        cond: { $eq: ["$$l.titleId", { $toString: "$orIndependenceRequirement" }] }
                    }
                },
                orMonitoringRemediation: {
                    $filter: {
                        input: "$leadership",
                        as: "l",
                        cond: { $eq: ["$$l.titleId", { $toString: "$orMonitoringRemediation" }] }
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
                                            assignment: {
                                                $let: {
                                                    vars: {
                                                        fa: {
                                                            $arrayElemAt: [
                                                                {
                                                                    $filter: {
                                                                        input: "$$r.firmAssignments",
                                                                        as: "fa",
                                                                        cond: { $eq: ["$$fa.firmId", "$firmGroupId"] }
                                                                    }
                                                                },
                                                                0
                                                            ]
                                                        }
                                                    },
                                                    in: "$$fa.assignmentString"
                                                }
                                            },
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
                                            assignment: {
                                                $let: {
                                                    vars: {
                                                        fa: {
                                                            $arrayElemAt: [
                                                                {
                                                                    $filter: {
                                                                        input: "$$r.firmAssignments",
                                                                        as: "fa",
                                                                        cond: { $eq: ["$$fa.firmId", "$firmGroupId"] }
                                                                    }
                                                                },
                                                                0
                                                            ]
                                                        }
                                                    },
                                                    in: "$$fa.assignmentString"
                                                }
                                            },
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
                                            assignment: {
                                                $let: {
                                                    vars: {
                                                        fa: {
                                                            $arrayElemAt: [
                                                                {
                                                                    $filter: {
                                                                        input: "$$r.firmAssignments",
                                                                        as: "fa",
                                                                        cond: { $eq: ["$$fa.firmId", "$firmGroupId"] }
                                                                    }
                                                                },
                                                                0
                                                            ]
                                                        }
                                                    },
                                                    in: "$$fa.assignmentString"
                                                }
                                            },
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
                                            assignment: {
                                                $let: {
                                                    vars: {
                                                        fa: {
                                                            $arrayElemAt: [
                                                                {
                                                                    $filter: {
                                                                        input: "$$r.firmAssignments",
                                                                        as: "fa",
                                                                        cond: { $eq: ["$$fa.firmId", "$firmGroupId"] }
                                                                    }
                                                                },
                                                                0
                                                            ]
                                                        }
                                                    },
                                                    in: "$$fa.assignmentString"
                                                }
                                            },
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
        { $out: 'memberFirmTitles' }
    ], { allowDiskUse: true });
    
    // db.titleAndAssignments.drop();

    var endTime = new Date();
    print("SYSTEM:Power-BI-Info:: Completed SQM Leadership Roles script at ", endTime.toISOString(), " Total execution time (minutes): ", (endTime.getTime() - startTime.getTime())/60000);
}
catch(error){
    print("SYSTEM:Power-BI-Error:: Error in SQM_Leadership_Roles ", error);
    throw (error);
}
