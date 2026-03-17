try {
    var startTime = new Date();
    print("SYSTEM:Power-BI-Info:: Starting SQM Leadership Roles script at ", startTime.toISOString());

    db.sqmleadership.drop();

    var fiscalYearFilter = 2026;

    db.firm.aggregate([

        {
            $match: {
                fiscalYear: fiscalYearFilter,
                type: 'EntityType_MemberFirm'
            }
        },

        // STEP 1: Build assignedTitles
        {
            $addFields: {
                assignedTitles: {
                    $concatArrays: [

                        {
                            $map: {
                                input: { $ifNull: ["$ultimateResponsibility", []] },
                                as: "id",
                                in: { id: { $toString: "$$id" }, role: "ultimateResponsibility" }
                            }
                        },
                        {
                            $map: {
                                input: { $ifNull: ["$operationalResponsibilitySqm", []] },
                                as: "id",
                                in: { id: { $toString: "$$id" }, role: "operationalResponsibilitySqm" }
                            }
                        },
                        {
                            $cond: [
                                { $ne: ["$orIndependenceRequirement", null] },
                                [{ id: { $toString: "$orIndependenceRequirement" }, role: "orIndependenceRequirement" }],
                                []
                            ]
                        },
                        {
                            $cond: [
                                { $ne: ["$orMonitoringRemediation", null] },
                                [{ id: { $toString: "$orMonitoringRemediation" }, role: "orMonitoringRemediation" }],
                                []
                            ]
                        }
                    ]
                }
            }
        },

        // ✅ STEP 2: Inject default roles if empty
        {
            $addFields: {
                assignedTitles: {
                    $cond: [
                        { $gt: [{ $size: "$assignedTitles" }, 0] },
                        "$assignedTitles",
                        [
                            { id: "", role: "ultimateResponsibility" },
                            { id: "", role: "operationalResponsibilitySqm" },
                            { id: "", role: "orIndependenceRequirement" },
                            { id: "", role: "orMonitoringRemediation" }
                        ]
                    ]
                }
            }
        },

        // STEP 3: Unwind
        {
            $unwind: {
                path: "$assignedTitles",
                preserveNullAndEmptyArrays: false
            }
        },

        // STEP 4: Lookup
        {
            $lookup: {
                from: "titleFirmAssignments",
                let: {
                    tId: "$assignedTitles.id",
                    fy: "$fiscalYear",
                    fg: "$firmGroupId"
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$titleId", "$$tId"] },
                                    { $eq: ["$fiscalYear", "$$fy"] }
                                ]
                            }
                        }
                    }
                ],
                as: "leadership"
            }
        },

        // STEP 5: Select assignment or fallback
        {
            $addFields: {
                selectedLeadership: {
                    $let: {
                        vars: {
                            firmMatch: {
                                $filter: {
                                    input: "$leadership",
                                    as: "l",
                                    cond: { $eq: ["$$l.firmId", "$firmGroupId"] }
                                }
                            },
                            anyTitle: { $arrayElemAt: ["$leadership", 0] }
                        },
                        in: {
                            $cond: [
                                { $gt: [{ $size: "$$firmMatch" }, 0] },
                                { $arrayElemAt: ["$$firmMatch", 0] },
                                {
                                    titleName: { $ifNull: ["$$anyTitle.titleName", ""] },
                                    assignmentString: ""
                                }
                            ]
                        }
                    }
                }
            }
        },

        // STEP 6: Final output
        {
            $project: {
                _id: 0,
                memberFirmId: 1,
                fiscalYear: 1,
                country: 1,
                firmGroupId: 1,
                name: 1,
                type: 1,

                role: "$assignedTitles.role",
                leadershipId: {
                    $cond: [
                        { $or: [
                            { $eq: ["$assignedTitles.id", ""] },
                            { $eq: ["$assignedTitles.id", null] }
                        ]},
                        "",
                        { $toObjectId: "$assignedTitles.id" }
                    ]
                },

                title: { $ifNull: ["$selectedLeadership.titleName", ""] },
                assignment: { $ifNull: ["$selectedLeadership.assignmentString", ""] }
            }
        },

        { $sort: { fiscalYear: 1, memberFirmId: 1 } },

        { $out: "sqmleadership" }

    ], { allowDiskUse: true });

    var endTime = new Date();
    print("SYSTEM:Power-BI-Info:: Completed SQM Leadership Roles script at ", endTime.toISOString(),
        " Total execution time (minutes): ",
        (endTime.getTime() - startTime.getTime()) / 60000
    );

} catch (error) {
    print("SYSTEM:Power-BI-Error:: Error in SQM_Leadership_Roles ", error);
    throw error;
}