try {
    var startTime = new Date();
    print("SYSTEM:Power-BI-Info:: Starting SQM Leadership Roles script at ", startTime.toISOString());

    db.sqmleadership.drop();

    var fiscalYearFilter = 2026;

    db.firm.aggregate([

        // STEP 1: Filter firms
        {
            $match: {
                fiscalYear: fiscalYearFilter,
                type: 'EntityType_MemberFirm'
            }
        },

        // STEP 2: Normalize all roles into one array
        {
            $addFields: {
                assignedTitles: {
                    $concatArrays: [

                        // ultimateResponsibility
                        {
                            $map: {
                                input: { $ifNull: ["$ultimateResponsibility", []] },
                                as: "id",
                                in: {
                                    id: { $toString: "$$id" },
                                    role: "ultimateResponsibility"
                                }
                            }
                        },

                        // operationalResponsibilitySqm
                        {
                            $map: {
                                input: { $ifNull: ["$operationalResponsibilitySqm", []] },
                                as: "id",
                                in: {
                                    id: { $toString: "$$id" },
                                    role: "operationalResponsibilitySqm"
                                }
                            }
                        },

                        // orIndependenceRequirement
                        {
                            $cond: [
                                { $ne: ["$orIndependenceRequirement", null] },
                                [{
                                    id: { $toString: "$orIndependenceRequirement" },
                                    role: "orIndependenceRequirement"
                                }],
                                []
                            ]
                        },

                        // orMonitoringRemediation
                        {
                            $cond: [
                                { $ne: ["$orMonitoringRemediation", null] },
                                [{
                                    id: { $toString: "$orMonitoringRemediation" },
                                    role: "orMonitoringRemediation"
                                }],
                                []
                            ]
                        }
                    ]
                }
            }
        },

        // STEP 3: Unwind roles (preserves multiple roles & titles)
        {
            $unwind: {
                path: "$assignedTitles",
                preserveNullAndEmptyArrays: false
            }
        },

        // STEP 4: Lookup from temp collection (NO firm filter here)
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

        // STEP 5: Select correct assignment OR fallback to title only
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
                            anyTitle: {
                                $arrayElemAt: ["$leadership", 0]
                            }
                        },
                        in: {
                            $cond: [
                                // ✅ Case 1: firm-specific assignment exists
                                { $gt: [{ $size: "$$firmMatch" }, 0] },
                                {
                                    $arrayElemAt: ["$$firmMatch", 0]
                                },

                                // ✅ Case 2: fallback → title only
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

        // STEP 6: Final projection
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
                        {
                            $or: [
                                { $eq: ["$assignedTitles.id", null] },
                                { $eq: ["$assignedTitles.id", ""] }
                            ]
                        },
                        "",
                        { $toObjectId: "$assignedTitles.id" }
                    ]
                },

                title: { $ifNull: ["$selectedLeadership.titleName", ""] },
                assignment: { $ifNull: ["$selectedLeadership.assignmentString", ""] }
            }
        },

        // STEP 7: Sort (optional but good for Power BI)
        {
            $sort: { fiscalYear: 1, memberFirmId: 1 }
        },

        // STEP 8: Output
        {
            $out: "sqmleadership"
        }

    ], { allowDiskUse: true });

    var endTime = new Date();
    print(
        "SYSTEM:Power-BI-Info:: Completed SQM Leadership Roles script at ",
        endTime.toISOString(),
        " Total execution time (minutes): ",
        (endTime.getTime() - startTime.getTime()) / 60000
    );

} catch (error) {
    print("SYSTEM:Power-BI-Error:: Error in SQM_Leadership_Roles ", error);
    throw error;
}