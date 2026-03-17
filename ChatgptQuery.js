var fiscalYearFilter = 2026;

db.firm.aggregate([

    {
        $match: {
            fiscalYear: fiscalYearFilter,
            type: 'EntityType_MemberFirm'
        }
    },

    // Step 1: Normalize all role IDs WITH role context
    {
        $addFields: {
            assignedTitles: {
                $concatArrays: [

                    // ultimateResponsibility
                    {
                        $map: {
                            input: { $ifNull: ["$ultimateResponsibility", []] },
                            as: "id",
                            in: { id: "$$id", role: "ultimateResponsibility" }
                        }
                    },

                    // operationalResponsibilitySqm
                    {
                        $map: {
                            input: { $ifNull: ["$operationalResponsibilitySqm", []] },
                            as: "id",
                            in: { id: "$$id", role: "operationalResponsibilitySqm" }
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

    // Step 2: explode roles (SAFE now because role is preserved)
    {
        $unwind: {
            path: "$assignedTitles",
            preserveNullAndEmptyArrays: false
        }
    },

    // Step 3: lookup title + assignments
    {
        $lookup: {
            from: "title",
            let: { titleId: "$assignedTitles.id", fy: "$fiscalYear" },
            pipeline: [
                {
                    $addFields: {
                        id_str: { $toString: "$_id" }
                    }
                },
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$id_str", "$$titleId"] },
                                { $eq: ["$fiscalYear", "$$fy"] }
                            ]
                        }
                    }
                },
                {
                    $lookup: {
                        from: "titleassignment",
                        localField: "id_str",
                        foreignField: "titleId",
                        as: "tas"
                    }
                },
                {
                    $unwind: {
                        path: "$tas",
                        preserveNullAndEmptyArrays: true
                    }
                },
                {
                    $addFields: {
                        firmAssignments: {
                            $reduce: {
                                input: {
                                    $map: {
                                        input: { $ifNull: ["$tas.assignments", []] },
                                        as: "a",
                                        in: {
                                            $concat: [
                                                "$$a.displayName",
                                                " (",
                                                "$$a.email",
                                                ")"
                                            ]
                                        }
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
                },
                {
                    $project: {
                        _id: 0,
                        titleId: "$id_str",
                        titleName: "$name",
                        firmId: "$tas.firmId",
                        firmAssignments: 1
                    }
                }
            ],
            as: "leadership"
        }
    },

    // Step 4: pick correct firm-specific assignment (fallback if not found)
    {
        $addFields: {
            selectedLeadership: {
                $let: {
                    vars: {
                        matched: {
                            $filter: {
                                input: "$leadership",
                                as: "l",
                                cond: { $eq: ["$$l.firmId", "$firmGroupId"] }
                            }
                        }
                    },
                    in: {
                        $cond: [
                            { $gt: [{ $size: "$$matched" }, 0] },
                            { $arrayElemAt: ["$$matched", 0] },
                            { $arrayElemAt: ["$leadership", 0] }
                        ]
                    }
                }
            }
        }
    },

    // Step 5: final clean output
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
            leadershipId: "$assignedTitles.id",

            title: "$selectedLeadership.titleName",
            assignment: "$selectedLeadership.firmAssignments"
        }
    },

    {
        $out: "sqmleadership"
    }

]);