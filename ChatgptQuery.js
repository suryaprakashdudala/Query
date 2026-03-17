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
                        assignmentString: {
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
                        assignmentString: 1
                    }
                }
            ],
            as: "leadership"
        }
    },

    // STEP 4: explode leadership → THIS restores missing records
    {
        $unwind: {
            path: "$leadership",
            preserveNullAndEmptyArrays: true
        }
    },

    // STEP 5: filter only relevant firm OR fallback
    {
        $addFields: {
            isMatchingFirm: {
                $eq: ["$leadership.firmId", "$firmGroupId"]
            }
        }
    },

    // STEP 6: prefer matching firm, but allow fallback
    {
        $group: {
            _id: {
                firm: "$_id",
                role: "$assignedTitles.role",
                titleId: "$assignedTitles.id"
            },
            doc: { $first: "$$ROOT" },
            matching: {
                $push: {
                    $cond: ["$isMatchingFirm", "$$ROOT", "$$REMOVE"]
                }
            }
        }
    },

    {
        $addFields: {
            selected: {
                $cond: [
                    { $gt: [{ $size: "$matching" }, 0] },
                    { $arrayElemAt: ["$matching", 0] },
                    "$doc"
                ]
            }
        }
    },

    {
        $replaceRoot: { newRoot: "$selected" }
    },

    // STEP 7: final output
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

            title: "$leadership.titleName",
            assignment: "$leadership.assignmentString"
        }
    },

    {
        $sort: {
            fiscalYear: 1,
            memberFirmId: 1
        }
    },

    {
        $out: "sqmleadership"
    }

], { allowDiskUse: true });