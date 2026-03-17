var fiscalYearFilter = 2026;

db.sqmleadership.drop();

db.firm.aggregate([

    {
        $match: {
            fiscalYear: fiscalYearFilter,
            type: 'EntityType_MemberFirm'
        }
    },

    // STEP 1: create role-aware IDs
    {
        $addFields: {
            assignedTitles: {
                $concatArrays: [

                    {
                        $map: {
                            input: { $ifNull: ["$ultimateResponsibility", []] },
                            as: "id",
                            in: { id: "$$id", role: "ultimateResponsibility" }
                        }
                    },
                    {
                        $map: {
                            input: { $ifNull: ["$operationalResponsibilitySqm", []] },
                            as: "id",
                            in: { id: "$$id", role: "operationalResponsibilitySqm" }
                        }
                    },
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

    // STEP 2: explode titles
    {
        $unwind: "$assignedTitles"
    },

    // STEP 3: lookup into pre-expanded collection
    {
        $lookup: {
            from: "titleAndAssignments",
            let: {
                id: "$assignedTitles.id",
                fy: "$fiscalYear"
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$titleId", "$$id"] },
                                { $eq: ["$fiscalYear", "$$fy"] }
                            ]
                        }
                    }
                }
            ],
            as: "leadership"
        }
    },

    // 🔥 CRITICAL STEP → THIS CREATES MULTIPLE ROWS
    {
        $unwind: {
            path: "$leadership",
            preserveNullAndEmptyArrays: true
        }
    },

    // STEP 4: keep only matching firm OR allow null fallback
    {
        $match: {
            $expr: {
                $or: [
                    { $eq: ["$leadership.firmId", "$firmGroupId"] },
                    { $eq: ["$leadership.firmId", null] }
                ]
            }
        }
    },

    // STEP 5: final projection
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
            assignment: "$leadership.firmAssignments"
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