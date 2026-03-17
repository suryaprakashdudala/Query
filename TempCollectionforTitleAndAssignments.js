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
        { $unwind: { path: "$tas", preserveNullAndEmptyArrays: true } },
        {
            $addFields: {
                _id: 0,
                titleIdS: "$id_str", 
                fiscalYear: "$fiscalYear",
                titleNameS: "$name",
                firmIdS: { $ifNull: ["$tas.firmId", null] },
                firmAssignmentsS: {
                    $reduce: {
                            input: {
                                $map: {
                                    input: { $ifNull: ["$tas.assignments", []] },
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
            },
        },
        {
            $project: {
                 _id: 0,
                titleId: "$titleIdS",
                fiscalYear: "$fiscalYear",
                titleName: "$titleNameS",
                firmId: "$firmIdS",
                firmAssignments: "$firmAssignmentsS"
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
            $project:{
                _id: 0,
                memberFirmId: "$memberFirmId",
                country: 1,
                firmGroupId: 1,
                name: 1,
                type: 1,
                fiscalYear: 1,
                ultimateResponsibility: "$ultimateResponsibility",
                operationalResponsibilitySqm: "$operationalResponsibilitySqm",
                orIndependenceRequirement: "$orIndependenceRequirement",
                orMonitoringRemediation: "$orMonitoringRemediation"
            }
        },
        {
            $addFields: {
                assignedTitleIds: {
                    $concatArrays: [
                        { $ifNull: ["$ultimateResponsibility", []] },
                        { $ifNull: ["$operationalResponsibilitySqm", []] },
                        { $cond: [{ $ne: ["$orIndependenceRequirement", null] }, [{$toString: "$orIndependenceRequirement"}], []] },
                        { $cond: [{ $ne: ["$orMonitoringRemediation", null] }, [{$toString: "$orMonitoringRemediation"}], []] }
                    ]
                }
            }
        },
        {$unwind: { path: "$assignedTitleIds", preserveNullAndEmptyArrays: true } },
        {
            $lookup: {
                from: 'titleAndAssignments',
                let: { ids: "$assignedTitleIds", fy: "$fiscalYear" },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$titleId", "$$ids"] },
                                    // { $eq: ["$fiscalYear", "$$fy"] }
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
                        cond: { $in: ["$$l.titleId", "$ultimateResponsibility"] }
                    }
                },
                operationalResponsibilitySqm: {
                    $filter: {
                        input: "$leadership",
                        as: "l",
                        cond: { $in: ["$$l.titleId", "$operationalResponsibilitySqm"] }
                    }
                },
                orIndependenceRequirement: {
                    $filter: {
                        input: "$leadership",
                        as: "l",
                        cond: { $in: ["$$l.titleId", { $cond: [{ $ne: ["$orIndependenceRequirement", null] }, [{$toString: "$orIndependenceRequirement"}], []] }] }
                    }
                },
                orMonitoringRemediation: {
                    $filter: {
                        input: "$leadership",
                        as: "l",
                        cond: { $in: ["$$l.titleId", { $cond: [{ $ne: ["$orMonitoringRemediation", null] }, [{$toString: "$orMonitoringRemediation"}], []] }] }
                    }
                }
            }
        },
        {
            $addFields: {
                ultimateResponsibilityResolved: {
                    $let: {
                        vars: {
                            matchedAssignment: {
                                $filter: {
                                    input: "$ultimateResponsibility",
                                    cond: { $eq: ["$$this.firmId", "$firmGroupId"] }
                                }
                            }
                        },
                        in: {
                            $let: {
                                vars: {
                                    selectedAssignment: {
                                        $cond: [
                                            { $gt: [{ $size: "$$matchedAssignment" }, 0] },
                                            { $arrayElemAt: ["$$matchedAssignment", 0] },
                                            { $arrayElemAt: ["$ultimateResponsibility", 0] }
                                        ]
                                    }
                                },
                                in: {
                                    memberFirmId: "$memberFirmId",
                                    fiscalYear: "$fiscalYear",
                                    country: "$country",
                                    firmGroupId: "$firmGroupId",
                                    name: "$name",
                                    type: "$type",
                                    role: "ultimateResponsibility",
                                    leadershipId: "$assignedTitleIds",
                                    title: "$$selectedAssignment.titleName",
                                    assignment: "$$matchedAssignment.firmAssignments"
                                }
                            }
                        }
                    }
                },
                // operationalResponsibilitySqmResolved: {
                //     $let: {
                //         vars: {
                //             matchedAssignment: {
                //                 $filter: {
                //                     input: "$operationalResponsibilitySqm",
                //                     cond: { $eq: ["$$this.firmId", "$firmGroupId"] }
                //                 }
                //             }
                //         },
                //         in: {
                //             $cond: [
                //                 { $gt: [{ $size: "$$matchedAssignment" }, 0] },
                //                 { $arrayElemAt: ["$$matchedAssignment", 0] },
                //                 { $arrayElemAt: ["$operationalResponsibilitySqm", 0] }
                //             ]
                //         }
                //     }
                // },
                // orIndependenceRequirementResolved: {
                //     $let: {
                //         vars: {
                //             matchedAssignment: {
                //                 $filter: {
                //                     input: "$orIndependenceRequirement",
                //                     cond: { $eq: ["$$this.firmId", "$firmGroupId"] }
                //                 }
                //             }
                //         },
                //         in: {
                //             $cond: [
                //                 { $gt: [{ $size: "$$matchedAssignment" }, 0] },
                //                 { $arrayElemAt: ["$$matchedAssignment", 0] },
                //                 { $arrayElemAt: ["$orIndependenceRequirement", 0] }
                //             ]
                //         }
                //     }
                // },
                // orMonitoringRemediationResolved: {
                //     $let: {
                //         vars: {
                //             matchedAssignment: {
                //                 $filter: {
                //                     input: "$orMonitoringRemediation",
                //                     cond: { $eq: ["$$this.firmId", "$firmGroupId"] }
                //                 }
                //             }
                //         },
                //         in: {
                //             $cond: [
                //                 { $gt: [{ $size: "$$matchedAssignment" }, 0] },
                //                 { $arrayElemAt: ["$$matchedAssignment", 0] },
                //                 { $arrayElemAt: ["$orMonitoringRemediation", 0] }
                //             ]
                //         }
                //     }
                // },
            }
        },
        {
            $project: {
                // memberFirmId: "$memberFirmId",
                // country: "$country",
                // firmGroupId: "$firmGroupId",
                // name: "$name",
                // type: "$type",
                // fiscalYear: "$fiscalYear",
                // ultimateResponsibility: 1,
                // operationalResponsibilitySqm: 1,
                // orIndependenceRequirement: 1,
                // orMonitoringRemediation: 1,
                // leadership: 1,
                ultimateResponsibilityResolved:1,
                // operationalResponsibilitySqmResolved:1,
                // orIndependenceRequirementResolved:1,
                // orMonitoringRemediationResolved:1,
                // assignedTitleIds:"$assignedTitleIds",

            }
        },
        { $out: 'sqmleadership' }
    ])