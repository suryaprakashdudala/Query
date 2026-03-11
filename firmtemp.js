try {

    db.firm.aggregate([
        {
            $match: {
                type: 'EntityType_MemberFirm',
                fiscalYear: 2026
            }
        },
        {
            $project: {
                _id: 0,
                memberFirmId: 1,
                fiscalYear: 1,
                country: 1,
                firmGroupId: 1,
                name: 1,
                type: 1,
                ultimate: {
                    $map: {
                        input: { $ifNull: ["$ultimateResponsibility", []] },
                        as: "u",
                        in: {
                            Role: "Ultimate Responsibility",
                            leadershipId: "$$u",
                            memberFirmId: "$memberFirmId",
                            fiscalYear: "$fiscalYear",
                            country: "$country",
                            firmGroupId: "$firmGroupId",
                            name: "$name",
                            type: "$type"
                        }
                    }
                },
                operational: {
                    $map: {
                        input: { $ifNull: ["$operationalResponsibilitySqm", []] },
                        as: "o",
                        in: {
                            Role: "Operational Responsibility",
                            leadershipId: "$$o",
                            memberFirmId: "$memberFirmId",
                            fiscalYear: "$fiscalYear",
                            country: "$country",
                            firmGroupId: "$firmGroupId",
                            name: "$name",
                            type: "$type"
                        }
                    }
                },
                compliance: {
                    $cond: [
                        { $ne: ["$orIndependenceRequirement", null] },
                        [{
                            Role: "Independence Requirement",
                            leadershipId: "$orIndependenceRequirement",
                            memberFirmId: "$memberFirmId",
                            fiscalYear: "$fiscalYear",
                            country: "$country",
                            firmGroupId: "$firmGroupId",
                            name: "$name",
                            type: "$type"
                        }],
                        []
                    ]
                },
                monitoring: {
                    $cond: [
                        { $ne: ["$orMonitoringRemediation", null] },
                        [{
                            Role: "Monitoring Remediation",
                            leadershipId: "$orMonitoringRemediation",
                            memberFirmId: "$memberFirmId",
                            fiscalYear: "$fiscalYear",
                            country: "$country",
                            firmGroupId: "$firmGroupId",
                            name: "$name",
                            type: "$type"
                        }],
                        []
                    ]
                }
            }
        },
        {
            $project: {
                combined: {
                    $concatArrays: [
                        "$ultimate",
                        "$operational",
                        "$compliance",
                        "$monitoring"
                    ]
                }
            }
        },
        {
            $unwind: "$combined"
        },
        {
            $replaceRoot: { newRoot: "$combined" }
        },
        {
            $lookup: {
                from: 'titleAssignedUsers',
                let: {
                    lId: "$leadershipId",
                    fId: "$firmGroupId"
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ["$id", "$$lId"] },
                                    { $eq: ["$firmId", "$$fId"] }
                                ]
                            }
                        }
                    }
                ],
                as: 'assignmentData'
            }
        },
        {
            $unwind: {
                path: "$assignmentData",
                preserveNullAndEmptyArrays: true
            }
        },
        {
            $project: {
                _id: 0,
                "memberFirmId": "$memberFirmId",
                "fiscalYear": "$fiscalYear",
                "country": "$country",
                "firmGroupId": "$firmGroupId",
                "name": "$name",
                "type": "$type",
                "role": "$Role",
                "leadershipId": "$leadershipId",
                "title": "$assignmentData.name",
                "assignment": {
                    $cond: [
                        { $and: [ { $ne: ["$assignmentData.assignedName", null] }, { $ne: ["$assignmentData.assignedEmail", null] } ] },
                        { $concat: ["$assignmentData.assignedName", " (", "$assignmentData.assignedEmail", ")"] },
                        { $ifNull: ["$assignmentData.assignedName", "$assignmentData.assignedEmail"] }
                    ]
                }
            }
        },
        {
            $out: "firmtemp"
        }
    ]);

} catch (error) {
    print("SYSTEM:Power-BI-Error:: Error in FirmTempQuery ", error);
    throw (error);
}
