try{
    // var db = db.getSiblingDB('isqc');

    var startTIme = new Date();
    print("SYSTEM:Power-BI-Info:: Starting SQM Leadership Roles script at ", startTIme.toISOString());
    db.sqmleadership.drop();

    var fiscalYearFilter = 2026;

    db.firm.aggregate([
        {
            $match: {
                fiscalYear:fiscalYearFilter,
                type: 'EntityType_MemberFirm'
            }
        },
        {
            $lookup: {
                from: 'title',
                let: {
                    ultimateResponsibility: '$ultimateResponsibility',
                    firmId: '$firmGroupId',
                    titleFiscalYear: '$fiscalYear'
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $in: [{ $toString: '$_id' }, { $ifNull: ['$$ultimateResponsibility', []] }] },
                                    { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                                ]
                            }
                        }
                    },
                    {
                        $lookup: {
                            from: 'titleassignment',
                            let: {
                                titleId: { $toString: '$_id' },
                                titleAssignmentFiscalYear: '$fiscalYear'
                            },
                            pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $and: [
                                        { $eq: ['$$titleId', '$titleId'] },
                                        { $eq: ['$$firmId', '$firmId'] },
                                        { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                        ]
                                    }
                                }
                            }
                            ],
                            as: 'titleAssignments'
                        }
                    },
                    {
                    $unwind: {
                        path: '$titleAssignments',
                        preserveNullAndEmptyArrays: true
                    }
                    }
                ],
                as: 'ultimateResponsibility'
            }
        },
        {
            $lookup: {
                from: 'title',
                let: {
                    operationalResponsibilitySqm: '$operationalResponsibilitySqm',
                    firmId: '$firmGroupId',
                    titleFiscalYear: '$fiscalYear'
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $in: [{ $toString: '$_id' }, { $ifNull: ['$$operationalResponsibilitySqm', []] }] },
                                    { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                                ]
                            }
                        }
                    },
                    {
                        $lookup: {
                            from: 'titleassignment',
                            let: {
                                titleId: { $toString: '$_id' },
                                titleAssignmentFiscalYear: '$fiscalYear'
                            },
                            pipeline: [
                                {
                                    $match: {
                                        $expr: {
                                            $and: [
                                                { $eq: ['$$titleId', '$titleId'] },
                                                { $eq: ['$$firmId', '$firmId'] },
                                                { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                            ]
                                        }
                                    }
                                }
                            ],
                            as: 'titleAssignments'
                        }
                    },
                    {
                        $unwind: {
                            path: '$titleAssignments',
                            preserveNullAndEmptyArrays: true
                        }
                    }
                ],
                as: 'operationalResponsibilitySqm'
            }
        },
        {
            $lookup: {
                from: 'title',
                let: {
                    orIndependenceRequirement: '$orIndependenceRequirement',
                    firmId: '$firmGroupId',
                    titleFiscalYear: '$fiscalYear'
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$$orIndependenceRequirement', '$_id'] },
                                    { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                                ]
                            }
                        }
                    },
                    {
                    $lookup: {
                        from: 'titleassignment',
                        let: {
                            titleId: { $toString: '$_id' },
                            titleAssignmentFiscalYear: '$fiscalYear'
                        },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $and: [
                                        { $eq: ['$$titleId', '$titleId'] },
                                        { $eq: ['$$firmId', '$firmId'] },
                                        { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                        ]
                                    }
                                }
                            }
                        ],
                        as: 'titleAssignments'
                    }
                    },
                    {
                        $unwind: {
                            path: '$titleAssignments',
                            preserveNullAndEmptyArrays: true
                        }
                    }
                ],
                as: 'orIndependenceRequirement'
            }
        },
        {
            $unwind: {
                path: '$orIndependenceRequirement',
                preserveNullAndEmptyArrays: true
            }
        },
        {
            $lookup: {
                from: 'title',
                let: {
                    orMonitoringRemediation: '$orMonitoringRemediation',
                    firmId: '$firmGroupId',
                    titleFiscalYear: '$fiscalYear'
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$$orMonitoringRemediation', '$_id'] },
                                    { $eq: ['$fiscalYear', '$$titleFiscalYear'] }
                                ]
                            }
                        }
                    },
                    {
                        $lookup: {
                            from: 'titleassignment',
                            let: {
                                titleId: { $toString: '$_id' },
                                titleAssignmentFiscalYear: '$fiscalYear'
                            },
                            pipeline: [
                            {
                                $match: {
                                $expr: {
                                    $and: [
                                        { $eq: ['$$titleId', '$titleId'] },
                                        { $eq: ['$$firmId', '$firmId'] },
                                        { $eq: ['$fiscalYear', '$$titleAssignmentFiscalYear'] }
                                    ]
                                }
                                }
                            }
                            ],
                            as: 'titleAssignments'
                        }
                    },
                    {
                        $unwind: {
                            path: '$titleAssignments',
                            preserveNullAndEmptyArrays: true
                        }
                    }
                ],
                as: 'orMonitoringRemediation'
            }
        },
        {
            $unwind: {
                path: '$orMonitoringRemediation',
                preserveNullAndEmptyArrays: true
            }
        },
        {
            $addFields: {
                combined: {
                    $concatArrays: [
                    {
                        $cond: [
                            { $gt: [{ $size: { $ifNull: ['$ultimateResponsibility', []] } }, 0] },
                            {
                                $map: {
                                    input: { $ifNull: ['$ultimateResponsibility', []] },
                                    as: 'u',
                                    in: {
                                        role: 'ultimateResponsibility',
                                        title: '$$u.name',
                                        assignment: {
                                            $reduce: {
                                                input: {
                                                    $map: {
                                                        input: { $ifNull: ['$$u.titleAssignments.assignments', []] },
                                                        as: 'a',
                                                        in: { $concat: ["$$a.displayName", "(", "$$a.email", ")"] }
                                                    }
                                                },
                                                initialValue: '',
                                                in: {
                                                    $cond: [
                                                        { $eq: ['$$value', ''] },
                                                        '$$this',
                                                        { $concat: ['$$value', '; ', '$$this'] }
                                                    ]
                                                }
                                            }
                                        },
                                        fiscalYear: '$fiscalYear',
                                        memberFirmId: '$memberFirmId',
                                        country: '$country',
                                        firmGroupId: '$firmGroupId',
                                        name: '$name',
                                        type: '$type',
                                        leadershipId: {$toObjectId: '$$u._id'}
                                    }
                                }
                            },
                            [
                                {
                                    role: 'ultimateResponsibility',
                                    title: '',
                                    assignmentName: '',
                                    assignment: '',
                                    fiscalYear: '$fiscalYear',
                                    memberFirmId: '$memberFirmId',
                                    country: '$country',
                                    firmGroupId: '$firmGroupId',
                                    name: '$name',
                                    type: '$type'
                                }
                            ]
                        ]
                    },
                    {
                        $cond: [
                            { $gt: [{ $size: { $ifNull: ['$operationalResponsibilitySqm', []] } }, 0] },
                            {
                                $map: {
                                    input: { $ifNull: ['$operationalResponsibilitySqm', []] },
                                    as: 'o',
                                    in: {
                                        role: 'operationalResponsibilitySqm',
                                        title: '$$o.name',
                                        assignment: {
                                            $reduce: {
                                                input: {
                                                    $map: {
                                                        input: { $ifNull: ['$$o.titleAssignments.assignments', []] },
                                                        as: 'a',
                                                        in: { $concat: ["$$a.displayName", "(", "$$a.email", ")"] }
                                                    }
                                                },
                                                initialValue: '',
                                                in: {
                                                    $cond: [
                                                        { $eq: ['$$value', ''] },
                                                        '$$this',
                                                        { $concat: ['$$value', '; ', '$$this'] }
                                                    ]
                                                }
                                            }
                                        },
                                        fiscalYear: '$fiscalYear',
                                        memberFirmId: '$memberFirmId',
                                        country: '$country',
                                        firmGroupId: '$firmGroupId',
                                        name: '$name',
                                        type: '$type',
                                        leadershipId: {$toObjectId: '$$o._id'}
                                    }
                                }
                            },
                            [
                                {
                                    role: 'operationalResponsibilitySqm',
                                    title: '',
                                    assignmentName: '',
                                    assignment: '',
                                    fiscalYear: '$fiscalYear',
                                    memberFirmId: '$memberFirmId',
                                    country: '$country',
                                    firmGroupId: '$firmGroupId',
                                    name: '$name',
                                    type: '$type'
                                }
                            ]
                        ]
                    },
                    {
                        $cond: [
                            { $ne: ['$orIndependenceRequirement', null] },
                            [
                                {
                                    role: 'orIndependenceRequirement',
                                    title: '$orIndependenceRequirement.name',
                                    assignment: {
                                        $reduce: {
                                            input: {
                                                $map: {
                                                input: { $ifNull: ['$orIndependenceRequirement.titleAssignments.assignments', []] },
                                                as: 'a',
                                                in: { $concat: ["$$a.displayName", "(", "$$a.email", ")"] }
                                                }
                                            },
                                            initialValue: '',
                                            in: {
                                                $cond: [
                                                    { $eq: ['$$value', ''] },
                                                    '$$this',
                                                    { $concat: ['$$value', '; ', '$$this'] }
                                                ]
                                            }
                                        }
                                    },
                                    fiscalYear: '$fiscalYear',
                                    memberFirmId: '$memberFirmId',
                                    country: '$country',
                                    firmGroupId: '$firmGroupId',
                                    name: '$name',
                                    type: '$type',
                                    leadershipId: {$toObjectId: '$orIndependenceRequirement._id'}
                                }
                            ],
                            []
                        ]
                    },
                    {
                        $cond: [
                            { $ne: ['$orMonitoringRemediation', null] },
                            [
                                {
                                    role: 'orMonitoringRemediation',
                                    title: '$orMonitoringRemediation.name',
                                    assignment: {
                                        $reduce: {
                                            input: {
                                                $map: {
                                                input: { $ifNull: ['$orMonitoringRemediation.titleAssignments.assignments', []] },
                                                as: 'a',
                                                in: { $concat: ["$$a.displayName", "(", "$$a.email", ")"] }
                                                }
                                            },
                                            initialValue: '',
                                            in: {
                                                $cond: [
                                                { $eq: ['$$value', ''] },
                                                '$$this',
                                                { $concat: ['$$value', '; ', '$$this'] }
                                                ]
                                            }
                                        }
                                    },
                                    fiscalYear: '$fiscalYear',
                                    memberFirmId: '$memberFirmId',
                                    country: '$country',
                                    firmGroupId: '$firmGroupId',
                                    name: '$name',
                                    type: '$type',
                                    leadershipId: {$toObjectId: '$orMonitoringRemediation._id'}
                                }
                            ],
                            []
                        ]
                    }
                    ]
                }
            }
        },
        { $unwind: '$combined' },
        { $replaceRoot: { newRoot: '$combined' } },
        { $sort: { fiscalYear: 1, memberFirmId: 1 } },
        { $out: 'sqmleadership' }
    ]);
    var endTime = new Date();
    print("SYSTEM:Power-BI-Info:: Completed SQM Leadership Roles script at ", endTime.toISOString(), " Total execution time (minutes): ", (endTime.getTime() - startTIme.getTime())/60000);
}
catch(error){
    print("SYSTEM:Power-BI-Error:: Error in SQM_Leadership_Roles ", error);
    throw (error);
}