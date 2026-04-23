db = db.getSiblingDB('isqc');

//Drop existing collection
db.archerfunctionsprocesses.drop()

db.firm.aggregate([{
    $lookup: {
        from: 'firmprocess',
        let: {
            firmId: '$abbreviation'
        },
        pipeline: [{
            $match: {
                $expr: {
                    $eq: ['$firmId', '$$firmId']
                }
            }
        }],
        as: 'firmprocess'
    }
}, {
    $lookup: {
        from: 'globaldocumentation',
        let: {
            firmId: '$abbreviation',
            firmProcesses: {
                $cond: {
                    if: {
                        $isArray: '$firmprocess'
                    },
                    then: '$firmprocess',
                    else: []
                }
            }
        },
        pipeline: [{
            $match:{
                $expr:{
                    $eq:['$type','Function']
                }
            }
        },{
            $lookup: {
                from: 'functionowner',
                let: {
                    functionId: {
                        $toString: '$_id'
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [{
                                $eq: ['$$firmId', '$firmId']
                            },
                            {
                                $eq: ['$$functionId', '$functionId']
                            }
                            ]
                        }
                    }
                }],
                as: 'functionowner'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    functionId: {
                        $toString: '$_id'
                    }
                },
                pipeline: [{
                    $match:{
                        $expr:{
                            $eq:['$type','Process']
                        }
                    }
                },{
                    $match: {
                        $expr: {
                            $let: {
                                vars: {
                                    firmProcess: {
                                        $arrayElemAt: [{
                                            $filter: {
                                                input: '$$firmProcesses',
                                                as: 'fp',
                                                cond: {
                                                    $eq: ['$$fp.processId', '$uniqueId']
                                                }
                                            }
                                        }, 0]
                                    }
                                },
                                in: {
                                    $cond: {
                                        if: {
                                            $and: ['$$firmProcess.function', {
                                                $ne: ['$$firmProcess.function', '']
                                            }]
                                        },
                                        then: {
                                            $eq: ['$$functionId', '$$firmProcess.function']
                                        },
                                        else: {
                                            $eq: ['$$functionId', '$assignFunction']
                                        }
                                    }
                                }
                            }
                        }
                    }
                }, {
                    $lookup: {
                        from: 'title',
                        let: {
                            firmProcess: {
                                $arrayElemAt: [{
                                    $filter: {
                                        input: '$$firmProcesses',
                                        as: 'fp',
                                        cond: {
                                            $eq: ['$$fp.processId', '$uniqueId']
                                        }
                                    }
                                }, 0]
                            }
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $in: [
                                        '$_id',
                                        {
                                            $cond: {
                                                if: {
                                                    $and: ['$$firmProcess', {
                                                        $isArray: '$$firmProcess.processOwners'
                                                    }]
                                                },
                                                then: '$$firmProcess.processOwners',
                                                else: []
                                            }
                                        }
                                    ]
                                }
                            }
                        }, {
                            $lookup: {
                                from: 'titleassignment',
                                let: {
                                    titleId: {
                                        $toString: '$_id'
                                    }
                                },
                                pipeline: [{
                                    $match: {
                                        $expr: {
                                            $and: [{
                                                $eq: ['$$titleId', '$titleId']
                                            }, {
                                                $eq: ['$$firmId', '$firmId']
                                            }]
                                        }
                                    }
                                }],
                                as: 'titleAssignments'
                            }
                        }, {
                            $unwind: {
                                path: '$titleAssignments',
                                preserveNullAndEmptyArrays: true
                            }
                        }],
                        as: 'poTitles'
                    }
                }],
                as: 'process'
            }
        }, {
            $unwind: {
                path: '$functionowner',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    foTitle: '$functionowner.title'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $eq: ['$$foTitle', '$_id']
                        }
                    }
                }, {
                    $lookup: {
                        from: 'titleassignment',
                        let: {
                            titleId: {
                                $toString: '$_id'
                            }
                        },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $and: [{
                                        $eq: ['$$titleId', '$titleId']
                                    }, {
                                        $eq: ['$$firmId', '$firmId']
                                    }]
                                }
                            }
                        }],
                        as: 'titleAssignments'
                    }
                }, {
                    $unwind: {
                        path: '$titleAssignments',
                        preserveNullAndEmptyArrays: true
                    }
                }],
                as: 'foTitle'
            }
        }, {
            $unwind: {
                path: '$foTitle',
                preserveNullAndEmptyArrays: true
            }
        }, {
            $unwind: {
                path: '$process',
                preserveNullAndEmptyArrays: true
            }
        }],
        as: 'firmFunctions'
    }
}, {
    $unwind: '$firmFunctions'
}, {
    $project: {
        'Entity': '$name',
        NetworkId:{
            $cond:{if:{ $eq: ['$type', "EntityType_Network"] },then:'$abbreviation',else:''}
        },
        AreaId:{
            $cond:{if:{ $eq: ['$type', "EntityType_Area"] },then:'$abbreviation',else:''}
        },
        RegionId:{
            $cond:{if:{ $eq: ['$type', "EntityType_Region"] },then:'$abbreviation',else:''}
        },
        ClusterId:{
            $cond:{if:{ $eq: ['$type', "EntityType_Cluster"] },then:'$abbreviation',else:''}
        },
        SubClusterId:{
            $cond:{if:{ $eq: ['$type', "EntityType_SubCluster"] },then:'$abbreviation',else:''}
        },
        MemberFirmId:{
            $cond:{if:{$or:[{ $eq: ['$type', "EntityType_MemberFirm"] },{ $eq: ['$type', "EntityType_Group"] }]},then:'$abbreviation',else:''}
        },
        'Abbreviation': '$abbreviation',
        'FunctionId': {
            $toString: '$firmFunctions._id'
        },
        'FunctionName': '$firmFunctions.name',
        'FunctionOwnerTitle': {
            $cond: {
                if: '$firmFunctions.foTitle.name',
                then: '$firmFunctions.foTitle.name',
                else: ''
            }
        },
        'FunctionOwnerAssignments': {
            $cond: {
                if: '$firmFunctions.foTitle',
                then: {
                    $let: {
                        vars: {
                            assignments: {
                                $cond: {
                                    if: {
                                        $isArray: '$firmFunctions.foTitle.titleAssignments.assignments'
                                    },
                                    then: '$firmFunctions.foTitle.titleAssignments.assignments',
                                    else: []
                                }
                            }
                        },
                        in: {
                            $cond: {
                                if: {
                                    $gt: [{
                                        $size: '$$assignments'
                                    }, 0]
                                },
                                then: {
                                    $reduce: {
                                        input: '$$assignments',
                                        initialValue: '',
                                        in: {
                                            $concat: ['$$value', {
                                                $cond: {
                                                    if: {
                                                        $ne: ['', '$$value']
                                                    },
                                                    then: '; ',
                                                    else: ''
                                                }
                                            }, '$$this.email']
                                        }
                                    }
                                },
                                else: 'no assignment'
                            }
                        }
                    }
                },
                else: ''
            }
        },
        'ProcessUniqueId': '$firmFunctions.process.uniqueId',
        'ProcessName': '$firmFunctions.process.name',
        'ProcessDescription': '$firmFunctions.process.description',
        'ProcessOwnerTitles': {
            $reduce: {
                input: '$firmFunctions.process.poTitles',
                initialValue: '',
                in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.name'] }
            }
        },
        'ProcessOwnerAssignments': {
            $cond: {
                if: {$and:['$firmFunctions.process.poTitles',{$gt:[{$size:'$firmFunctions.process.poTitles'},0]}]},
                then: {
                    $let: {
                        vars: {
                            processOwnerAssignments: {
                                $reduce:{
                                    input:'$firmFunctions.process.poTitles',
                                    initialValue:[],
                                    in:{
                                        $cond: {
                                            if: { $isArray: '$$this.titleAssignments.assignments' },
                                            then: {$concatArrays:['$$value','$$this.titleAssignments.assignments']},
                                            else: '$$value'
                                        }
                                    }
                                }
                            }
                        },
                        in: {
                            $cond: {
                                if: {
                                    $gt: [{
                                        $size: '$$processOwnerAssignments'
                                    }, 0]
                                },
                                then: {
                                    $reduce: {
                                        input: '$$processOwnerAssignments',
                                        initialValue: '',
                                        in: {
                                            $concat: ['$$value', {
                                                $cond: {
                                                    if: {
                                                        $ne: ['', '$$value']
                                                    },
                                                    then: '; ',
                                                    else: ''
                                                }
                                            }, '$$this.email']
                                        }
                                    }
                                },
                                else: 'no assignment'
                            }
                        }
                    }
                },
                else: ''
            }
        },
        _id: 0
    }
}, {
    $out: 'archerfunctionsprocesses'
}])