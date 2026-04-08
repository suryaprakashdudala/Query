try {
    db = db.getSiblingDB('isqc');
      function formatTimestamp(timestamp) {
          var year = timestamp.getFullYear();
          var month = timestamp.getMonth() + 1;
          var day = timestamp.getDate();
          var hour = timestamp.getHours() > 12 ? timestamp.getHours()-12: timestamp.getHours();
          var minute = timestamp.getMinutes();
          var second = timestamp.getSeconds();
          var amOrPm = timestamp.getHours() >= 12 ? "PM" : "AM";
          return `${month}/${day}/${year} ${hour}:${minute}:${second} ${amOrPm}`;
     }
	print('RF started the batch ......... ',formatTimestamp(new Date()));

	db.firm.aggregate([
            {
                $match: {
                    isRollForwardTriggered: true,
                    isRollFwdComplete : false,
                    rollForwardStatus : 'RollForward_Inprogress'
                }
            }
    ]).forEach(function (getNextRec) {
    db.log.insertOne({ message: 'Rollforward initiated - Abbreviation ' + getNextRec.abbreviation + ' FiscalYear : ' + getNextRec.fiscalYear + ' RollforwardIntiatedBy : ' + getNextRec.rollForwardByEmail + ' RollforwardIntiatedOn : ' + getNextRec.rollForwardDate, text: new Date().toISOString() });
    db.firm.updateOne({ _id: getNextRec._id }, { $set: { rollForwardStatus: 'RollForward_Executing' } });
    print('Rollforward is in progress for ....... ', getNextRec._id, getNextRec.abbreviation, new Date().toISOString());
    //Specific roll-forwards for network
    if (getNextRec.type == 'EntityType_Network') {
        db.enumeration.aggregate([{
            $match: {//From FY25 the Category [A,B,C] doesn't hold FY25 and category is inScope/outOfScope
                _id: { $nin: ["ResourceCategory_C", "ResourceCategory_B", "ResourceCategory_A","ServiceCategory_C","ServiceCategory_B","ServiceCategory_A"] }
            }
        }, {
            $addFields: {
                fiscalYear: { $concatArrays: ['$fiscalYear', [{ $add: [{ $max: '$fiscalYear' }, 1] }]] },
            }
        }]).forEach(function (obj) {
            db.enumeration.updateOne({ _id: obj._id }, { $set: { fiscalYear: obj.fiscalYear } })
        });
        //To add new fiscalyear as _id in enumeration collection
        if (db.enumeration.find({ _id: getNextRec.fiscalYear + 1, type: 'FiscalYearType' }).count() === 0) {
            db.enumeration.insertOne({ _id: getNextRec.fiscalYear + 1, type: 'FiscalYearType', fiscalYear: null, order: db.enumeration.find({ type: 'FiscalYearType' }).sort({ order: -1 }).limit(1).toArray()[0].order + 1, isRetired: false });
        }
        db.country.aggregate([{
            $addFields: {
                fiscalYear: { $concatArrays: ['$fiscalYear', [{ $add: [{ $max: '$fiscalYear' }, 1] }]] },
            }
        }]).forEach(function (obj) {
            db.country.updateOne({ _id: obj._id }, { $set: { fiscalYear: obj.fiscalYear } })
        });
        db.keycontrolresource.aggregate([{
            $addFields: {
                fiscalYear: { $concatArrays: ['$fiscalYear', [{ $add: [{ $max: '$fiscalYear' }, 1] }]] },
            }
        }]).forEach(function (obj) {
            db.keycontrolresource.updateOne({ _id: obj._id }, { $set: { fiscalYear: obj.fiscalYear } })
        });
        db.resourcetype.aggregate([{
            $addFields: {
                fiscalYear: { $concatArrays: ['$fiscalYear', [{ $add: [{ $max: '$fiscalYear' }, 1] }]] },
            }
        }]).forEach(function (obj) {
            db.resourcetype.updateOne({ _id: obj._id }, { $set: { fiscalYear: obj.fiscalYear } })
        });
        db.component.aggregate([{
            $addFields: {
                fiscalYear: { $concatArrays: ['$fiscalYear', [{ $add: [{ $max: '$fiscalYear' }, 1] }]] },
            }
        }]).forEach(function (obj) {
            db.component.updateOne({ _id: obj._id }, { $set: { fiscalYear: obj.fiscalYear } })
        });
       
        //Firm hierarchy roll-forward
        db.firm.aggregate([{
            $match: {
                fiscalYear: { $eq: getNextRec.fiscalYear },
            }
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                isCreatedInCurrentFY: false,
                attachments: [],//update the attachments when the respective firms get RF
                isReadOnly: false,
                isRollForwardTriggered: false,
                isRollForwardedFromPreFY: false,
                isRollFwdComplete: false,
                rollForwardDate: '',
                rollForwardStatus: '',
                rollForwardByEmail: '',
                rollForwardByDisplayName: '',
                firstPublishedDate: '',
                publishedDate: '',
                publishedBy: '',
                publishedUserDisplayName: '',
                isPublishQueryRun: '',
                ultimateResponsibility: [],
                operationalResponsibilitySqm: [],
                orIndependenceRequirement: '',
                orMonitoringRemediation: '',
                pcaobUltimateResponsibility: '',
                pcaobOperationalResponsibilitySqm: '',
                pcaobOrIndependenceRequirement: '',
                pcaobOrMonitoringRemediation: '',
                pcaobOperationalResponsibilityGovernanceAndLeadership:'',
                pcaobOperationalResponsibilityAcceptanceAndContinuance:'',
                pcaobOperationalResponsibilityEngagementPerformance:'',
                pcaobOperationalResponsibilityResources:'',
                pcaobOperationalResponsibilityInformationAndCommunication:'',
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0
            }
        }]).forEach(function (f) {
            const insertedFirm = db.firm.insertOne(f);
            // Insert event for new record
            db.event.insertOne({
                actor: insertedFirm.insertedId,
                publisher: f.type === 'EntityType_MemberFirm' ? f.memberFirmId : f.abbreviation,
                actorType: 'firm',
                fiscalYear: f.fiscalYear,
                message: 'ActionType_Add',
                eventType: 'EventType_Generic',
                isPCAOBObject: f.categoryType !== 'EntityType_PCAOB' ? false : true,
                status: '',
                modifiedBy: 'SYSTEM_ROLLFORWARD',
                modifiedOn: new Date().toISOString(),
                createdBy: 'SYSTEM_ROLLFORWARD',
                createdOn: new Date().toISOString()
            });
        });
        //Global documentation roll-forward
        db.globaldocumentation.aggregate([{
            $match: {
                fiscalYear: { $eq: getNextRec.fiscalYear },
            }
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0
            }
        }]).forEach(function (f) {
            db.globaldocumentation.insertOne(f);
        });
        //Global Titlte roll-forward
        db.title.aggregate([{
            $match: {
                global: true,
                fiscalYear: { $eq: getNextRec.fiscalYear },
            }
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0
            }
        }]).forEach(function (f) {
            db.title.insertOne(f);
        });
         // update user Roles while network rollforward
        db.user.updateMany(
            { 
                $or: [
                    { "roles.roleId": { $regex: `_${getNextRec.fiscalYear}$` } }, // Match roles ending in fiscalYear
                    { "areaWideAccess.fiscalYear": getNextRec.fiscalYear },
                    { "regionWideAccess.fiscalYear": getNextRec.fiscalYear }
                ]
            },
            [
                {
                    $set: {
                        // Extract roles matching the current year
                        matchingRoles: {
                            $filter: {
                                input: "$roles",
                                as: "role",
                                cond: {
                                    $regexMatch: { input: "$$role.roleId", regex: `_${getNextRec.fiscalYear}$` }
                                }
                            }
                        },
                        matchingAreaWideAccess: {
                            $filter: {
                                input: "$areaWideAccess",
                                as: "areaAccess",
                                cond: { $eq: ["$$areaAccess.fiscalYear", getNextRec.fiscalYear] }
                            }
                        },
                        matchingRegionWideAccess: {
                            $filter: {
                                input: "$regionWideAccess",
                                as: "regionAccess",
                                cond: { $eq: ["$$regionAccess.fiscalYear", getNextRec.fiscalYear] }
                            }
                        }
                    }
                },
                {
                    $set: {
                        // Generate new roles with incremented year
                        newRoles: {
                            $map: {
                                input: "$matchingRoles",
                                as: "filteredRole",
                                in: {
                                    roleId: {
                                        $concat: [
                                        {
                                            $reduce: {
                                            input: {
                                                $slice: [
                                                { $split: ["$$filteredRole.roleId", "_"] }, // Split roleId into parts
                                                0,
                                                { $subtract: [{ $size: { $split: ["$$filteredRole.roleId", "_"] } }, 1] }
                                                ]
                                            },
                                            initialValue: "",
                                            in: {
                                                $concat: [
                                                "$$value",
                                                { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: "_" } },
                                                "$$this"
                                                ]
                                            }
                                            }
                                        },
                                        "_",
                                        { $toString: { $add: [getNextRec.fiscalYear, 1] } } // Increment year dynamically
                                        ]
                                    },
                                    geographyId: "$$filteredRole.geographyId"
                                }
                            }
                        },
                        newAreaWideAccess: {
                            $map: {
                                input: "$matchingAreaWideAccess",
                                as: "areaAccess",
                                in: {
                                abbreviation: "$$areaAccess.abbreviation",
                                accessType: "$$areaAccess.accessType",
                                fiscalYear: { $add: ["$$areaAccess.fiscalYear", 1] } // Increment year dynamically
                                }
                            }
                        },
                        newRegionWideAccess: {
                            $map: {
                                input: "$matchingRegionWideAccess",
                                as: "regionAccess",
                                in: {
                                abbreviation: "$$regionAccess.abbreviation",
                                accessType: "$$regionAccess.accessType",
                                fiscalYear: { $add: ["$$regionAccess.fiscalYear", 1] } // Increment year dynamically
                                }
                            }
                        },
                        newSuperAdminScopeYear: {
                            $cond: {
                                if: { 
                                    $and: [
                                        { $ne: [{ $indexOfCP: [{ $toString: "$superAdminScopeYear" }, `_${getNextRec.fiscalYear}`] }, -1] },
                                        { $eq: [{ $indexOfCP: [{ $toString: "$superAdminScopeYear" }, `_${getNextRec.fiscalYear + 1}`] }, -1] }
                                    ]
                                },
                                then: {
                                    $concat: [
                                        "$superAdminScopeYear",
                                        `|/RoleType_GlobalAdmin_${getNextRec.fiscalYear + 1}/`
                                    ]
                                },
                                else: "$superAdminScopeYear"
                            }
                        },
                        newNonSuperAdminScopeYear: {
                            $cond: {
                                if: { 
                                    $and: [
                                        { $ne: [{ $indexOfCP: [{ $toString: "$nonSuperAdminScopeYear" }, `_${getNextRec.fiscalYear}`] }, -1] },
                                        { $eq: [{ $indexOfCP: [{ $toString: "$nonSuperAdminScopeYear" }, `_${getNextRec.fiscalYear + 1}`] }, -1] }
                                    ]
                                },
                                then: {
                                    $concat: [
                                        "$nonSuperAdminScopeYear",
                                        `|/RoleType_NonGlobal_${getNextRec.fiscalYear + 1}/`
                                    ]
                                },
                                else: "$nonSuperAdminScopeYear"
                            }
                        }
                    }
                },
                {
                    $set: {
                        // Update only the missing roles
                        roles: { $concatArrays: ["$roles", "$newRoles"] },
                        areaWideAccess: { $concatArrays: ["$areaWideAccess", "$newAreaWideAccess"] },
                        regionWideAccess: { $concatArrays: ["$regionWideAccess", "$newRegionWideAccess"] },
                        superAdminScopeYear: "$newSuperAdminScopeYear",
                        nonSuperAdminScopeYear: "$newNonSuperAdminScopeYear"
                    }
                },
                // Delete temporary fields
                { $unset: ["matchingRoles", "newRoles", "matchingAreaWideAccess", "newAreaWideAccess", "matchingRegionWideAccess", "newRegionWideAccess", "newSuperAdminScopeYear", "newNonSuperAdminScopeYear"] }
            ]
        );
    }
    //Recreation of Baseline Title if deleted from FY24
    // try {
    //     db.title.aggregate([
    //         {
    //             $match: {
    //                 fiscalYear: { $eq: getNextRec.fiscalYear },
    //                 global: true
    //             }
    //         }]).forEach(function (f) {
    //             if (db.title.find({ prevId: f._id.toString().replace(/ObjectId\("(.*)"\)/, "$1") }).count() < 1) {
    //                 db.title.aggregate([{
    //                     $match: {
    //                         _id: f._id
    //                     }
    //                 }, {
    //                     $addFields: {
    //                         fiscalYear: { $add: ['$fiscalYear', 1] },
    //                         prevId: { $toString: '$_id' },
    //                         createdBy:'SYSTEM_ROLLFORWARD',
    //                         createdOn:new Date().toISOString(),
    //                         modifiedBy:'SYSTEM_ROLLFORWARD',
    //                         modifiedOn:new Date().toISOString()
    //                     }
    //                 }, {
    //                     $project: {
    //                         _id: 0,
    //                         id: 0
    //                     }

    //                 }]).forEach(function (z) {
    //                     db.title.insertOne(z);
    //                 });
    //             }
    //         })
    // } catch (error) {
    //     print("SYSTEM:RollForward-Error :: Error at Recreation of baseline title ",error);
    //     throw (error);
    // }
    //Custom title roll-forward
    try {
        db.title.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
                global: false
            }
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0
            }
        }]).forEach(function (f) {
            db.title.insertOne(f);
        });
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Custom title ",error);
        throw (error);
    }

    //FunctionOwner
    try {
        db.functionowner.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    titleId: {
                        $cond: {
                            if: { $isArray: '$title' },
                            then: {
                                $map: {
                                    input: '$title',
                                    as: 'p',
                                    in: { $toString: '$$p' }
                                }
                            },
                            else: []
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$titleId']
                        }
                    }
                }],
                as: 'titles'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                localField: 'functionId',
                foreignField: 'prevId',
                as: 'function'
            }
        }, {
            $unwind: '$function'
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
            }
        }]).forEach(function (f) {
            if (f.titles.length > 0) {
                f.title = f.titles.map(a => a._id.toString().replace(/ObjectId\("(.*)"\)/, "$1"))
            }else{
                f.title = [];
            }
            f.functionId = f.function._id.toString().replace(/ObjectId\("(.*)"\)/, "$1");
            delete f.titles;
            delete f.function;
            db.functionowner.insertOne(f)
        })
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at FunctionOwner ",error);
        throw (error);
    }
    //Firm process
    try {
        db.firmprocess.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    processId: '$processId',
                    fiscalYear: { $add: ['$fiscalYear', 1] }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$$processId', '$uniqueId'] },
                                { $eq: ['$fiscalYear', '$$fiscalYear'] }
                            ]
                        }
                    }
                }],
                as: 'process'
            }
        }, { $unwind: '$process' },
        {
            $lookup: {
                from: 'title',
                let: {
                    processOwners: {
                        $map: {
                            input: '$processOwners',
                            as: 'p',
                            in: { $toString: '$$p' }
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$processOwners']
                        }
                    }
                }],
                as: 'processOwnersCollection'
            }
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                processOwners: '$processOwnersCollection._id', //updation: reset processOwners with 24 ids    
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
                process: 0,
                processOwnersCollection: 0,

            }
        }]).forEach(function (f) {
            db.firmprocess.insertOne(f)
        })
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Firm process ",error);
        throw (error);
    }
    //Title assignments
    try {
        db.titleassignment.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
            }
        }, {
            $lookup: {
                from: 'title',
                localField: 'titleId',
                foreignField: 'prevId',
                as: 'title'
            }
        }, {
            $unwind: '$title'
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                titleId: { $toString: '$title._id' },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
                title: 0
            }
        }]).forEach(function (t) {
            db.titleassignment.insertOne(t);
        });
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Title assignments ",error);
        throw (error);
    }
    //QO roll-forward
    try {
        //var rfObjectives = [];
        //rolledForwardEntitiesForBkcAssignments.forEach(re => {
        db.documentation.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
                type: {
                    $in: ["QualityObjective"]
                }
            }
        }, {
            $addFields: {
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                dateSavedAfterPublish: '',
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0
            }
        }]).forEach(function (f) {
            db.documentation.insertOne(f);
        });
        // rfObjectives = db.documentation.aggregate([
        //     {
        //         $match: {
        //             fiscalYear: { $eq: getNextRec.fiscalYear + 1 },
        //             type: 'QualityObjective'
        //         }
        //     },
        //     {
        //         $group: {
        //             _id: "$type",
        //             uniqueIds: {
        //                 $push: {
        //                     $reduce: {
        //                         input: ['$uniqueId'],
        //                         initialValue: '',
        //                         in: { $concat: ["$$value", "$$this"] }
        //                     }
        //                 }
        //             }
        //         }
        //     }]).toArray();
        // if (rfObjectives[0] !== undefined) {
        //     rfObjectives = rfObjectives[0].uniqueIds;
        // }
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Quality Objective ",error);
        throw (error);
    }
    // QR roll-forward
    try {
        //var rfQualityRisk = [];
        //rolledForwardEntitiesForBkcAssignments.forEach(re => {
        db.documentation.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
                type: {
                    $in: ["QualityRisk"]
                }
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    relatedQualityRisks: '$relatedObjectives',
                    rfFiscalYear: { $add: ['$fiscalYear', 1] },
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$fiscalYear', '$$rfFiscalYear'] },
                                { $in: ['$uniqueId', '$$relatedQualityRisks'] }
                            ]
                        }
                    }
                },
                {
                    $project: {
                        uniqueId: 1,
                        fiscalYear: 1
                    }
                }],
                as: 'relatedObjectiveInCurrentYear'
            }
        },
        {
            $addFields: {
                RFqoCountCurrentYear: { $size: '$relatedObjectiveInCurrentYear' },
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                dateSavedAfterPublish: '',
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
                relatedObjectiveInCurrentYear: 0,
            }
        }]).forEach(function (f) {
            var a = [];
            if (f.RFqoCountCurrentYear > 0) {
                if (f.relatedObjectives !== undefined) {
                    f.relatedObjectives.forEach(function (qr) {
                        if (db.documentation.find({ type: 'QualityObjective', uniqueId: qr, fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                            a.push(qr);
                        }
                    })
                    f.relatedObjectives = a;
                }
                delete f.RFqoCountCurrentYear;
                db.documentation.insertOne(f);
            }
        });
        // rfQualityRisk = db.documentation.aggregate([
        //     {
        //         $match: {
        //             fiscalYear: { $eq: getNextRec.fiscalYear + 1 },
        //             type: 'QualityRisk'
        //         }
        //     },
        //     {
        //         $group: {
        //             _id: "$type",
        //             uniqueIds: {
        //                 $push: {
        //                     $reduce: {
        //                         input: ['$uniqueId'],
        //                         initialValue: '',
        //                         in: { $concat: ["$$value", "$$this"] }
        //                     }
        //                 }
        //             }
        //         }
        //     }]).toArray();
        // if (rfQualityRisk[0] !== undefined) {
        //     rfQualityRisk = rfQualityRisk[0].uniqueIds;
        // }
        //});
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Quality Risk ",error);
        throw (error);
    }
    //QSR roll-forward
    try {
        //var rfSubrisk = [];
        //rolledForwardEntitiesForBkcAssignments.forEach(re => {
        db.documentation.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
                type: {
                    $in: ["SubRisk"]
                }
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    relatedQualityRisks: '$relatedQualityRisks',
                    fiscalYear: { $add: ['$fiscalYear', 1] }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$fiscalYear', '$$fiscalYear'] },
                                { $in: ['$uniqueId', '$$relatedQualityRisks'] }
                            ]
                        }
                    }
                }, {
                    $project: {
                        _id: 0,
                        uniqueId: 1
                    }
                }],
                as: 'relatedQR'
            }
        },
        {
            $addFields: {
                RFqrCount: { $size: '$relatedQR' },
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                dateSavedAfterPublish: '',
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
                relatedQR: 0
            }
        }]).forEach(function (f) {
            var a = [];
            if (f.RFqrCount > 0) {
                f.relatedQualityRisks.forEach(function (qr) {
                    if (db.documentation.find({ type: 'QualityRisk', uniqueId: qr, fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                        a.push(qr);
                    }
                })
                f.relatedQualityRisks = a;
                delete f.RFqrCount;
                db.documentation.insertOne(f);
            }
        });
        // rfSubrisk = db.documentation.aggregate([
        //     {
        //         $match: {
        //             fiscalYear: { $eq: getNextRec.fiscalYear + 1 },
        //             type: 'SubRisk'
        //         }
        //     },
        //     {
        //         $group: {
        //             _id: "$type",
        //             uniqueIds: {
        //                 $push: {
        //                     $reduce: {
        //                         input: ['$uniqueId'],
        //                         initialValue: '',
        //                         in: { $concat: ["$$value", "$$this"] }
        //                     }
        //                 }
        //             }
        //         }
        //     }]).toArray();
        // if (rfSubrisk[0] !== undefined) {
        //     rfSubrisk = rfSubrisk[0].uniqueIds;
        // }
        //});
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Quality Sub Risk ",error);
        throw (error);
    }
    //Resource roll-forward
    try {
        //var rfResources = [];
        //rolledForwardEntitiesForBkcAssignments.forEach(re => {
        db.documentation.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear },
                type: {
                    $in: ["Resource"]
                }
            }
        },
        {
            $lookup: {
                from: 'documentation',
                let: {
                    relatedQualityRisks: '$relatedQualityRisks',
                    relatedSubRisks: '$relatedSubRisks',
                    fiscalYear: { $add: ['$fiscalYear', 1] }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$fiscalYear', '$$fiscalYear'] },
                                { $in: ['$uniqueId', '$$relatedQualityRisks'] }
                            ]
                        }
                    }
                }],
                as: 'relatedQR'
            }
        },{
            $lookup: {
                from: 'globaldocumentation',
                let: { relatedFunctionList: { $cond: { if: { $isArray: '$relatedFunctionList' }, then: '$relatedFunctionList', else: [] } } },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$relatedFunctionList']
                        }
                    }
                }],
                as: 'relatedFunctionList'
            }
            }, {
            $lookup: {
                from: 'globaldocumentation',
                let: { techRelatedFunctionList: { $cond: { if: { $isArray: '$techRelatedFunctionList' }, then: '$techRelatedFunctionList', else: [] } } },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$techRelatedFunctionList']
                        }
                    }
                }],
                as: 'techRelatedFunctionList'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    tags: { $cond: { if: { $isArray: '$tags' }, then: '$tags', else: [] } }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$tags']
                        }
                    }
                }],
                as: 'tags'
            }
        },
        {
            $addFields: {
                RFqrCount: { $size: '$relatedQR' },
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                dateSavedAfterPublish: '',
                categoryofResource: {//To change the resourceCategory to InScope/OutofScope when RF of Resource of happens to FY25 from FY24
                    $cond: {
                        if: { $in: ['$categoryofResource', ['ResourceCategory_A', 'ResourceCategory_B']] },
                        then: 'ResourceCategory_inScope',
                        else: {
                            $cond: {
                                if: { $eq: ['$categoryofResource', 'ResourceCategory_C'] },
                                then: 'ResourceCategory_out_Of_Scope',
                                else: '$categoryofResource'
                            }
                        }
                    }
                },
                categoryServiceProvider: {//To change the categoryServiceProvider to InScope/OutofScope when RF of Resource of happens to FY26 from FY25
                    $cond: {
                        if: { $in: ['$categoryServiceProvider', ['ServiceCategory_A', 'ServiceCategory_B']] },
                        then: 'ServiceCategory_inScope',
                        else: {
                            $cond: {
                                if: { $eq: ['$categoryServiceProvider', 'ServiceCategory_C'] },
                                then: 'ServiceCategory_out_Of_Scope',
                                else: '$categoryServiceProvider'
                            }
                        }
                    }
                },
                relatedFunction: '',
                techRelatedFunction: '',
                relatedFunctionList: {
                    $map: {
                        input: '$relatedFunctionList',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                techRelatedFunctionList: {
                    $map: {
                        input: '$techRelatedFunctionList',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                tags: {
                    $map: {
                        input: '$tags',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
                relatedQR: 0
            }
        }]).forEach(function (f) {
            var a = [];
            if (f.RFqrCount > 0) {
                f.relatedQualityRisks.forEach(function (qr) {
                    if (db.documentation.find({ type: 'QualityRisk', uniqueId: qr, fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                        a.push(qr);
                    }
                })
                f.relatedQualityRisks = a;
                f.relatedQualityRisksMitigatedByResource=f.relatedQualityRisksMitigatedByResource.length>0 ? a : [] ;
                delete f.RFqrCount;
                db.documentation.insertOne(f);
            }
        });
        // rfResources = db.documentation.aggregate([
        //     {
        //         $match: {
        //             fiscalYear: { $eq: (getNextRec.fiscalYear) + 1 },
        //             type: 'Resource'
        //         }
        //     },
        //     {
        //         $addFields: {
        //             id: { $toString: '$_id' }
        //         }
        //     },
        //     {
        //         $group: {
        //             _id: "$type",
        //             uniqueIds: {
        //                 $push: {
        //                     $reduce: {
        //                         input: ['$id'],
        //                         initialValue: '',
        //                         in: { $concat: ["$$value", "$$this"] }
        //                     }
        //                 }
        //             }
        //         }
        //     }]).toArray();
        // if (rfResources.length > 0) {
        //     if (rfResources[0] !== undefined) {
        //         rfResources = rfResources[0].uniqueIds;
        //     }
        // }
        //});
    } catch (error) {
        print("SYSTEM:RollForward-Error:: Error at Resources ",error);
        throw (error);
    }

//documenttag roll-forward
try {
db.documenttag.aggregate([{
$match: {
firmId: { $eq: getNextRec.abbreviation },
fiscalYear: { $eq: getNextRec.fiscalYear },
isDeleted: { $ne: true }
}
}, {
$addFields: {
fiscalYear: { $add: ['$fiscalYear', 1] },
prevId: { $toString: '$_id' },
createdBy:'SYSTEM_ROLLFORWARD',
createdOn:new Date().toISOString(),
modifiedBy:'SYSTEM_ROLLFORWARD',
modifiedOn:new Date().toISOString()
}
}, {
$project: {
_id: 0,
id: 0
}
}]).forEach(function (t) {
db.documenttag.insertOne(t);
});
} catch (error) {
print("SYSTEM:RollForward-Error :: Error at DocumentTag ",error);
throw (error);
}


    //Controls roll-forward
    try {
        //rolledForwardEntitiesForBkcAssignments.forEach(re => {
        db.documentation.aggregate([{
            $match: {
                firmId: { $eq: getNextRec.abbreviation },
                type: {
                    $in: ["KeyControl", "RequirementControl"]
                },
                fiscalYear: { $eq: getNextRec.fiscalYear }
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    relatedQualityRisks: '$relatedQualityRisks',
                    relatedSubRisks: '$relatedSubRisks',
                    fiscalYear: { $add: ['$fiscalYear', 1] }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $or: [
                                {
                                    $and: [
                                        { $eq: ['$fiscalYear', '$$fiscalYear'] },
                                        { $in: ['$uniqueId', '$$relatedSubRisks'] }
                                    ]
                                },
                                {
                                    $and: [
                                        { $eq: ['$fiscalYear', '$$fiscalYear'] },
                                        { $in: ['$uniqueId', '$$relatedQualityRisks'] }
                                    ]
                                }]
                        }
                    }
                }],
                as: 'relatedQRandSR'
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    tags: { $cond: { if: { $isArray: '$tags' }, then: '$tags', else: [] } }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$tags']
                        }
                    }
                }],
                as: 'tags'
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                localField: 'controlFunction',
                foreignField: 'prevId',
                as: 'controlFunction'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    localControlFunction: '$localControlFunction'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', { $cond: { if: { $isArray: '$$localControlFunction' }, then: '$$localControlFunction', else: [] } }]
                        }
                    }
                }],
                as: 'localControlFunction'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    executionControlFunction: '$executionControlFunction'
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', { $cond: { if: { $isArray: '$$executionControlFunction' }, then: '$$executionControlFunction', else: [] } }]
                        }
                    }
                }],
                as: 'executionControlFunction'
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    mitigatedResources: { $cond: { if: { $isArray: '$mitigatedResources' }, then: '$mitigatedResources', else: [] } }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$mitigatedResources']
                        }
                    }
                }],
                as: 'mitigatedResources'
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    supportingITApplication: { $cond: { if: { $isArray: '$supportingITApplication' }, then: '$supportingITApplication', else: [] } }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$supportingITApplication']
                        }
                    }
                }],
                as: 'supportingITApplication'
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    ipeSystems: {
                        $reduce: {
                            input: '$addInformationExecutionControls.ipeSystems',
                            initialValue: [],
                            in: { $concatArrays: ['$$value', '$$this'] }
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$ipeSystems']
                        }
                    }
                }],
                as: 'ipeSystems'
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    responseOwners: {
                        $map: {
                            input: '$responseOwners',
                            as: 'r',
                            in: { $toString: '$$r' }
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$responseOwners']
                        }
                    }
                }],
                as: 'responseOwners'
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    controlOperator: {
                        $map: {
                            input: '$controlOperator',
                            as: 'r',
                            in: { $toString: '$$r' }
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$controlOperator']
                        }
                    }
                }],
                as: 'controlOperator'
            }
        }, {
            $addFields: {
                RFSrQrCount: { $size: '$relatedQRandSR' },
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                dateSavedAfterPublish: '',
                ctrlStateUnchangedFromPrevYear: { $cond: { if: { $eq: ['$type', 'KeyControl'] }, then: true, else: false } },
                rollForwardReviewDate: '',
                tags: {
                    $map: {
                        input: '$tags',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                controlFunction: {
                    $map: {
                        input: '$controlFunction',
                        as: 'f',
                        in: { $toString: '$$f._id' }
                    }
                },
                localControlFunction: {
                    $map: {
                        input: '$localControlFunction',
                        as: 'f',
                        in: { $toString: '$$f._id' }
                    }
                },
                executionControlFunction: {
                    $map: {
                        input: '$executionControlFunction',
                        as: 'f',
                        in: { $toString: '$$f._id' }
                    }
                },
                mitigatedResources: {
                    $map: {
                        input: '$mitigatedResources',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                supportingITApplication: {
                    $map: {
                        input: '$supportingITApplication',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                addInformationExecutionControls: {
                    $map: {
                        input: '$addInformationExecutionControls',
                        as: 'iec',
                        in: {
                            $let: {
                                vars: {
                                    newiec: {
                                        ipeSystems: {
                                            $map: {
                                                input: {
                                                    $filter: {
                                                        input: '$ipeSystems',
                                                        as: 'sys',
                                                        cond: { $in: ['$$sys.prevId', '$$iec.ipeSystems'] }
                                                    }
                                                },
                                                as: 'ns',
                                                in: { $toString: '$$ns._id' }
                                            }
                                        }
                                    }
                                },
                                in: {
                                    $mergeObjects: ['$$iec', '$$newiec']
                                }
                            }
                        }
                    }
                },
                responseOwners: {
                    $map: {
                        input: '$responseOwners',
                        as: 'r',
                        in: '$$r._id'
                    }
                },
                controlOperator: {
                    $map: {
                        input: '$controlOperator',
                        as: 'r',
                        in: '$$r._id'
                    }
                },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
                relatedQRandSR: 0,
                ipeSystems: 0
            }
        }]).forEach(function (f) {
            var obj = [];
            var qr = [];
            var sr = [];
            var res = [];
            var mitiRes = [];
            var ipeSys = [];
            if (f.RFSrQrCount > 0) {
                if (f.relatedObjectives !== undefined) {
                    f.relatedObjectives.length && f.relatedObjectives.forEach(function (qr) {
                        if (db.documentation.find({ type: 'QualityObjective', uniqueId: qr, fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                            obj.push(qr);
                        }
                    })
                    f.relatedObjectives = obj;
                }
                f.relatedQualityRisks.length && f.relatedQualityRisks.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'QualityRisk', uniqueId: ctl, fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        qr.push(ctl);
                    }
                })
                f.relatedQualityRisks = qr;
                f.relatedSubRisks.length && f.relatedSubRisks.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'SubRisk', uniqueId: ctl, fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        sr.push(ctl);
                    }
                })
                f.relatedSubRisks = sr;
                f.supportingITApplication.length && f.supportingITApplication.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'Resource', _id: ObjectId(ctl), fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        res.push(ctl);
                    }
                })	
                f.supportingITApplication = res;
                f.mitigatedResources.length && f.mitigatedResources.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'Resource', _id: ObjectId(ctl), fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        mitiRes.push(ctl);
                    }
                })
                f.mitigatedResources = mitiRes;
                var getAddInfo = db.documentation.findOne({ _id: ObjectId(f.prevId) });
                var addInformationExecutionControlsLocal = [];
                getAddInfo.addInformationExecutionControls.forEach(function (r) {
                    var masterITem = r;
                    var localVar = r.ipeSystems;
                    if (r.ipeSystems && r.ipeSystems.length) {
                        localVar = [];
                        for (var ipeItem of r.ipeSystems) {
                            var Fy24ITem = ipeItem;
                            Fy24ITem = db.documentation.findOne({ prevId: ipeItem });
                            if (!Fy24ITem) {
                            } else {
                                localVar.push(Fy24ITem._id.toString().replace(/ObjectId\("(.*)"\)/, "$1"));
                            }
                        }
                    }
                    masterITem.ipeSystems = localVar;
                    addInformationExecutionControlsLocal.push(masterITem);
                })
                f.addInformationExecutionControls = addInformationExecutionControlsLocal;
                f.baselineModifiedAttributes = {}
                f.baselineAcceptedAttributes = {}
                delete f.RFSrQrCount;
                db.documentation.insertOne(f);
            }
        });
        //});
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Controls ",error);
        throw (error);
    }
    // RequirementControlAssignement RollForward
    try {
        //rolledForwardEntitiesForBkcAssignments.forEach(re => {
        db.documentation.aggregate([{
            $match: {
                type: "RequirementControlAssignment",
                firmId: { $eq: getNextRec.abbreviation },
                fiscalYear: { $eq: getNextRec.fiscalYear }
            }
        },
        {
            $lookup: {
                from: 'documentation',
                let: {
                    requirementControlFirmId: '$requirementControlFirmId',
                    type: 'RequirementControl',
                    uniqueId: '$uniqueId',
                    fiscalYear: { $add: ['$fiscalYear', 1] }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$fiscalYear', '$$fiscalYear'] },
                                { $eq: ['$uniqueId', '$$uniqueId'] },
                                { $eq: ['$firmId', '$$requirementControlFirmId'] },
                                { $eq: ['$type', '$$type'] },
                            ]
                        },
                    }
                }],
                as: 'requirementControlPresentInRFyear'
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                let: {
                    tags: { $cond: { if: { $isArray: '$tags' }, then: '$tags', else: [] } }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$tags']
                        }
                    }
                }],
                as: 'tags'
            }
        },
        {
            $lookup: {
                from: 'globaldocumentation',
                localField: 'controlFunction',
                foreignField: 'prevId',
                as: 'controlFunction'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                localField: 'localControlFunction',
                foreignField: 'prevId',
                as: 'localControlFunction'
            }
        }, {
            $lookup: {
                from: 'globaldocumentation',
                localField: 'executionControlFunction',
                foreignField: 'prevId',
                as: 'executionControlFunction'
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    mitigatedResources: { $cond: { if: { $isArray: '$mitigatedResources' }, then: '$mitigatedResources', else: [] } }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$mitigatedResources']
                        }
                    }
                }],
                as: 'mitigatedResources'
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    supportingITApplication: { $cond: { if: { $isArray: '$supportingITApplication' }, then: '$supportingITApplication', else: [] } }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$supportingITApplication']
                        }
                    }
                }],
                as: 'supportingITApplication'
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    ipeSystems: {
                        $reduce: {
                            input: '$addInformationExecutionControls.ipeSystems',
                            initialValue: [],
                            in: { $concatArrays: ['$$value', '$$this'] }
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$ipeSystems']
                        }
                    }
                }],
                as: 'ipeSystems'
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    responseOwners: {
                        $map: {
                            input: '$responseOwners',
                            as: 'r',
                            in: { $toString: '$$r' }
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$responseOwners']
                        }
                    }
                }],
                as: 'responseOwners'
            }
        }, {
            $lookup: {
                from: 'title',
                let: {
                    controlOperator: {
                        $map: {
                            input: '$controlOperator',
                            as: 'r',
                            in: { $toString: '$$r' }
                        }
                    }
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $in: ['$prevId', '$$controlOperator']
                        }
                    }
                }],
                as: 'controlOperator'
            }
        }, {
            $addFields: {
                requirementControlPresentInRFyearSize: { $size: '$requirementControlPresentInRFyear' },
                requirementControlPresentInRFyear:'$requirementControlPresentInRFyear',
                fiscalYear: { $add: ['$fiscalYear', 1] },
                prevId: { $toString: '$_id' },
                dateSavedAfterPublish: '',
                ctrlStateUnchangedFromPrevYear: true,
                rollForwardReviewDate: '',
                tags: {
                    $map: {
                        input: '$tags',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                controlFunction: {
                    $map: {
                        input: '$controlFunction',
                        as: 'f',
                        in: { $toString: '$$f._id' }
                    }
                },
                localControlFunction: {
                    $map: {
                        input: '$localControlFunction',
                        as: 'f',
                        in: { $toString: '$$f._id' }
                    }
                },
                executionControlFunction: {
                    $map: {
                        input: '$executionControlFunction',
                        as: 'f',
                        in: { $toString: '$$f._id' }
                    }
                },
                mitigatedResources: {
                    $map: {
                        input: '$mitigatedResources',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                supportingITApplication: {
                    $map: {
                        input: '$supportingITApplication',
                        as: 'r',
                        in: { $toString: '$$r._id' }
                    }
                },
                addInformationExecutionControls: {
                    $map: {
                        input: '$addInformationExecutionControls',
                        as: 'iec',
                        in: {
                            $let: {
                                vars: {
                                    newiec: {
                                        ipeSystems: {
                                            $map: {
                                                input: {
                                                    $filter: {
                                                        input: '$ipeSystems',
                                                        as: 'sys',
                                                        cond: { $in: ['$$sys.prevId', '$$iec.ipeSystems'] }
                                                    }
                                                },
                                                as: 'ns',
                                                in: { $toString: '$$ns._id' }
                                            }
                                        }
                                    }
                                },
                                in: {
                                    $mergeObjects: ['$$iec', '$$newiec']
                                }
                            }
                        }
                    }
                },
                responseOwners: {
                    $map: {
                        input: '$responseOwners',
                        as: 'r',
                        in: '$$r._id'
                    }
                },
                controlOperator: {
                    $map: {
                        input: '$controlOperator',
                        as: 'r',
                        in: '$$r._id'
                    }
                },
                createdBy:'SYSTEM_ROLLFORWARD',
                createdOn:new Date().toISOString(),
                modifiedBy:'SYSTEM_ROLLFORWARD',
                modifiedOn:new Date().toISOString()
            }
        }, {
            $project: {
                _id: 0,
                id: 0,
                ipeSystems: 0
            }
        }]).forEach(function (f) {
            var obj = [];
            var qr = [];
            var sr = [];
            var res = [];
            var mitiRes = [];
            var ipeSys = [];
            var additionalObj=[];
            var additionalQr=[];
            var additionalSr=[];
            if (f.requirementControlPresentInRFyearSize > 0) {
                f.relatedObjectives.length && f.relatedObjectives.forEach(function (qr) {
                    if (db.documentation.find({ type: 'QualityObjective', uniqueId: qr, fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        obj.push(qr);
                    }
                })
                f.relatedObjectives = obj;
                f.relatedQualityRisks.length && f.relatedQualityRisks.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'QualityRisk', uniqueId: ctl, fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                        qr.push(ctl);
                    }
                })
                f.relatedQualityRisks = qr;
                f.relatedSubRisks.length && f.relatedSubRisks.forEach(function (qr) {
                    if (db.documentation.find({ type: 'SubRisk', uniqueId: qr, fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                        sr.push(qr);
                    }
                })
                f.relatedSubRisks = sr;
                //additional QO QR QSR
                f.additionalQOs && f.additionalQOs.length && f.additionalQOs.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'QualityObjective', uniqueId: ctl, fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        additionalObj.push(ctl);
                    }
                })
                f.additionalQOs = additionalObj;
                f.additionalQRs && f.additionalQRs.length && f.additionalQRs.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'QualityRisk', uniqueId: ctl, fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        additionalQr.push(ctl);
                    }
                })
                f.additionalQRs = additionalQr;
                 f.additionalSRs && f.additionalSRs.length && f.additionalSRs.forEach(function (ctl) {
                    if (db.documentation.find({ type: 'SubRisk', uniqueId: ctl, fiscalYear: getNextRec.fiscalYear + 1 }).count()  > 0) {
                        additionalSr.push(ctl);
                    }
                })
                f.additionalSRs = additionalSr;
                //additional QO QR QSR
                f.supportingITApplication && f.supportingITApplication.length && f.supportingITApplication.forEach(function (qr) {
                    if (db.documentation.find({ type: 'Resource', _id: ObjectId(qr), fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                        res.push(qr);
                    }
                })
                f.supportingITApplication = res;
                f.mitigatedResources && f.mitigatedResources.length && f.mitigatedResources.forEach(function (qr) {
                    if (db.documentation.find({ type: 'Resource', _id: ObjectId(qr), fiscalYear: getNextRec.fiscalYear + 1 }).count() > 0) {
                        mitiRes.push(qr);
                    }
                })
                f.mitigatedResources = mitiRes;
                // f.addInformationExecutionControls.forEach(function (r) {
                //     ipeSys = [];
                //     r.ipeSystems.length && r.ipeSystems.forEach(function (qr) {
                //         if (rfResources.includes(qr)) {
                //             ipeSys.push(qr);
                //         }
                //     });
                //     r.ipeSystems = ipeSys;
                // });

                // for draft rf rej controls, the designer modified changes are pulled but still we see a pending review until user with admin/edit access opens atleast once.
                // instead, setting the modified and accepted attributes conditionally will remove the pending review status and no user intervention is not required
                // fixing in rf script is more suitable as we are setting the fields into db the code will work without any changes
                var rcPresent = Array.isArray(f.requirementControlPresentInRFyear) && f.requirementControlPresentInRFyear.length > 0
                    ? f.requirementControlPresentInRFyear[0]
                    : null;

                if (
                    f.status === 'StatusType_Draft' &&
                    rcPresent &&
                    rcPresent.baselineModifiedAttributes &&
                    Object.keys(rcPresent.baselineModifiedAttributes).length > 0
                ) {
                    f.baselineModifiedAttributes = rcPresent.baselineModifiedAttributes;
                    f.baselineAcceptedAttributes = rcPresent.baselineModifiedAttributes;
                }
                else{
                    f.baselineModifiedAttributes = {};
                    f.baselineAcceptedAttributes = {};
                }
                var newFY_requirementControl = db.documentation.findOne({ prevId: f.requirementControlId })
                if (newFY_requirementControl !== null) {
                    f.requirementControlId = newFY_requirementControl._id.toString().replace(/ObjectId\("(.*)"\)/, "$1");
                }
                delete f.requirementControlPresentInRFyear;
                delete f.requirementControlPresentInRFyearSize;
                db.documentation.insertOne(f);
            }
        });
        //});
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Requirement control assignments ",error);
        throw (error);
    }
    // Action rollforward
    try {
        db.action.aggregate(
            [
                {
                    $match: {
                        firmId: { $eq: getNextRec.abbreviation },
                        fiscalYear: { $eq: getNextRec.fiscalYear }
                    }
                },
                {
                    $lookup: {
                        from: 'documentation',
                        let: { objectId: '$objectId', fiscalYear: '$fiscalYear' },
                        pipeline: [{
                            $match: {
                                $expr: {
                                    $or: [{ $and: [{ $eq: ['$$objectId', '$uniqueId'] }, { $eq: ['$$fiscalYear', '$fiscalYear'] }] }, { $eq: ['$$objectId', '$prevId'] }]
                                }
                            }
                        }],
                        as: 'docu'
                    }
                },
                {
                    $unwind: '$docu'
                },
                {
                    $addFields: {
                        fiscalYear: { $add: ['$fiscalYear', 1] },
                        prevId: { $toString: '$_id' },
                        ctrlStateUnchangedFromPrevYear: true,
                        rollForwardReviewDate: '',
                        objectId: { $cond: { if: { $ne: ['$objectType', 'QualityRisk'] }, then: { $toString: '$docu._id' }, else: '$objectId' } },
                        createdBy:'SYSTEM_ROLLFORWARD',
                        createdOn:new Date().toISOString(),
                        modifiedBy:'SYSTEM_ROLLFORWARD',
                        modifiedOn:new Date().toISOString()
                    }
                },
                {
                    $project: {
                        _id: 0,
                        id: 0,
                        docu: 0
                    }
                }
            ],
            {
                "allowDiskUse": true
            }
        ).forEach(function (f) {
            db.action.insertOne(f)
        });
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Action ",error);
        throw (error);
    }
    //Local Alternate Object roll forward
    try {
        if (db.localalternateobject.find().count()) {
            //rolledForwardEntitiesForBkcAssignments.forEach(re => {
            db.localalternateobject.aggregate(
                [
                    {
                        $match: {
                            firmId: { $eq: getNextRec.abbreviation },
                            fiscalYear: { $eq: getNextRec.fiscalYear }
                        }
                    },
                    {
                        $lookup: {
                            from: 'documentation',
                            let: {
                                objectId: '$objectId'
                            },
                            pipeline: [{
                                $match: {
                                    $expr: {
                                        $and: [
                                            { $eq: ['$fiscalYear', getNextRec.fiscalYear + 1] },
                                            { $eq: ['$prevId', '$$objectId'] }
                                        ]
                                    }
                                }
                            }],
                            as: 'docu'
                        }
                    },
                    {
                        $unwind: '$docu'
                    },
                    {
                        $addFields: {
                            fiscalYear: { $add: ['$fiscalYear', 1] },
                            prevId: { $toString: '$_id' },
                            additionalQOs: { $cond: { if: '$additionalQOs', then: '$additionalQOs', else: [] } },
                            additionalQRs: { $cond: { if: '$additionalQRs', then: '$additionalQRs', else: [] } },
                            isAdditionalQOQR: { $cond: { if: '$isAdditionalQOQR', then: '$isAdditionalQOQR', else: false} },
                            objectId: { $toString: '$docu._id' },
                            createdBy:'SYSTEM_ROLLFORWARD',
                            createdOn:new Date().toISOString(),
                            modifiedBy:'SYSTEM_ROLLFORWARD',
                            modifiedOn:new Date().toISOString()
                        }
                    },
                    {
                        $project: {
                            _id: 0,
                            id: 0,
                            docu: 0
                        }
                    }
                ],
                {
                    "allowDiskUse": true
                }
            ).forEach(function (f) {
                db.localalternateobject.insertOne(f)
            });
            //})
        }
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Local Alternate Object ",error);
        throw (error);
    }
    //removed deleted BKC from BKCC assignments
    try {
        db.documentation.aggregate([{
            $match: {
                type: 'RequirementControlAssignment',
                fiscalYear: { $eq: getNextRec.fiscalYear }
            }
        }, {
            $lookup: {
                from: 'documentation',
                let: {
                    requirementControlId: '$requirementControlId'
                },
                pipeline: [{
                    $match: {
                        type: 'RequirementControl'
                    }
                }, {
                    $match: {
                        $expr: {
                            $eq: [{ $toString: '$_id' }, '$$requirementControlId']
                        }
                    }
                }],
                as: 'requirementcontrol'
            }
        }, {
            $match: {
                $expr: {
                    $eq: [{ $size: { $cond: { if: { $isArray: '$requirementcontrol' }, then: '$requirementcontrol', else: [] } } }, 0]
                }
            }
        }]).forEach(assignment => {
            db.bkcassignment.updateMany(
                { "assignments": { $elemMatch: { "executingEntityId": assignment.firmId, "bkcId": assignment.uniqueId } } },
                {$set: {"modifiedBy": 'SYSTEM_ROLLFORWARD',"modifiedOn":new Date().toISOString()}}
            );
            db.bkcassignment.updateMany(
                { "assignments": { $elemMatch: { "executingEntityId": assignment.firmId, "bkcId": assignment.uniqueId } } },
                { $pull: { "assignments": { "executingEntityId": assignment.firmId, "bkcId": assignment.uniqueId } } }
            );
            db.documentation.remove({ _id: assignment._id });
        });
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at Remove deleted BKC from BKCC assignments ",error);
        throw (error);
    }
    // BKC assignments 
    try {
        //rolledForwardEntitiesForBkcAssignments.forEach(re=> { 
        db.bkcassignment.aggregate(
            [
                {
                    $match: {
                        'assignments.executingEntityId': { $eq: getNextRec.abbreviation },
                        'fiscalYear': { $eq: getNextRec.fiscalYear }
                    }
                }
            ]
        ).forEach(function (f) {
            var filteredAssignments = f.assignments.filter(assign => getNextRec.abbreviation === assign.executingEntityId);
            var bkcassignmentexists = db.bkcassignment.findOne({ firmId: f.firmId, fiscalYear: f.fiscalYear + 1 })
            if (bkcassignmentexists) {
                db.bkcassignment.updateOne({ _id: bkcassignmentexists._id }, { $addToSet: { assignments: { $each: filteredAssignments } } , $set:{modifiedBy:'SYSTEM_ROLLFORWARD', modifiedOn:new Date().toISOString() }});
                // printjson(db.bkcassignment.findOne({_id:bkcassignmentexists._id}))
            }
            else {
                delete f._id;
                delete f.id;
                f.fiscalYear = f.fiscalYear + 1;
                f.assignments = filteredAssignments;
                f.createdBy = 'SYSTEM_ROLLFORWARD';
                f.createdOn = new Date().toISOString();
                f.modifiedBy = 'SYSTEM_ROLLFORWARD';
                f.modifiedOn = new Date().toISOString();
                db.bkcassignment.insertOne(f);
                // printjson(db.bkcassignment.findOne({fiscalYear:f.fiscalYear+1, firmId : f.firmId}))
            }
        })
        //})
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at BKC assignments ",error);
        throw (error);
    }

    try {
        db.firm.aggregate([
            {
                $match: {
                    firmGroupId: getNextRec.abbreviation,
                    type: 'EntityType_MemberFirm',
                    fiscalYear: getNextRec.fiscalYear
                }
            },
            {
                $lookup: {
                    from: 'title',
                    let:
                    {
                        ultimateResponsibility: '$ultimateResponsibility'
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $in: ['$prevId', '$$ultimateResponsibility']
                            }
                        }
                    }],
                    as: 'ultimateResponsibility2024'
                }
            },
        ]).forEach(function (f) {
            if (Array.isArray(f.ultimateResponsibility2024)) {
                const newUltimateResponsibility = f.ultimateResponsibility2024.map(item => item._id.toString());
                db.firm.updateOne(
                    { prevId: f._id.toString().replace(/ObjectId\("(.*)"\)/, "$1") },
                    { 
                        $set: { 
                            ultimateResponsibility: newUltimateResponsibility, 
                            modifiedBy: 'SYSTEM_ROLLFORWARD', 
                            modifiedOn: new Date().toISOString() 
                        } 
                    }
                );
            }
        })


    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at ultimate Responsibility ",error);
        throw (error);
    }

    //Update the MemberFirm SQM LeaderShip Objects -- orIndependenceRequirement
    try {
        db.firm.aggregate([
            {
                $match: {
                    firmGroupId: getNextRec.abbreviation,
                    type: 'EntityType_MemberFirm',
                    fiscalYear: getNextRec.fiscalYear
                }
            },
            {
                $lookup: {
                    from: 'title',
                    let:
                    {
                        orIndependenceRequirement: { $toString: '$orIndependenceRequirement' }
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $eq: ['$prevId', '$$orIndependenceRequirement']
                            }
                        }
                    }],
                    as: 'orIndependenceRequirement2024'
                }
            },
            {
                $unwind: '$orIndependenceRequirement2024'
            },
        ]).forEach(function (f) {
            db.firm.updateOne({ prevId: f._id.toString().replace(/ObjectId\("(.*)"\)/, "$1") }, { $set: { orIndependenceRequirement: f.orIndependenceRequirement2024._id, modifiedBy:'SYSTEM_ROLLFORWARD', modifiedOn:new Date().toISOString() } })
        })


    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at orIndependenceRequirement ",error);
        throw (error);
    }

    //Update the MemberFirm SQM LeaderShip Objects -- orMonitoringRemediation
    try {
        db.firm.aggregate([
            {
                $match: {
                    firmGroupId: getNextRec.abbreviation,
                    type: 'EntityType_MemberFirm',
                    fiscalYear: getNextRec.fiscalYear
                }
            },
            {
                $lookup: {
                    from: 'title',
                    let:
                    {
                        orMonitoringRemediation: { $toString: '$orMonitoringRemediation' }
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $eq: ['$prevId', '$$orMonitoringRemediation']
                            }
                        }
                    }],
                    as: 'orMonitoringRemediation2024'
                }
            },
            {
                $unwind: '$orMonitoringRemediation2024'
            },
        ]).forEach(function (f) {
            db.firm.updateOne({ prevId: f._id.toString().replace(/ObjectId\("(.*)"\)/, "$1") }, { $set: { orMonitoringRemediation: f.orMonitoringRemediation2024._id, modifiedBy:'SYSTEM_ROLLFORWARD', modifiedOn:new Date().toISOString() } })
        })


    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at orMonitoringRemediation ",error);
        throw (error);
    }

    //Update the MemberFirm SQM LeaderShip Objects -- operationalResponsibilitySqm
    try {
        db.firm.aggregate([
            {
                $match: {
                    firmGroupId: getNextRec.abbreviation,
                    type: 'EntityType_MemberFirm',
                    fiscalYear: getNextRec.fiscalYear
                }
            },
            {
                $lookup: {
                    from: 'title',
                    let:
                    {
                        operationalResponsibilitySqm: '$operationalResponsibilitySqm'
                    },
                    pipeline: [{
                        $match: {
                            $expr: {
                                $in: ['$prevId', '$$operationalResponsibilitySqm']
                            }
                        }
                    }],
                    as: 'operationalResponsibilitySqm2024'
                }
            },
        ]).forEach(function (f) {
            if (Array.isArray(f.operationalResponsibilitySqm2024)) {
                const newOperationalResponsibilitySqm = f.operationalResponsibilitySqm2024.map(item => item._id.toString());
                db.firm.updateOne(
                    { prevId: f._id.toString().replace(/ObjectId\("(.*)"\)/, "$1") },
                    { 
                        $set: { 
                            operationalResponsibilitySqm: newOperationalResponsibilitySqm, 
                            modifiedBy: 'SYSTEM_ROLLFORWARD', 
                            modifiedOn: new Date().toISOString() 
                        } 
                    }
                );
            }  
        })
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at operationalResponsibilitySqm ",error);
        throw (error);
    }
    //To set the flag roll-Forward flags  to MemberFirms while the Location rollsforward
    try {
        db.firm.aggregate([
            {
                $match: {
                    type: 'EntityType_Group',
                    abbreviation: getNextRec.abbreviation
                }
            }
        ]).forEach(function (f) {
            db.firm.updateMany({ firmGroupId: f.abbreviation, fiscalYear: getNextRec.fiscalYear }, { $set: { isRollFwdComplete: true, rollForwardStatus: 'RollForward_Complete', isRollForwardTriggered: true, rollForwardDate: new Date().toISOString(), modifiedBy:'SYSTEM_ROLLFORWARD', modifiedOn:new Date().toISOString() } })
            db.firm.updateMany({ firmGroupId: f.abbreviation, fiscalYear: getNextRec.fiscalYear + 1 }, { $set: { isRollForwardedFromPreFY: true, modifiedBy:'SYSTEM_ROLLFORWARD', modifiedOn:new Date().toISOString() } });            
            db.event.insertOne({actor: f._id,publisher:f.type,message : "ActionType_Rollforward",fiscalYear : f.fiscalYear, actorType : "firm", modifiedBy : "SYSTEM_ROLLFORWARD", modifiedOn : new Date().toISOString(), createdOn : new Date().toISOString(), createdBy : "SYSTEM_ROLLFORWARD"});
        })
    } catch (error) {
        print("SYSTEM:RollForward-Error :: Error at MemberFirms ",error);
        throw (error);
    }

    //The below migartion script is added to set pcaob/isqm values to documentation objects for RF b/w FY25--FY26 
    //The script will be removed for the next upcoming RF and added at appropriate place while RF of documentation objects
    try{
    //QualityRisk
    db.documentation.find({fiscalYear:getNextRec.fiscalYear+1,firmId:getNextRec.abbreviation,type:'QualityRisk'}).forEach(function(qr){
        var pcaobValues={relatedObjectives:[]};
        var isqmValues={relatedObjectives:[]};
        var relatedObjectives=qr.relatedObjectives;
        relatedObjectives && relatedObjectives.forEach(function(obj){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:obj,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedObjectives.push(obj):isqmValues.relatedObjectives.push(obj)
        })
        db.documentation.updateOne({_id:qr._id},{$set:{'pcaobValues':pcaobValues,'isqmValues':isqmValues}})
    })

    //QualitySubRisk
    db.documentation.find({fiscalYear:getNextRec.fiscalYear+1,firmId:getNextRec.abbreviation,type:'SubRisk'}).forEach(function(qsr){
        var pcaobValues={relatedObjectives:[],relatedQualityRisks:[]};
        var isqmValues={relatedObjectives:[],relatedQualityRisks:[]};
        var relatedObjectives=qsr.relatedObjectives;
        var relatedQualityRisks=qsr.relatedQualityRisks;
        relatedObjectives && relatedObjectives.forEach(function(obj){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:obj,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedObjectives.push(obj):isqmValues.relatedObjectives.push(obj)
        })
        relatedQualityRisks && relatedQualityRisks.forEach(function(qr){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:qr,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedQualityRisks.push(qr):isqmValues.relatedQualityRisks.push(qr)
        })
        db.documentation.updateOne({_id:qsr._id},{$set:{'pcaobValues':pcaobValues,'isqmValues':isqmValues}})
    })

    //Resource
    db.documentation.find({fiscalYear:getNextRec.fiscalYear+1,firmId:getNextRec.abbreviation,type:'Resource'}).forEach(function(res){
       var pcaobValues={relatedQualityRisks:[],relatedQualityRisksMitigatedByResource:[]};
        var isqmValues={relatedQualityRisks:[],relatedQualityRisksMitigatedByResource:[]};
        var relatedQualityRisks=res.relatedQualityRisks;
        var relatedQualityRisksMitigatedByResource=res.relatedQualityRisksMitigatedByResource;
        relatedQualityRisks && relatedQualityRisks.forEach(function(res){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:res,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedQualityRisks.push(res):isqmValues.relatedQualityRisks.push(res);
        })
        relatedQualityRisksMitigatedByResource && relatedQualityRisksMitigatedByResource.forEach(function(res){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:res,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedQualityRisksMitigatedByResource.push(res):isqmValues.relatedQualityRisksMitigatedByResource.push(res);
        })
        db.documentation.updateOne({_id:res._id},{$set:{'pcaobValues':pcaobValues,'isqmValues':isqmValues}})
    })

    //Controls
     db.documentation.find({fiscalYear:getNextRec.fiscalYear+1,firmId:getNextRec.abbreviation,type:{$in:['KeyControl','RequirementControl','RequirementControlAssignment']}}).forEach(function(kc){
        var pcaobValues={relatedObjectives:[],relatedQualityRisks:[],relatedSubRisks:[],relatedProcesses:[],mitigatedResources:[],supportingITApplication:[], addInformationExecutionControls: []};
        var isqmValues={relatedObjectives:[],relatedQualityRisks:[],relatedSubRisks:[],relatedProcesses:[],mitigatedResources:[],supportingITApplication:[], addInformationExecutionControls: []};
        var relatedObjectives=kc.relatedObjectives;
        var relatedQualityRisks=kc.relatedQualityRisks;
        var relatedSubRisks=kc.relatedSubRisks;
        var mitigatedResources=kc.mitigatedResources;
        var supportingITApplication=kc.supportingITApplication;
        var controls = kc.addInformationExecutionControls;
        relatedObjectives && relatedObjectives.forEach(function(obj){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:obj,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedObjectives.push(obj):isqmValues.relatedObjectives.push(obj)
        })
        relatedQualityRisks && relatedQualityRisks.forEach(function(qr){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:qr,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedQualityRisks.push(qr):isqmValues.relatedQualityRisks.push(qr);
        })
        relatedSubRisks && relatedSubRisks.forEach(function(qsr){
            var isPCAOBRegistered=db.documentation.findOne({uniqueId:qsr,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedSubRisks.push(qsr):isqmValues.relatedSubRisks.push(qsr);
        })
        mitigatedResources && mitigatedResources.forEach(function(res){
            var isPCAOBRegistered=db.documentation.findOne({_id:ObjectId(res),fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.mitigatedResources.push(res):isqmValues.mitigatedResources.push(res);
        })
        supportingITApplication && supportingITApplication.forEach(function(res){
            var isPCAOBRegistered=db.documentation.findOne({_id:ObjectId(res),fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.supportingITApplication.push(res):isqmValues.supportingITApplication.push(res);
        })
        var relatedProcesses=db.firmprocess.find({mappedKeyControls:{$in:[kc.uniqueId]},fiscalYear:getNextRec.fiscalYear+1,firmId:getNextRec.abbreviation}).map(function(process){
            return process.processId;
        })
        relatedProcesses && relatedProcesses.forEach(function(process){
            var isPCAOBRegistered=db.globaldocumentation.findOne({uniqueId:process,fiscalYear:getNextRec.fiscalYear+1}).isPCAOBRegistered;
            isPCAOBRegistered?pcaobValues.relatedProcesses.push(process):isqmValues.relatedProcesses.push(process);
        }) 
        controls.forEach(function (control) {
            if (kc.controlType !== 'ControlType_ItDependentManual') {
                isqmValues.addInformationExecutionControls.push(control);
                pcaobValues.addInformationExecutionControls.push(control);
                return;
            }
            var isqmControl = {};
            var pcaobControl = {};
            for (var key in control) {
            if (control.hasOwnProperty(key)) {
                isqmControl[key] = key === 'ipeSystems' ? [] : control[key];
                pcaobControl[key] = key === 'ipeSystems' ? [] : control[key];
            }
            }
            control.ipeSystems.forEach(function (ipeSystemId) {
            var ipeDoc = db.documentation.findOne({ _id: ObjectId(ipeSystemId), fiscalYear: getNextRec.fiscalYear+1 });
            if(ipeDoc){
                if (ipeDoc.isPCAOBRegistered) {
                    pcaobControl.ipeSystems.push(ipeSystemId);
                } 
                else {
                    isqmControl.ipeSystems.push(ipeSystemId);
                }
            }
            });
            isqmValues.addInformationExecutionControls.push(isqmControl);
            pcaobValues.addInformationExecutionControls.push(pcaobControl);
        });
        db.documentation.updateOne({_id:kc._id},{$set:{'pcaobValues':pcaobValues,'isqmValues':isqmValues}})
    })

    }catch(error){
        print("SYSTEM:RollForward-Error :: Error at Migration script while setting pcaobValues and isqmValues for documentation objects ", error);
        throw (error);
    }
    //Update firm isRollForwardedFromPreFY
    //rolledForwardEntitiesForBkcAssignments.forEach(re => {
    db.firm.aggregate([{
        $match: {
            abbreviation: {
                $eq: getNextRec.abbreviation
            },
            fiscalYear: { $eq: getNextRec.fiscalYear + 1 }
        }
    }]).forEach(function (f) {
        db.firm.updateOne({ abbreviation: f.abbreviation, fiscalYear: f.fiscalYear }, { $set: { isRollForwardedFromPreFY: true, modifiedBy:'SYSTEM_ROLLFORWARD', modifiedOn:new Date().toISOString() } })
    });
    
    //rollforward Entity which created after network RollForward
    //rolledForwardEntitiesForBkcAssignments.forEach(re => {
   /* if (db.firm.find({ abbreviation: { $eq: getNextRec.abbreviation }, fiscalYear: getNextRec.fiscalYear + 1 }).count() == 0) {
        db.firm.aggregate([
            {
                $match: {
                    $expr: {
                        $and: [
                            { $or: [{ $eq: ['$abbreviation', getNextRec.abbreviation] }, { $eq: ['$firmGroupId', getNextRec.abbreviation] }] },
                            { $eq: ['$fiscalYear', getNextRec.fiscalYear] }
                        ]
                    }
                }
            },
            {
                $addFields: {
                    fiscalYear: { $add: ['$fiscalYear', 1] },
                    prevId: { $toString: '$_id' },
                    isRollForwardTriggered: false,
                    isRollFwdComplete: false,
                    rollForwardDate: '',
                    rollForwardStatus: '',
                    rollForwardByEmail: '',
                    rollForwardByDisplayName: '',
                    isRollForwardedFromPreFY: true,
                    createdBy:'SYSTEM_ROLLFORWARD',
                    createdOn:new Date().toISOString(),
                    modifiedBy:'SYSTEM_ROLLFORWARD',
                    modifiedOn:new Date().toISOString()
                }
            }, {
                $project: {
                    _id: 0,
                    id: 0,
                    ipeSystems: 0
                }
            }
        ]).forEach(function (f) {
            db.firm.insertOne(f);
        })
    }  */
    //});
    
    db.firm.updateOne({ _id: getNextRec._id }, { $set: { isRollFwdComplete: true, rollForwardStatus: 'RollForward_Complete', modifiedBy: 'SYSTEM_ROLLFORWARD', modifiedOn: new Date().toISOString() } });
    if (getNextRec.isPartOfRestructure === undefined || !getNextRec.isPartOfRestructure) {
      //  db.firm.updateOne({ abbreviation: getNextRec.abbreviation, fiscalYear: getNextRec.fiscalYear + 1 }, { $set: { publishedDate: getNextRec.rollForwardDate, publishedBy: getNextRec.rollForwardByEmail, publishedUserDisplayName: getNextRec.rollForwardByDisplayName, isPublishQueryRun: false } });
    }
    db.log.insertOne({ message: 'Rollforward Completed - Abbreviation ' + getNextRec.abbreviation + ' FiscalYear : ' + getNextRec.fiscalYear + ' RollforwardIntiatedBy : ' + getNextRec.rollForwardByEmail + ' RollforwardIntiatedOn : ' + getNextRec.rollForwardDate, text: new Date().toISOString() });
    print('Rollforward completed for ....... ', getNextRec._id, getNextRec.abbreviation, new Date().toISOString());
});
print('RF batch executed ......... ',formatTimestamp(new Date()));
}
catch (error) {
    db.log.insertOne({ message: 'Rollforward Failed - Abbreviation ' + getNextRec.abbreviation + ' FiscalYear : ' + getNextRec.fiscalYear + ' RollforwardIntiatedBy : ' + getNextRec.rollForwardByEmail + ' RollforwardIntiatedOn : ' + getNextRec.rollForwardDate + ' ERROR : ' + error.toString(), text: new Date().toISOString() });
    //db.firm.updateOne({_id:processingData._id},{$set:{"errorResponse":error.toString(),rollForwardStatus:'Rollforward_Failed'}});
    print("SYSTEM:RollForward-Error - The RollForward errored with following issue ................., ERROR :: ", error.toString());
}
