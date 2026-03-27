var TARGET_ENTITY_ID = ["USA",'NTW']; // e.g., "USA"
var TARGET_REF_UNIQUE_ID = "RC-NTW-454"; // e.g., "CTRL-12345"

// --- STEP 1: INITIALIZE ENVIRONMENT ---
var fiscalYearFilter = [2026];

var publishedEntityIds = db.firm.aggregate([{
    $match: {
        $expr: {
            $and: [
                '$publishedDate',
                { $ne: ['$publishedDate', ''] },
                { $in: ["$fiscalYear", fiscalYearFilter] },
                { $in: ['$abbreviation',TARGET_ENTITY_ID] } // Filter for debugging
            ]
        }
    }
},
{
    $project: { abbreviation: 1, fiscalYear: 1, _id: 0 }
}]).toArray();


// var iecIpeCategoryResources = db.keycontrolresource.find({ "type": "IpeCategory", fiscalYear: { $in: fiscalYearFilter } }, { _id: 1, name: 1 }).toArray();
// var iecReportNameResources = db.keycontrolresource.find({ "type": "IecReport", fiscalYear: { $in: fiscalYearFilter } }, { _id: 1, name: 1 }).toArray();

// --- STEP 2: GENERATE TEMP DATA FOR TARGET RECORD ---
print("Generating debug temp data for: " + TARGET_REF_UNIQUE_ID + " (" + TARGET_ENTITY_ID + ")");

db.sqmarcherkeycontroltempdebug.drop();

db.firm.aggregate([
    {
        $match: {
            abbreviation: {$in: TARGET_ENTITY_ID},
            fiscalYear: { $in: fiscalYearFilter }
        }
    },
    {
        $lookup: {
            from: 'documentation',
            let: { firmId: '$abbreviation', fiscalYearOfFirm: '$fiscalYear' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$type', 'RequirementControlAssignment'] },
                                { $eq: ['$firmId', '$$firmId'] },
                                { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] },
                                { $eq: ['$uniqueId', TARGET_REF_UNIQUE_ID] } // TARGET FILTER
                            ]
                        }
                    }
                }
            ],
            as: 'keyControl'
        }
    },
    { $unwind: '$keyControl' },
    // Simplified stages for debugging QualityObjectiveUniquesIds
    {
        $addFields: {
            'objectives': {
                $cond: {
                    if: { $not: { $or: [{ $eq: ['$keyControl.isQoOverrideEnabled', undefined] }, { $eq: ['$keyControl.isQoOverrideEnabled', ''] }, { $eq: ['$keyControl.isQoOverrideEnabled', null] }] } },
                    then: '$keyControl.relatedObjectives',
                    else: {
                        $reduce: {
                            input: { $ifNull: ['$keyControl.relatedObjectives', []] },
                            initialValue: [],
                            in: {
                                $concatArrays: [
                                '$$value',
                                {
                                    $cond: [
                                    { $eq: [{ $type: '$$this' }, 'array'] },
                                    '$$this',
                                    ['$$this']
                                    ]
                                }
                                ]
                            }
                        }
                    }
                }
            }
        }
    },
    
        {
            $lookup: {
                from: 'rebacpolicy',
                let: {
                    firmId: '$abbreviation',
                    objectives: '$objectives'
                },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$policyId', 'EXC-GLOBAL-LOC-GLOBAL-USA'] },
                                    { $eq: ['$$firmId', 'USA'] },
                                    { $eq: ['$toEntity', '$$firmId'] },
                                    {
                                        $in: ['$objectId', '$$objectives']
                                    }
                                ]
                            }
                        }
                    }
                ],
                as: 'rebacPoliciesRelatedToQOs'
            }
        },
    {
            $set: {
                'relatedQualityRisks': {
                    $cond: {
                        if: { $eq: ['$abbreviation', 'USA'] },
                        then: {
                            $filter: {
                                input: { $ifNull: ['$objectives', []] },
                                as: 'qr',
                                cond: {
                                    $let:{
                                        vars: {
                                            objectives: { $ifNull: ['$objectives', []] }
                                        },
                                        in: {
                                            $anyElementTrue: {
                                                $map: {
                                                    input: '$$objectives',
                                                    as: 'objId',
                                                    in: {
                                                        $not: {
                                                            $in: ['$$objId', '$rebacPoliciesRelatedToQOs.objectId']
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        else: '$objectives'
                    }
                }
            }
        },
    {
        $lookup: {
            from: 'documentation',
            let: { fiscalYearOfFirm: '$fiscalYear', relatedObjectives: '$objectives' },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ["$fiscalYear", '$$fiscalYearOfFirm'] },
                                { $in: ['$uniqueId', { $ifNull: ['$$relatedObjectives', []] }] }
                            ]
                        }
                    }
                },
                { $project: { _id: 0, qualityObjectiveId: 1, uniqueId: 1 } }
            ],
            as: 'associatedQualityObjectives'
        }
    },
    {
        $project: {
            _id: 0,
            Ref_UniqueId: '$keyControl.uniqueId',
            EntityId: '$abbreviation',
            FiscalYear: '$fiscalYear',
            associatedQualityObjectives: '$associatedQualityObjectives',
            relatedQualityRisks:1,
            QualityObjectiveUniquesIds: {
                $reduce: {
                    input: '$associatedQualityObjectives',
                    initialValue: '',
                    in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.qualityObjectiveId'] }
                }
            },
            rebacPoliciesRelatedToQOs: '$rebacPoliciesRelatedToQOs',
            qualityObjectiveUniqueIdArray: {
                    $setUnion: [{
                        $reduce: {
                            input: { $ifNull: ['$keyControl.relatedObjectives', []] },
                            initialValue: [],
                            in: {
                                $concatArrays: [
                                '$$value',
                                {
                                    $cond: [
                                    { $eq: [{ $type: '$$this' }, 'array'] },
                                    '$$this',
                                    ['$$this']
                                    ]
                                }
                                ]
                            }
                        }
                    }]
                },
        }
    },
    { $out: 'sqmarcherkeycontroltempdebug' }
]);

// // --- STEP 3: COMPARE WITH MASTER ---
// var kc = db.sqmarcherkeycontroltempdebug.findOne({ EntityId: {$in: TARGET_ENTITY_ID}, Ref_UniqueId: TARGET_REF_UNIQUE_ID });
// if (!kc) {
//     print("Error: Targeted record not found in temp collection. Check EntityId/Ref_UniqueId.");
//     quit();
// }

// var existingControl = db.sqmarcherkeycontrolmaster.findOne({ EntityId: kc.EntityId, Ref_UniqueId: kc.Ref_UniqueId, FiscalYear: 'FY' + String(kc.FiscalYear).slice(-2) });
// // Try alternate FiscalYear format if not found
// if (!existingControl) {
//     existingControl = db.sqmarcherkeycontrolmaster.findOne({ EntityId: kc.EntityId, Ref_UniqueId: kc.Ref_UniqueId, FiscalYear: kc.FiscalYear });
// }

// if (!existingControl) {
//     print("Warning: No existing record found in sqmarcherkeycontrolmaster for comparison.");
// } else {
//     print("\n--- COMPARISON RESULTS ---");
//     print("Current (Temp) QOs:  " + (kc.QualityObjectiveUniquesIds || "EMPTY"));
//     print("Existing (Master) QOs: " + (existingControl.QualityObjectiveUniquesIds || "EMPTY"));

//     var isUpdateFromOutside = []
//     var current_QOs = kc.qualityObjectiveUniqueIdArray ? kc.qualityObjectiveUniqueIdArray : [];
//     var existing_QOs = existingControl.qualityObjectiveUniqueIdArray ? existingControl.qualityObjectiveUniqueIdArray : [] ;

//     var differenceInQOs = [
//         ...current_QOs.filter(qo => !existing_QOs.includes(qo)),
//         ...existing_QOs.filter(qo => !current_QOs.includes(qo))
//     ];

//     var isQOUpdated = db.event.find({
//         fiscalYear: 2026,
//         actor: {$in: differenceInQOs}, actorType: 'QualityObjective',
//         message: {$in: ['ActionType_Delete','ActionType_Add']},
//         modifiedOn: { $gt: existingControl.ArcherPublishedOn }
//     }).sort({ 'modifiedOn': -1 }).limit(1).toArray();
//     if (isQOUpdated.length > 0) {
//         isUpdateFromOutside.push(isQOUpdated[0]);
//     }
//     print("Difference Array: " + JSON.stringify(isUpdateFromOutside));
//     // print("modifedon: "+ existingControl.ArcherPublishedOn)


//     if (differenceInQOs.length > 0) {
//         print("RESULT: CHANGES DETECTED in Quality Objectives.");
//     } else {
//         print("RESULT: NO CHANGES detected in Quality Objectives.");
//     }
// }
