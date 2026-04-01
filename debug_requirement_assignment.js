// Config for debugging
var TARGET_ENTITY_ID = ["USA"]; 
var TARGET_REF_UNIQUE_ID = "RC-NTW-452"; 

// --- STEP 1: INITIALIZE ENVIRONMENT ---
var fiscalYearFilter = [2026];
var autoQoNotReqFirms = ['USA'];
var globalQOApplicabilityPolicyId = 'EXC-GLOBAL-LOC-GLOBAL-USA';

var reBacPolicyQOIds = [];
if (globalQOApplicabilityPolicyId) {
    reBacPolicyQOIds = db.rebacpolicy.distinct('objectId', {
        fiscalYear: { $in: fiscalYearFilter },
        policyId: globalQOApplicabilityPolicyId,
        active: true
    });
}

print("reBacPolicyQOIds: " + JSON.stringify(reBacPolicyQOIds));

// --- STEP 2: AGGREGATION ---
db.debug_requirement_assignment_results.drop();

db.firm.aggregate([
    {
        $match: {
            abbreviation: { $in: TARGET_ENTITY_ID },
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
                                { $eq: ['$uniqueId', TARGET_REF_UNIQUE_ID] }
                            ]
                        }
                    }
                }
            ],
            as: 'assignment'
        }
    },
    { $unwind: '$assignment' },
    {
        $lookup: {
            from: 'documentation',
            let: {
                reqCtrlId: '$assignment.requirementControlId',
                fiscalYearOfFirm: '$fiscalYear'
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $eq: ['$type', 'RequirementControl'] },
                                { $eq: [{ $toString: '$_id' }, '$$reqCtrlId'] },
                                { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] }
                            ]
                        }
                    }
                }
            ],
            as: 'requirementcontrol'
        }
    },
    { $unwind: '$requirementcontrol' },

    // Pre-processing to match sqmarcherkeycontrol.js state
    {
        $lookup: {
            from: 'documentation',
            let: {
                riskIds: '$requirementcontrol.relatedQualityRisks',
                fiscalYearOfFirm: '$fiscalYear'
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $in: ['$uniqueId', { $ifNull: ['$$riskIds', []] }] },
                                { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] }
                            ]
                        }
                    }
                }
            ],
            as: 'requirementcontrol.relatedQualityRisks'
        }
    },
    {
        $lookup: {
            from: 'documentation',
            let: {
                ids: '$requirementcontrol.relatedObjectives',
                fiscalYearOfFirm: '$fiscalYear'
            },
            pipeline: [
                {
                    $match: {
                        $expr: {
                            $and: [
                                { $in: ['$uniqueId', { $ifNull: ['$$ids', []] }] },
                                { $eq: ['$fiscalYear', '$$fiscalYearOfFirm'] }
                            ]
                        }
                    }
                }
            ],
            as: 'requirementcontrol.relatedObjectives'
        }
    },

    // --- REPLICATING logic from sqmarcherkeycontrol.js line 1324-1370 ---
    {
        $addFields: {
            'objectives': {
                $cond: {
                    if: { $in: ['$abbreviation', autoQoNotReqFirms] },
                    then: {
                        $cond: {
                            if: {
                                $not: {
                                    $or: [
                                        { $eq: ['$assignment.isQoOverrideEnabled', undefined] },
                                        { $eq: ['$assignment.isQoOverrideEnabled', ''] },
                                        { $eq: ['$assignment.isQoOverrideEnabled', null] }
                                    ]
                                }
                            },
                            then: '$assignment.relatedObjectives',
                            else: {
                                $reduce: {
                                    input: '$requirementcontrol.relatedQualityRisks.relatedObjectives', // Array of arrays of strings
                                    initialValue: [],
                                    in: { $concatArrays: ['$$value', '$$this'] }
                                }
                            }
                        }
                    },
                    else: []
                }
            }
        }
    },
    {
        $addFields: {
            'objectives_stage_1_content': '$objectives'
        }
    },
    {
        $set: {
            "objectives": {
                $cond: {
                    if: { $in: ['$abbreviation', autoQoNotReqFirms] },
                    then: {
                        $filter: {
                            input: { $ifNull: ['$objectives', []] },
                            as: 'qo',
                            cond: {
                                $not: {
                                    $in: ['$$qo', reBacPolicyQOIds]
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
            EntityId: '$abbreviation',
            Ref_UniqueId: '$assignment.uniqueId',
            isQoOverrideEnabled_raw: '$assignment.isQoOverrideEnabled',
            is_USA: { $in: ['$abbreviation', autoQoNotReqFirms] },
            objectives_stage_1_content: '$objectives_stage_1_content',
            objectives_after_filter: '$objectives',
            associatedQualityObjectives: '$associatedQualityObjectives',
            QualityObjectiveUniquesIds: {
                $reduce: {
                    input: '$associatedQualityObjectives',
                    initialValue: '',
                    in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.qualityObjectiveId'] }
                }
            }
        }
    },
    // { $out: 'debug_requirement_assignment_results' }
]);

print("Debug results written.");
// var result = db.debug_requirement_assignment_results.findOne();
// print(JSON.stringify(result, null, 2));
