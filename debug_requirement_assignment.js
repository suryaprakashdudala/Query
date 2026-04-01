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

print("reBacPolicyQOIds (these should be filtered out): " + JSON.stringify(reBacPolicyQOIds));

// --- STEP 2: AGGREGATION ---
print("Generating debug data for: " + TARGET_REF_UNIQUE_ID);

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
    
    // Simulating the QO lookup that overwrites relatedObjectives with objects
    {
        $lookup: {
            from: 'documentation',
            let: {
                ids: '$requirementcontrol.relatedObjectives', // Initial strings
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
            as: 'requirementcontrol.relatedObjectives' // Now objects!
        }
    },

    // --- LOGIC FROM sqmarcherkeycontrol.js ---

    // Stage 1: Initial objectives assignment (line 1324)
    {
        $addFields: {
            'objectives_stage_1': {
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
                            else: [] // In real script this flattens relatedQualityRisks
                        }
                    },
                    else: []
                }
            }
        }
    },

    // Stage 2: reBac filtering (line 1351)
    {
        $addFields: {
            'objectives_stage_2': {
                $cond: {
                    if: { $in: ['$abbreviation', autoQoNotReqFirms] },
                    then: {
                        $filter: {
                            input: { $ifNull: ['$objectives_stage_1', []] },
                            as: 'qo',
                            cond: {
                                $not: {
                                    $in: ['$$qo', reBacPolicyQOIds]
                                }
                            }
                        }
                    },
                    else: '$objectives_stage_1'
                }
            }
        }
    },

    // Stage 3: Final QualityObjectiveUniquesIds calculation (similar to line 1798)
    {
        $lookup: {
            from: 'documentation',
            let: { fiscalYearOfFirm: '$fiscalYear', relatedObjectives: '$objectives_stage_2' },
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
            isQoOverrideEnabled: '$assignment.isQoOverrideEnabled',
            is_USA: { $in: ['$abbreviation', autoQoNotReqFirms] },
            
            objectives_stage_1_content: '$objectives_stage_1',
            objectives_stage_2_content: '$objectives_stage_2',
            
            QualityObjectiveUniquesIds: {
                $reduce: {
                    input: '$associatedQualityObjectives',
                    initialValue: '',
                    in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.qualityObjectiveId'] }
                }
            }
        }
    },
    { $out: 'debug_requirement_assignment_results' }
]);

print("Debug results written. Checking output...");
var result = db.debug_requirement_assignment_results.findOne();
if (result) {
    print("--- DEBUG RESULT ---");
    print("Entity: " + result.EntityId);
    print("isQoOverrideEnabled: " + result.isQoOverrideEnabled);
    print("Objectives after Stage 1 (Initial): " + JSON.stringify(result.objectives_stage_1_content));
    print("Objectives after Stage 2 (reBac Filtered): " + JSON.stringify(result.objectives_stage_2_content));
    print("Final QualityObjectiveUniquesIds: " + result.QualityObjectiveUniquesIds);
    
    // Check for type mismatch
    if (result.objectives_stage_1_content && result.objectives_stage_1_content.length > 0) {
        var firstItem = result.objectives_stage_1_content[0];
        if (typeof firstItem === 'object') {
            print("\nWARNING: Type Mismatch detected! objectives_stage_1 contains OBJECTS.");
            print("Comparison with reBacPolicyQOIds (strings) will fail at Stage 2.");
        } else {
            print("\nOK: objectives_stage_1 contains STRINGS.");
        }
    }
} else {
    print("Error: No record found for the target filter.");
}
