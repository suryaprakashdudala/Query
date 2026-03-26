/**
 * DEBUG SCRIPT: SQM Archer Key Control QualityObjectiveUniquesIds Synchronization
 * 
 * INSTRUCTIONS:
 * 1. Set the TARGET_ENTITY_ID and TARGET_REF_UNIQUE_ID below.
 * 2. Run this script in the mongo shell:
 *    mongo isqc debug_sqmarchercontrol.js
 */

var TARGET_ENTITY_ID = "YOUR_ENTITY_ID"; // e.g., "USA"
var TARGET_REF_UNIQUE_ID = "YOUR_REF_UNIQUE_ID"; // e.g., "CTRL-12345"

// --- STEP 1: INITIALIZE ENVIRONMENT ---
var fiscalYearFilter = [2026];

var publishedEntityIds = db.firm.aggregate([{
    $match: {
        $expr: {
            $and: [
                '$publishedDate',
                { $ne: ['$publishedDate', ''] },
                { $in: ["$fiscalYear", fiscalYearFilter] },
                { $abbreviation: TARGET_ENTITY_ID } // Filter for debugging
            ]
        }
    }
},
{
    $project: { abbreviation: 1, fiscalYear: 1, _id: 0 }
}]).toArray();

if (publishedEntityIds.length === 0) {
    print("Warning: No published firm found for " + TARGET_ENTITY_ID + " in FY 2026. Checking all firms...");
    publishedEntityIds = db.firm.find({ abbreviation: TARGET_ENTITY_ID, fiscalYear: { $in: fiscalYearFilter } }, { abbreviation: 1, fiscalYear: 1, _id: 0 }).toArray();
}

var iecIpeCategoryResources = db.keycontrolresource.find({ "type": "IpeCategory", fiscalYear: { $in: fiscalYearFilter } }, { _id: 1, name: 1 }).toArray();
var iecReportNameResources = db.keycontrolresource.find({ "type": "IecReport", fiscalYear: { $in: fiscalYearFilter } }, { _id: 1, name: 1 }).toArray();

// --- STEP 2: GENERATE TEMP DATA FOR TARGET RECORD ---
print("Generating debug temp data for: " + TARGET_REF_UNIQUE_ID + " (" + TARGET_ENTITY_ID + ")");

db.sqmarcherkeycontroltemp.drop();

db.firm.aggregate([
    {
        $match: {
            abbreviation: TARGET_ENTITY_ID,
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
                                { $eq: ['$type', 'KeyControl'] },
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
                    then: '$keyControl.relatedObjectives', // Override Enabled
                    else: {
                        $reduce: {
                            input: { $ifNull: ['$keyControl.relatedQualityRisks.relatedObjectives', []] },
                            initialValue: [],
                            in: { $concatArrays: ['$$value', '$$this'] }
                        }
                    }
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
            QualityObjectiveUniquesIds: {
                $reduce: {
                    input: '$associatedQualityObjectives',
                    initialValue: '',
                    in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: ';' } }, '$$this.qualityObjectiveId'] }
                }
            }
        }
    },
    { $out: 'sqmarcherkeycontroltemp' }
]);

// --- STEP 3: COMPARE WITH MASTER ---
var kc = db.sqmarcherkeycontroltemp.findOne({ EntityId: TARGET_ENTITY_ID, Ref_UniqueId: TARGET_REF_UNIQUE_ID });
if (!kc) {
    print("Error: Targeted record not found in temp collection. Check EntityId/Ref_UniqueId.");
    quit();
}

var existingControl = db.sqmarcherkeycontrolmaster.findOne({ EntityId: kc.EntityId, Ref_UniqueId: kc.Ref_UniqueId, FiscalYear: 'FY' + String(kc.FiscalYear).slice(-2) });
// Try alternate FiscalYear format if not found
if (!existingControl) {
    existingControl = db.sqmarcherkeycontrolmaster.findOne({ EntityId: kc.EntityId, Ref_UniqueId: kc.Ref_UniqueId, FiscalYear: kc.FiscalYear });
}

if (!existingControl) {
    print("Warning: No existing record found in sqmarcherkeycontrolmaster for comparison.");
} else {
    print("\n--- COMPARISON RESULTS ---");
    print("Current (Temp) QOs:  " + (kc.QualityObjectiveUniquesIds || "EMPTY"));
    print("Existing (Master) QOs: " + (existingControl.QualityObjectiveUniquesIds || "EMPTY"));

    var current_QOs = kc.QualityObjectiveUniquesIds ? kc.QualityObjectiveUniquesIds.split(';') : [];
    var existing_QOs = existingControl.QualityObjectiveUniquesIds ? existingControl.QualityObjectiveUniquesIds.split(';') : [];

    var differenceInQOs = [
        ...current_QOs.filter(qo => !existing_QOs.includes(qo)),
        ...existing_QOs.filter(qo => !current_QOs.includes(qo))
    ];

    print("Difference Array: " + JSON.stringify(differenceInQOs));

    if (differenceInQOs.length > 0) {
        print("RESULT: CHANGES DETECTED in Quality Objectives.");
    } else {
        print("RESULT: NO CHANGES detected in Quality Objectives.");
    }
}
