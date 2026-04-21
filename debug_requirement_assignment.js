
// Debug query to view requirementcontrol records
// Exporting to a temp collection: debug_requirementcontrol_temp

try {
    var fiscalYearFilter = [2026]; // Adjust as needed
    var autoQoNotReqFirms = ['USA'];

    // Assuming we can use the same logic to find publishedEntityIds or just filter manually
    // For debugging, let's just use a sample if possible, or use the whole collection if it's not too big.
    
    db.firm.aggregate([
        {
            $match: {
                fiscalYear: { $in: fiscalYearFilter },
                // You can add more filters here if you want to target specific firms
            }
        },
        {
            $lookup: {
                from: 'documentation',
                let: { firmId: '$abbreviation', fy: '$fiscalYear' },
                pipeline: [
                    {
                        $match: {
                            $expr: {
                                $and: [
                                    { $eq: ['$type', 'RequirementControl'] },
                                    { $eq: ['$fiscalYear', '$$fy'] }
                                ]
                            }
                        }
                    }
                ],
                as: 'requirementcontrols'
            }
        },
        { $unwind: '$requirementcontrols' },
        {
            $project: {
                _id: 0,
                abbreviation: 1,
                fiscalYear: 1,
                requirementcontrol: '$requirementcontrols',
                isAutoQoFirm: { $in: ['$abbreviation', autoQoNotReqFirms] }
            }
        },
        {
            $out: 'debug_requirementcontrol_temp'
        }
    ]);

    print("Debug data exported to 'debug_requirementcontrol_temp' successfully.");

} catch (e) {
    print("Error in debug query: " + e);
}
