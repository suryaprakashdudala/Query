// Debug Query: Verify missing Quality Objectives in Requirement Control Overrides
db.firm.aggregate([
  {
    $match: {
      abbreviation: { $eq: 'AME' }, // Focus on non-USA firms as reported
      fiscalYear: 2026               // Update if using a different FY
    }
  },
  {
    $lookup: {
      from: 'bkcassignment',
      pipeline: [{ $match: { fiscalYear: 2026 } }],
      as: 'bkc'
    }
  },
  { $unwind: '$bkc' },
  { $unwind: '$bkc.assignments' },
  // Filter for the executing entity
  { $match: { $expr: { $eq: ['$bkc.assignments.executingEntityId', '$abbreviation'] } } },
  {
    $lookup: {
      from: 'documentation',
      let: { bkcId: '$bkc.assignments.bkcId' },
      pipeline: [
        { $match: { $expr: { $and: [{ $eq: ['$uniqueId', '$$bkcId'] }, { $eq: ['$type', 'RequirementControl'] }] } } }
      ],
      as: 'control'
    }
  },
  { $unwind: '$control' },
  {
    $lookup: {
      from: 'documentation',
      let: { reqId: { $toString: '$control._id' }, firmId: '$abbreviation' },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [{ $eq: ['$type', 'RequirementControlAssignment'] }, { $eq: ['$firmId', '$$firmId'] }, { $eq: ['$requirementControlId', '$$reqId'] }]
            }
          }
        }
      ],
      as: 'assignment'
    }
  },
  { $unwind: '$assignment' },
  { $project: {
      _id: 0,
      Firm: '$abbreviation',
      ControlUniqueId: '$control.uniqueId',
      ControlName: '$control.controlName',
      
      // ORIGIN: Objectives in the global control document
      GlobalRelatedObjectives: { $ifNull: ['$control.relatedObjectives', []] },
      
      // OVERRIDE: Objectives in the local assignment document
      AssignmentRelatedObjectives: { $ifNull: ['$assignment.relatedObjectives', []] },
      
      // DISCREPANCY CHECK: If assignment has objectives but global doesn't, this was being lost
      IsDataMissingInOriginalScript: {
          $and: [
              { $gt: [{ $size: { $ifNull: ['$assignment.relatedObjectives', []] } }, 0] },
              { $eq: [{ $size: { $ifNull: ['$control.relatedObjectives', []] } }, 0] }
          ]
      }
  }},
  // { $match: { IsDataMissingInOriginalScript: true } } // Show only records that were failing
]);
