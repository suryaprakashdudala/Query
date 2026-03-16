 var fiscalYearFilter = 2026;
    db.title.aggregate([
        { $match: { fiscalYear: fiscalYearFilter } },
        { $addFields: { id_str: { $toString: "$_id" } } },
        {
            $lookup: {
                from: 'titleassignment',
                localField: 'id_str',
                foreignField: 'titleId',
                as: 'tas'
            }
        },
        {
            $project: {
                _id: 0,
                titleId: "$id_str",
                fiscalYear: 1,
                titleName: "$name",
                firmId: "$tas.firmId",
                firmAssignments: {
                    $map: {
                        input: "$tas",
                        as: "t",
                        in: {
                            firmId: "$$t.firmId",
                            assignmentString: {
                                $reduce: {
                                    input: {
                                        $map: {
                                            input: { $ifNull: ["$$t.assignments", []] },
                                            as: "a",
                                            in: { $concat: ["$$a.displayName", "(", "$$a.email", ")"] }
                                        }
                                    },
                                    initialValue: "",
                                    in: {
                                        $cond: [
                                            { $eq: ["$$value", ""] },
                                            "$$this",
                                            { $concat: ["$$value", "; ", "$$this"] }
                                        ]
                                    }
                                }
                            }
                        }
                    }
                }
            }, 
        },
        { $out: "titleAndAssignments" }
    ]);

    db.titleAndAssignments.aggregate([
      { $unwind: "$firmAssignments" },
      {
        $project: {
          _id: 0,
          titleId: 1,
          fiscalYear: 1,
          titleName: 1,
          firmId: "$firmAssignments.firmId",
          assignmentString: "$firmAssignments.assignmentString"
        }
      },
      { $out: "titleFirmAssignments" }
    ]);

    // ultimateResponsibility: [
    //   {
    //     _id: ObjectId('69b7c4c7694f50492e88280d'),
    //     fiscalYear: 2026,
    //     titleId: '6855d87771835f94ed69e686',
    //     titleName: 'Country Managing Partner',
    //     firmId: 'AFRI',
    //     assignmentString: 'Emmanuel Adekahlor(Emmanuel.Adekahlor@gh.ey.com)'
    //   },
    //   {
    //     _id: ObjectId('69b7c4c7694f50492e88280e'),
    //     fiscalYear: 2026,
    //     titleId: '6855d87771835f94ed69e686',
    //     titleName: 'Country Managing Partner',
    //     firmId: 'AGDS',
    //     assignmentString: ''
    //   },
    // ]


//    db.titleAndAssignments.find({titleId:'69044a794d636931b9cecba6'})
// [
//   {
//     _id: ObjectId('69b440b9694f50492e84815a'),
//     fiscalYear: 2026,
//     titleId: '69044a794d636931b9cecba6',
//     titleName: '07988974A&*',
//     firmId: [ 'ARLO9', 'NTW', 'SRA', 'nloc1' ],
//     firmAssignments: [
//       {
//         firmId: 'ARLO9',
//         assignmentString: 'Satya Suryaprakash Dudala(Satya.Suryaprakash.Dudala@ey.com); Subha Korla(Subha.Korla@ey.com)'
//       },
//       {
//         firmId: 'NTW',
//         assignmentString: 'Satya Suryaprakash Dudala(Satya.Suryaprakash.Dudala@ey.com)'
//       },
//       {
//         firmId: 'SRA',
//         assignmentString: 'Satya Suryaprakash Dudala(Satya.Suryaprakash.Dudala@ey.com)'
//       },
//       {
//         firmId: 'nloc1',
//         assignmentString: 'Zeeshan Mohammad(Zeeshan.Mohammad@ey.com)'
//       }
//     ]
//   }
// ]