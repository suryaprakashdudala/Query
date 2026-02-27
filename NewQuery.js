// Update the target collection and query specific fields
db.archerentities.aggregate([
  { $match: { FiscalYear: "FY26", MemberFirmId: 10307 } },
  {
    $facet: {
      ultimate: [
        { $unwind: "$UltimateResponsibilityTitleId" },
        {
          $lookup: {
            from: "title",
            localField: "UltimateResponsibilityTitleId",
            foreignField: "_id",
            as: "titleDoc"
          }
        },
        { $unwind: "$titleDoc" },
        {
          $lookup: {
            from: "titleassignment",
            let: {
              titleId: { $toString: "$titleDoc._id" },
              firmId: "$FirmGroupId",
              fiscalYear: "$EntityFiscalYear"
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ["$titleId", "$$titleId"] },
                      { $eq: ["$firmId", "$$firmId"] },
                      { $eq: ["$fiscalYear", "$$fiscalYear"] }
                    ]
                  }
                }
              }
            ],
            as: "assignmentDoc"
          }
        },
        { $unwind: { path: "$assignmentDoc", preserveNullAndEmptyArrays: true } },
        {
          $project: {
            Role: "UltimateResponsibilitySqmTitle",
            Title: "$titleDoc.name",
            AssignmentName: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: { $concat: ["$$a.displayName", ": ", "$$a.email"] }
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            Assignment: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: "$$a.email"
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            FY: "$FiscalYear",
            MemberFirmId: "$MemberFirmId"
          }
        }
      ],
      operationalSqm: [
        { $unwind: "$OperationalResponsibilitySqmTitleId" },
        {
          $lookup: {
            from: "title",
            localField: "OperationalResponsibilitySqmTitleId",
            foreignField: "_id",
            as: "titleDoc"
          }
        },
        { $unwind: "$titleDoc" },
        {
          $lookup: {
            from: "titleassignment",
            let: {
              titleId: { $toString: "$titleDoc._id" },
              firmId: "$FirmGroupId",
              fiscalYear: "$EntityFiscalYear"
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ["$titleId", "$$titleId"] },
                      { $eq: ["$firmId", "$$firmId"] },
                      { $eq: ["$fiscalYear", "$$fiscalYear"] }
                    ]
                  }
                }
              }
            ],
            as: "assignmentDoc"
          }
        },
        { $unwind: { path: "$assignmentDoc", preserveNullAndEmptyArrays: true } },
        {
          $project: {
            Role: "OperationalResponsibilitySqmTitle",
            Title: "$titleDoc.name",
            AssignmentName: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: { $concat: ["$$a.displayName", ": ", "$$a.email"] }
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            Assignment: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: "$$a.email"
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            FY: "$FiscalYear",
            MemberFirmId: "$MemberFirmId"
          }
        }
      ],
      compliance: [
        { $match: { OrIndependenceRequirementTitleId: { $ne: null, $ne: "" } } },
        {
          $lookup: {
            from: "title",
            localField: "OrIndependenceRequirementTitleId",
            foreignField: "_id",
            as: "titleDoc"
          }
        },
        { $unwind: "$titleDoc" },
        {
          $lookup: {
            from: "titleassignment",
            let: {
              titleId: { $toString: "$titleDoc._id" },
              firmId: "$FirmGroupId",
              fiscalYear: "$EntityFiscalYear"
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ["$titleId", "$$titleId"] },
                      { $eq: ["$firmId", "$$firmId"] },
                      { $eq: ["$fiscalYear", "$$fiscalYear"] }
                    ]
                  }
                }
              }
            ],
            as: "assignmentDoc"
          }
        },
        { $unwind: { path: "$assignmentDoc", preserveNullAndEmptyArrays: true } },
        {
          $project: {
            Role: "OperationalResponsibilityComplianceWithRequirementTitle",
            Title: "$titleDoc.name",
            AssignmentName: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: { $concat: ["$$a.displayName", ": ", "$$a.email"] }
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            Assignment: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: "$$a.email"
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            FY: "$FiscalYear",
            MemberFirmId: "$MemberFirmId"
          }
        }
      ],
      monitoring: [
        { $match: { OrMonitoringRemediationTitleId: { $ne: null, $ne: "" } } },
        {
          $lookup: {
            from: "title",
            localField: "OrMonitoringRemediationTitleId",
            foreignField: "_id",
            as: "titleDoc"
          }
        },
        { $unwind: "$titleDoc" },
        {
          $lookup: {
            from: "titleassignment",
            let: {
              titleId: { $toString: "$titleDoc._id" },
              firmId: "$FirmGroupId",
              fiscalYear: "$EntityFiscalYear"
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ["$titleId", "$$titleId"] },
                      { $eq: ["$firmId", "$$firmId"] },
                      { $eq: ["$fiscalYear", "$$fiscalYear"] }
                    ]
                  }
                }
              }
            ],
            as: "assignmentDoc"
          }
        },
        { $unwind: { path: "$assignmentDoc", preserveNullAndEmptyArrays: true } },
        {
          $project: {
            Role: "OperationalResponsibilityMonitoringAndRemediationTitle",
            Title: "$titleDoc.name",
            AssignmentName: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: { $concat: ["$$a.displayName", ": ", "$$a.email"] }
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            Assignment: {
              $reduce: {
                input: {
                  $map: {
                    input: { $ifNull: ["$assignmentDoc.assignments", []] },
                    as: "a",
                    in: "$$a.email"
                  }
                },
                initialValue: "",
                in: { $cond: [{ $eq: ["$$value", ""] }, "$$this", { $concat: ["$$value", "; ", "$$this"] }] }
              }
            },
            FY: "$FiscalYear",
            MemberFirmId: "$MemberFirmId"
          }
        }
      ]
    }
  },
  {
    $project: {
      combined: { $concatArrays: ["$ultimate", "$operationalSqm", "$compliance", "$monitoring"] }
    }
  },
  { $unwind: "$combined" },
  { $replaceRoot: { newRoot: "$combined" } },
  { $out: "archerentitiestitles" }
]);

