var fiscalYearFilter = 2026;
var memberFirmIdFilter = 10307;

db.firm.aggregate([
  { $match: { fiscalYear: fiscalYearFilter, memberFirmId: memberFirmIdFilter} },
  {
    $facet: {
      ultimateResponsibility: [
        { $unwind: { path: '$ultimateResponsibility', preserveNullAndEmptyArrays: true } },
        {
            $lookup: {
              from: 'title',
              let: { ultimateResponsibilityStrs: '$ultimateResponsibility' },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $eq: [
                        { $toString: '$_id' },
                        '$$ultimateResponsibilityStrs'
                      ]
                    }
                  }
                }
              ],
              as: 'ultimateResponsibilityDoc'
            }
        },
        { $unwind: { path: '$ultimateResponsibilityDoc', preserveNullAndEmptyArrays: true } },
        {
          $lookup: {
            from: 'titleassignment',
            let: {
              titleIdStr: { $toString: '$ultimateResponsibilityDoc._id' },
              firmGroupId: '$firmGroupId',
              fiscalYear: '$fiscalYear'
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ['$titleId', '$$titleIdStr'] },
                      { $eq: ['$firmId', '$$firmGroupId'] },
                      { $eq: ['$fiscalYear', '$$fiscalYear'] }
                    ]
                  }
                }
              }
            ],
            as: 'ultimateTitleAssignments'
          }
        },
        { $unwind: { path: '$ultimateTitleAssignments', preserveNullAndEmptyArrays: true } },
        // { $unwind: { path: '$ultimateTitleAssignments.assignments', preserveNullAndEmptyArrays: true } },
        {
          $project: {
            _id: 1,
            Role: { $literal: 'UltimateResponsibilitySqmTitle' },
            Title: '$ultimateResponsibilityDoc.name',
            AssignmentName: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$ultimateTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$ultimateTitleAssignments.assignments',
                        as: 'a',
                        in: {
                          $concat: ['$$a.displayName', ': ', '$$a.email']
                        }
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            Assignment: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$ultimateTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$ultimateTitleAssignments.assignments',
                        as: 'a',
                        in: '$$a.email'
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            FY: {$concat:['FY',{$substr:['$fiscalYear',2,2]}]},
            MemberFirmId: '$memberFirmId',
            _id:0
          }
        }
      ],

      operationalResponsibilitySqm: [
        { $unwind: { path: '$operationalResponsibilitySqm', preserveNullAndEmptyArrays: true } },
        {
            $lookup: {
              from: 'title',
              let: { operationalResponsibilitySqmStrs: '$operationalResponsibilitySqm' },
              pipeline: [
                {
                  $match: {
                    $expr: {
                      $eq: [
                        { $toString: '$_id' },
                        '$$operationalResponsibilitySqmStrs'
                      ]
                    }
                  }
                }
              ],
              as: 'operationalResponsibilitySqmDoc'
            }
        },
        { $unwind: { path: '$operationalResponsibilitySqmDoc', preserveNullAndEmptyArrays: true } },
        {
          $lookup: {
            from: 'titleassignment',
            let: {
              titleIdStr: { $toString: '$operationalResponsibilitySqmDoc._id' },
              firmGroupId: '$firmGroupId',
              fiscalYear: '$fiscalYear'
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ['$titleId', '$$titleIdStr'] },
                      { $eq: ['$firmId', '$$firmGroupId'] },
                      { $eq: ['$fiscalYear', '$$fiscalYear'] }
                    ]
                  }
                }
              }
            ],
            as: 'operationalTitleAssignments'
          }
        },
        { $unwind: { path: '$operationalTitleAssignments', preserveNullAndEmptyArrays: true } },
        {
          $project: {
            Role: { $literal: 'OperationalResponsibilitySqmTitle' },
            Title: '$operationalResponsibilitySqmDoc.name',
           
            AssignmentName: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$operationalTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$operationalTitleAssignments.assignments',
                        as: 'a',
                        in: {
                          $concat: ['$$a.displayName', ': ', '$$a.email']
                        }
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            Assignment: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$operationalTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$operationalTitleAssignments.assignments',
                        as: 'a',
                        in: '$$a.email'
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            FY: {$concat:['FY',{$substr:['$fiscalYear',2,2]}]},
            MemberFirmId: '$memberFirmId',
            _id:0
          }
        }
      ],

      orIndependenceRequirement: [
        {
          $match: {
            orIndependenceRequirement: { $exists: true, $ne: null }
          }
        },
        {
          $lookup: {
            from: 'title',
            localField: 'orIndependenceRequirement',
            foreignField: '_id',
            as: 'orIndependenceRequirementDoc'
          }
        },
        { $unwind: { path: '$orIndependenceRequirementDoc', preserveNullAndEmptyArrays: true } },
        {
          $lookup: {
            from: 'titleassignment',
            let: {
              titleIdStr: { $toString: '$orIndependenceRequirementDoc._id' },
              firmGroupId: '$firmGroupId',
              fiscalYear: '$fiscalYear'
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ['$titleId', '$$titleIdStr'] },
                      { $eq: ['$firmId', '$$firmGroupId'] },
                      { $eq: ['$fiscalYear', '$$fiscalYear'] }
                    ]
                  }
                }
              }
            ],
            as: 'orIndependenceTitleAssignments'
          }
        },
        { $unwind: { path: '$orIndependenceTitleAssignments', preserveNullAndEmptyArrays: true } },
        // { $unwind: { path: '$orIndependenceTitleAssignments.assignments', preserveNullAndEmptyArrays: true } },
        {
          $project: {
            Role: { $literal: 'OperationalResponsibilityComplianceWithRequirementTitle' },
            Title: '$orIndependenceRequirementDoc.name',
            // Assignment: '$orIndependenceTitleAssignments.assignments.email',
            // AssignmentName: '$orIndependenceTitleAssignments.assignments.displayName',
            
            Assignment: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$orIndependenceTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$orIndependenceTitleAssignments.assignments',
                        as: 'a',
                        in: '$$a.email'
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            AssignmentName: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$orIndependenceTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$orIndependenceTitleAssignments.assignments',
                        as: 'a',
                        in: {
                          $concat: ['$$a.displayName', ': ', '$$a.email']
                        }
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            FY: {$concat:['FY',{$substr:['$fiscalYear',2,2]}]},
            MemberFirmId: '$memberFirmId',
            _id:0
          }
        }
      ],

      orMonitoringRemediation: [
        {
          $match: {
            orMonitoringRemediation: { $exists: true, $ne: null }
          }
        },
        {
          $lookup: {
            from: 'title',
            localField: 'orMonitoringRemediation',
            foreignField: '_id',
            as: 'orMonitoringRemediationDoc'
          }
        },
        { $unwind: { path: '$orMonitoringRemediationDoc', preserveNullAndEmptyArrays: true } },
        {
          $lookup: {
            from: 'titleassignment',
            let: {
              titleIdStr: { $toString: '$orMonitoringRemediationDoc._id' },
              firmGroupId: '$firmGroupId',
              fiscalYear: '$fiscalYear'
            },
            pipeline: [
              {
                $match: {
                  $expr: {
                    $and: [
                      { $eq: ['$titleId', '$$titleIdStr'] },
                      { $eq: ['$firmId', '$$firmGroupId'] },
                      { $eq: ['$fiscalYear', '$$fiscalYear'] }
                    ]
                  }
                }
              }
            ],
            as: 'orMonitoringRemediationTitleAssignments'
          }
        },
        { $unwind: { path: '$orMonitoringRemediationTitleAssignments', preserveNullAndEmptyArrays: true } },
        // { $unwind: { path: '$orMonitoringRemediationTitleAssignments.assignments', preserveNullAndEmptyArrays: true } },
        {
          $project: {
            Role: { $literal: 'OperationalResponsibilityMonitoringAndRemediationTitle' },
            Title: '$orMonitoringRemediationDoc.name',
            // Assignment: '$orMonitoringRemediationTitleAssignments.assignments.email',
            // AssignmentName: '$orMonitoringRemediationTitleAssignments.assignments.displayName',
             AssignmentName: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$orMonitoringRemediationTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$orMonitoringRemediationTitleAssignments.assignments',
                        as: 'a',
                        in: {
                          $concat: ['$$a.displayName', ': ', '$$a.email']
                        }
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            Assignment: {
              $cond: [
                { $gt: [{ $size: { $ifNull: ['$orMonitoringRemediationTitleAssignments.assignments', []] } }, 0] },
                {
                  $reduce: {
                    input: {
                      $map: {
                        input: '$orMonitoringRemediationTitleAssignments.assignments',
                        as: 'a',
                        in: '$$a.email'
                      }
                    },
                    initialValue: '',
                    in: {
                      $cond: [
                        { $eq: ['$$value', ''] },
                        '$$this',
                        { $concat: ['$$value', '; ', '$$this'] }
                      ]
                    }
                  }
                },
                null
              ]
            },
            FY: {$concat:['FY',{$substr:['$fiscalYear',2,2]}]},
            MemberFirmId: '$memberFirmId',
            _id:0
          }
        }
      ]
    }
  },
  {
    $project: {
      combined: {
        $concatArrays: [
          "$ultimateResponsibility",
          "$operationalResponsibilitySqm",
          "$orIndependenceRequirement",
          "$orMonitoringRemediation"
        ]
      },
    }
  },

  { $unwind: "$combined" },
  { $replaceRoot: { newRoot: "$combined" } },
  {$out: "archerentitiestitles"}
]);