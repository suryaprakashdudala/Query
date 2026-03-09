db.archerentities.aggregate([

{
  $match:{
    Type:'Member Firm',
    EventType:{$ne:'Deleted'}
  }
},

{
  $project:{
    ultimate:{
      $map:{
        input:{$ifNull:["$ultimateResponsibility",[]]},
        as:"u",
        in:{
          Role:"UltimateResponsibilitySqmTitle",
          Title:"$$u.name",
          AssignmentName:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$$u.titleAssignments.assignments",[]]},
                  as:"a",
                  in:{$concat:["$$a.displayName",": ","$$a.email"]}
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          Assignment:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$$u.titleAssignments.assignments",[]]},
                  as:"a",
                  in:"$$a.email"
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          FY:"$FiscalYear",
          MemberFirmId:"$MemberFirmId"
        }
      }
    },

    operational:{
      $map:{
        input:{$ifNull:["$operationalResponsibilitySqm",[]]},
        as:"o",
        in:{
          Role:"OperationalResponsibilitySqmTitle",
          Title:"$$o.name",
          AssignmentName:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$$o.titleAssignments.assignments",[]]},
                  as:"a",
                  in:{$concat:["$$a.displayName",": ","$$a.email"]}
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          Assignment:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$$o.titleAssignments.assignments",[]]},
                  as:"a",
                  in:"$$a.email"
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          FY:"$FiscalYear",
          MemberFirmId:"$MemberFirmId"
        }
      }
    },

    compliance:{
      $cond:[
        {$ne:["$orIndependenceRequirement",null]},
        [{
          Role:"OperationalResponsibilityComplianceWithRequirementTitle",
          Title:"$orIndependenceRequirement.name",
          AssignmentName:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$orIndependenceRequirement.titleAssignments.assignments",[]]},
                  as:"a",
                  in:{$concat:["$$a.displayName",": ","$$a.email"]}
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          Assignment:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$orIndependenceRequirement.titleAssignments.assignments",[]]},
                  as:"a",
                  in:"$$a.email"
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          FY:"$FiscalYear",
          MemberFirmId:"$MemberFirmId"
        }],
        []
      ]
    },

    monitoring:{
      $cond:[
        {$ne:["$orMonitoringRemediation",null]},
        [{
          Role:"OperationalResponsibilityMonitoringAndRemediationTitle",
          Title:"$orMonitoringRemediation.name",
          AssignmentName:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$orMonitoringRemediation.titleAssignments.assignments",[]]},
                  as:"a",
                  in:{$concat:["$$a.displayName",": ","$$a.email"]}
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          Assignment:{
            $reduce:{
              input:{
                $map:{
                  input:{$ifNull:["$orMonitoringRemediation.titleAssignments.assignments",[]]},
                  as:"a",
                  in:"$$a.email"
                }
              },
              initialValue:"",
              in:{
                $cond:[
                  {$eq:["$$value",""]},
                  "$$this",
                  {$concat:["$$value","; ","$$this"]}
                ]
              }
            }
          },
          FY:"$FiscalYear",
          MemberFirmId:"$MemberFirmId"
        }],
        []
      ]
    }
  }
},

{
  $project:{
    combined:{
      $concatArrays:[
        "$ultimate",
        "$operational",
        "$compliance",
        "$monitoring"
      ]
    }
  }
},

{
  $unwind:"$combined"
},

{
  $replaceRoot:{newRoot:"$combined"}
},

{
  $sort:{MemberFirmId:1,FY:1}
},

{
  $out:"archerentitiestitle"
}

])