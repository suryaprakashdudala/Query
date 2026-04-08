db = db.getSiblingDB('isqc');
print("Revert Rollforward is in progress.....");
var abbreviation = ['NTW'];
var currentFY = 2025;
var nextFY = currentFY + 1;

//To update the respective flags to default values
db.firm.aggregate([{
    $match: {
        $expr:{
            $and: [
                { $or: [{$in:['$abbreviation',abbreviation]}, {$in:['$firmGroupId',abbreviation]}] },
                { $eq:['$fiscalYear',currentFY] },
                { $eq:['$isRollForwardTriggered', true]}
            ]
        }
        
    }
}]).forEach(function (f) {
    if (['EntityType_MemberFirm'].includes(f.type)) {
        //Update the flags for MF's
        db.firm.updateMany({ firmGroupId: f.firmGroupId, fiscalYear: f.fiscalYear }, { $set: { 
            isRollForwardTriggered: false, 
            isRollFwdComplete: false,
            rollForwardStatus: '',
            rollForwardByDisplayName: '',
            rollForwardByEmail: '', 
            rollForwardDate: '', 
            isPublishQueryRun: ''
        } });
    }
    //update flags for entities other than MF's
        db.firm.updateOne({ abbreviation: f.abbreviation, fiscalYear: f.fiscalYear }, { $set: { 
            isRollForwardTriggered: false, 
            isRollFwdComplete: false,
            rollForwardStatus: '',
            rollForwardByDisplayName: '',
            rollForwardByEmail: '', 
            rollForwardDate: '', 
            isPublishQueryRun: '' 
        } });
});

//set isRollForwardedFromPreFY to false and sqmleadership values
db.firm.aggregate([{
    $match: {
        $expr:{
            $and: [
                { $or: [{$in:['$abbreviation',abbreviation]}, {$in:['$firmGroupId',abbreviation]}] },
                { $eq:['$fiscalYear',nextFY] },
            ]
        }
        
    }
}]).forEach(function (f) {
    if (['EntityType_MemberFirm'].includes(f.type)) {
        //To set the sqmleadership values as empty while we Rollback location
        db.firm.updateMany({ firmGroupId: f.firmGroupId, fiscalYear: f.fiscalYear }, { $set: { "isRollForwardedFromPreFY": false,"ultimateResponsibility": '',"operationalResponsibilitySqm": '',"orIndependenceRequirement": '',"orMonitoringRemediation": '' } });
    }
    db.firm.updateOne({ abbreviation: f.abbreviation, fiscalYear: f.fiscalYear }, { $set: { "isRollForwardedFromPreFY": false } });
});


//delete custom titles
db.titleassignment.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation }})
db.documentation.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })
db.functionowner.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })
db.firmprocess.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })
db.title.deleteMany({ global: false, fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })
db.action.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })
db.event.deleteMany({ fiscalYear: { $eq: nextFY }, publisher: { $in: abbreviation } })
db.localalternateobject.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })
db.unrejectedaction.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })
db.documenttag.deleteMany({ fiscalYear: { $eq: nextFY }, firmId: { $in: abbreviation } })

db.bkcassignment.aggregate([{
    $match: {
        'assignments.executingEntityId': { $in: abbreviation },
        'fiscalYear': nextFY
    }
}]).forEach(function (f) {
    if (db.bkcassignment.find({ areaId: { $in: abbreviation }, fiscalYear: { $eq: nextFY } }).count() > 0) {
        db.bkcassignment.deleteMany({ fiscalYear: { $eq: nextFY }, areaId: { $in: abbreviation } })
    }
    else {
        var filteredAssignments = f.assignments.filter(assign => abbreviation.some(r => r === assign.executingEntityId))
        var bkcassignmentexists = db.bkcassignment.findOne({ firmId: f.firmId, fiscalYear: nextFY })
        filteredAssignments.forEach(re => {
            if (bkcassignmentexists) {
                db.bkcassignment.update(
                    { _id: bkcassignmentexists._id },
                    { $pull: { "assignments": { "executingEntityId": re.executingEntityId, "bkcId": re.bkcId } } }
                );
            }
        })
    }
});

if (abbreviation.includes('NTW')){
    //delete baseline titles
    db.title.deleteMany({ global: true, fiscalYear: { $eq: nextFY } }),
    db.documentation.deleteMany({fiscalYear: { $eq: nextFY }}),
    db.titleassignment.deleteMany({ fiscalYear: { $eq: nextFY }})
    db.functionowner.deleteMany({ fiscalYear: { $eq: nextFY } })
    db.firmprocess.deleteMany({ fiscalYear: { $eq: nextFY } })
    db.globaldocumentation.deleteMany({fiscalYear: { $eq: nextFY }})
    db.action.deleteMany({ fiscalYear: { $eq: nextFY } })
    db.localalternateobject.deleteMany({ fiscalYear: { $eq: nextFY } })
    db.unrejectedaction.deleteMany({ fiscalYear: { $eq: nextFY } })
    db.documenttag.deleteMany({ fiscalYear: { $eq: nextFY } })
    db.enumeration.updateMany({type:{$ne:'FiscalYearType'}},{$pull:{fiscalYear:nextFY}})
    db.enumeration.deleteOne({type:'FiscalYearType',_id:nextFY})
    db.keycontrolresource.updateMany({},{$pull:{fiscalYear:nextFY}})
    db.country.updateMany({},{$pull:{fiscalYear:nextFY}})
    db.resourcetype.updateMany({},{$pull:{fiscalYear:nextFY}})
    db.component.updateMany({},{$pull:{fiscalYear:nextFY}})
    db.firm.deleteMany({fiscalYear:nextFY})
    db.bkcassignment.deleteMany({ fiscalYear: { $eq: nextFY }})
    db.user.updateMany({},
        [
            {
            $set: {
                // Remove roles and areaWideAccess entries matching the year dynamically
                roles: {
                $filter: {
                    input: "$roles",
                    as: "role",
                    cond: { $not: { $regexMatch: { input: "$$role.roleId", regex: `_${nextFY}$` } } }
                }
                },
                areaWideAccess: {
                $filter: {
                    input: "$areaWideAccess",
                    as: "access",
                    cond: { $ne: ["$$access.fiscalYear", nextFY] }
                }
                },
                // Remove the year dynamically from superAdminScopeYear
                superAdminScopeYear: {
                $reduce: {
                    input: {
                    $filter: {
                        input: { $split: ["$superAdminScopeYear", "|"] },
                        as: "part",
                        cond: { $not: { $regexMatch: { input: "$$part", regex: `_${nextFY}/` } } }
                    }
                    },
                    initialValue: "",
                    in: {
                    $concat: [
                        "$$value",
                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: "|" } },
                        "$$this"
                    ]
                    }
                }
                },
                // Remove the year dynamically from nonSuperAdminScopeYear
                nonSuperAdminScopeYear: {
                $reduce: {
                    input: {
                    $filter: {
                        input: { $split: ["$nonSuperAdminScopeYear", "|"] },
                        as: "part",
                        cond: { $not: { $regexMatch: { input: "$$part", regex: `_${nextFY}/` } } }
                    }
                    },
                    initialValue: "",
                    in: {
                    $concat: [
                        "$$value",
                        { $cond: { if: { $eq: ["$$value", ""] }, then: "", else: "|" } },
                        "$$this"
                    ]
                    }
                }
                }
            }
            }
        ]
    );
}
print("Reverted the Rollforward changes!!");
