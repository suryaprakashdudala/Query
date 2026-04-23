try{
    db = db.getSiblingDB('isqc');
        
    var fiscalYearFilter = process.env.FiscalYear;
        
    fiscalYearFilter = parseInt(fiscalYearFilter,10);
    
    //Filter the df_firm DataFrame based on the publish conditions specified in the application query
    var publishedEntityIds = db.firm.aggregate([{
    $match: {
        $expr: {
            $and: [
                '$publishedDate',
                { $ne: ['$publishedDate', ''] },
                { $and: [{ $eq: ['$isPublishQueryRun', false] }, { $or: [{ $eq: ['$isAutoPublished', true] },{ $eq: ['$isReadOnly', false] }]}]},
				{ $eq: ["$fiscalYear", fiscalYearFilter]},
				{ $or: [{ $eq: ["$isRollForwardedFromPreFY" , true]},{ $eq: ["$isCreatedInCurrentFY" , true]}]}
            ]
        }
    }
    },
    {	$project:{
		    abbreviation:1,
            _id: 0
	    }
    }]).toArray().map(f => f.abbreviation); //['AFRI','AME','USA']
    //var iecSystemsResources = db.documentation.find({type: 'Resource', fiscalYear: fiscalYearFilter},{_id:{ $toString: "$_id" },name:1}).toArray();
        
    db.documentation.aggregate([
        {
            $match: {
                type: "Resource",
                fiscalYear: fiscalYearFilter
            }
        },
        {
            $project: {
                _id: 0,
                resource_id: { $toString: "$_id" },
                resource_name: "$name"
            }
        },
        {
            $out: "documentation_resource"
        }
    ])
    
    db.documentation.aggregate([
        // Filter documents with type KeyControl, RequirementControl, RequirementControlAssignment and fiscalYear
        { $match: { type: { $in: ["KeyControl", "RequirementControlAssignment"] }, fiscalYear: fiscalYearFilter, firmId: { $in: publishedEntityIds } } },
        { $unwind: { path: "$addInformationExecutionControls", preserveNullAndEmptyArrays: true } },
        { $unwind: { path: "$addInformationExecutionControls.id", preserveNullAndEmptyArrays: true } },
        {
            $lookup: {
                from: 'keycontrolresource',
                let: {
                    reqCtrlAddIpeCategory: '$addInformationExecutionControls.ipeCategory' 
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$_id", "$$reqCtrlAddIpeCategory"]
                                },
                                {
                                    $in: [fiscalYearFilter, "$fiscalYear"]
                                }
                            ]
                        }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        id: { $toString: '$_id' },
                        name: 1
                    }
                }],
                as: 'ipeCategory'
            }
        },
        {
            $lookup: {
                from: 'keycontrolresource',
                let: {
                    reqCtrlAddIecReports: '$addInformationExecutionControls.iecReports' 
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $eq: ["$_id", "$$reqCtrlAddIecReports"]
                                },
                                {
                                    $in: [fiscalYearFilter, "$fiscalYear"]
                                }
                            ]
                        }
                    }
                },
                {
                    $project: {
                        _id: 0,
                        id: { $toString: '$_id' },
                        name: 1
                    }
                }],
                as: 'iecReportsOrOther'
            }
        },
        {
            $lookup: {
                from: 'documentation_resource',
                let: {
                    reqCtrlIECSystems: '$addInformationExecutionControls.ipeSystems' 
                },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and: [
                                {
                                    $in: ["$resource_id", "$$reqCtrlIECSystems"]
                                }
                            ]
                        }
                    }
                }],
                as: 'iecSystemsResource'
            }
        },
        // Project the fields as columns
        {
            $project: {
                _id: 0,
                UniqueControlId:{ $cond: { if: { $eq: ['$type','KeyControl'] }, then: '$uniqueId', else: { $concat: ['$firmId', '-', '$uniqueId'] } } },
                EntityId:'$firmId',
                ControlName:'$controlName',
                FiscalYear: "FY" + fiscalYearFilter.toString().slice(-2),
                IECId: { $concat: ['"', "$addInformationExecutionControls.id", '"']},
                IEC: "$addInformationExecutionControls.name",
                IECLocal: "$addInformationExecutionControls.nameAlt",
                IpeSystemsApplicable: "$addInformationExecutionControls.ipeSystemsApplicable",
                iecReports: "$addInformationExecutionControls.iecReports",
                IsThisIECReportOrOther: {
                    $reduce: {
                        input: '$iecReportsOrOther',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
    
                    }
                },
                ipeCategory: "$addInformationExecutionControls.ipeCategory",
                IECReportType: {
                    $reduce: {
                        input: '$ipeCategory',
                        initialValue: '',
                        in: { $concat: ['$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.name'] }
    
                    }
                },
                ControlOwnerProceduresOverCompleteness: "$addInformationExecutionControls.iecOwnerProcedures",
                ControlOwnerProceduresOverCompletenessLocal: "$addInformationExecutionControls.iecOwnerProceduresAlt",
                IECRationale: "$iecRationale",
                IECRationaleLocal: "$iecRationaleAlt",
                ipeSystems: "$addInformationExecutionControls.ipeSystems",
                IECSystems: { $cond: { if: { $eq: ['$controlType','ControlType_ItDependentManual'] }, then: {
                    $reduce: {
                        input: {
                            $cond: {
                                if: { $gt: [{ $size: { $ifNull: ['$addInformationExecutionControls.ipeSystems', []] } }, 0] },
                                then: '$iecSystemsResource',
                                else: [{ resource_name: 'N/A' }]
                            }
                        },
                        initialValue: '',
                        in: { $concat: [ '$$value', { $cond: { if: { $eq: ['$$value', ''] }, then: '', else: '; ' } }, '$$this.resource_name' ] }
                    }
                }, else: '' } },
                firmId: 1,
                uniqueId: 1,
                type: 1,
                fiscalYear: 1,
                id: { $toString: "$_id" }
            }
        },
        // Output to a new collection
        { $out: "sqmarcherkeycontrol_iec" }
    ]);
    db.documentation_resource.drop();
} catch (error) {
        print("SYSTEM:Archer Error :: Error at IEC Query ", error);        
        db.documentation_resource.drop();
        throw (error);
}
