try{
    db = db.getSiblingDB('isqc');

   // var fiscalYearFilter = process.env.FiscalYear;
    var fiscalYearFilter = process.env.FiscalYears;
    
	fiscalYearFilter = fiscalYearFilter.split(",").map(function (year) {
        return parseInt(year, 10);
    });

    db.firm.aggregate([
        {
            $match:{
                $expr:{
                    $and:[
                        {
                            $eq:['$type','EntityType_MemberFirm']
                        },
                        {
                            $in:['$fiscalYear',fiscalYearFilter]
                        }
                    ]
                }
            }
        },{
            $lookup:{
                from:'titleassignment',
                let:
                    {
                        ultimateResponsibility: { $toString: '$ultimateResponsibility' },
                        operationalResponsibilitySqm: { $toString: '$operationalResponsibilitySqm' },
                        orIndependenceRequirement: { $toString: '$orIndependenceRequirement' },
                        orMonitoringRemediation: { $toString: '$orMonitoringRemediation' },
                        pcaobUltimateResponsibility: { $toString: '$pcaobUltimateResponsibility' } ,
                        pcaobOperationalResponsibilitySqm: { $toString: '$pcaobOperationalResponsibilitySqm' },
                        pcaobOrIndependenceRequirement: { $toString: '$pcaobOrIndependenceRequirement' },
                        pcaobOrMonitoringRemediation:{ $toString: '$pcaobOrMonitoringRemediation' },
                        pcaobOrMonitoringRemediationComponents: { $toString: '$pcaobOrMonitoringRemediationComponents' },
                        firmGroupId:'$firmGroupId',//added firmGroupId check
                        fiscalYear: '$fiscalYear'

                    },
                pipeline: [{
                    $match: {
                        $expr: {
                            $and:[
                                {
                                    $or:[
                                        {$eq: [ '$$ultimateResponsibility','$titleId']} ,
                                        {$eq: [ '$$operationalResponsibilitySqm','$titleId'] },
                                        {$eq: [ '$$orIndependenceRequirement','$titleId']},
                                        {$eq: [ '$$orMonitoringRemediation','$titleId']},
                                        {$eq: [ '$$pcaobUltimateResponsibility','$titleId']},
                                        {$eq: [ '$$pcaobOperationalResponsibilitySqm','$titleId']},
                                        {$eq: [ '$$pcaobOrIndependenceRequirement','$titleId']},
                                        {$eq: [ '$$pcaobOrMonitoringRemediation','$titleId']},
                                        {$eq: [ '$$pcaobOrMonitoringRemediationComponents','$titleId']}
                                    ]
                                },
                                {
                                    $in: [ '$fiscalYear',fiscalYearFilter]
                                },
                                {
                                    $eq:['$firmId','$$firmGroupId']
                                }
                            ]
                            
                        }
                    }
                }],
                as:'SQMLeadership'
            }
        },
        {
            $unwind:'$SQMLeadership'
        },
        {
            $unwind:'$SQMLeadership.assignments'
        },
        {
            $group:{
                _id:
                    {
                        displayName:'$SQMLeadership.assignments.displayName',
                        email:'$SQMLeadership.assignments.email',
                        fiscalYear:'$SQMLeadership.fiscalYear'
                    },
            }
        },
        {
            $project:{
                _id:0,
                UserName:'$_id.displayName',
                Email:'$_id.email',
                EntityFiscalYear: '$_id.fiscalYear',
                FiscalYear: {$concat:['FY',{$substr:['$_id.fiscalYear',2,2]}]}
            }
        },
        {
            $out:'SQMLeadershipRoles'
        }
    ])
} catch(error) {
    print("SYSTEM:Archer Error :: Error at AEC Leadership",error);
    throw(error)
}