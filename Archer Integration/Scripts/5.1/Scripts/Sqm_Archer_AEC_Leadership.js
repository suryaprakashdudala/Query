try{
    db = db.getSiblingDB('isqc');

    var fiscalYearFilter = process.env.FiscalYear;
	
	fiscalYearFilter = parseInt(fiscalYearFilter,10);

    db.firm.aggregate([
        {
            $match:{
                $expr:{
                    $and:[
                        {
                            $eq:['$type','EntityType_MemberFirm']
                        },
                        {
                            $eq:['$fiscalYear',fiscalYearFilter]
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
                        firmGroupId:'$firmGroupId'  //added firmGroupId check

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
                                    $eq: [ '$fiscalYear',fiscalYearFilter]
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
                FiscalYear:"FY" + fiscalYearFilter.toString().slice(-2)
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