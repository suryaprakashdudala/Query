var $ = require('jquery');
var _ = require('underscore');
var Authorization = require('Authorization');
var AddFirmRule = require('AddFirmRule');
var AddAttachment = require('AddAttachment');
var Behaviors = require('Behaviors');
var Country = _.find(EY.ISQC1.Resources.Enumeration, { type: 'CountryNetworkType', id: 'CountryNetworkType_Country' }).id;
var CountryNetwork = _.find(EY.ISQC1.Resources.Enumeration, { type: 'CountryNetworkType', id: 'CountryNetworkType_Network' }).id;
var DocumentationDelete = require("DocumentationDelete");
var Dialog = require("Dialog");
var Entities = require('Entities');
var EntityTypes = _.filter(EY.ISQC1.Resources.Enumeration, { type: 'EntityType'});
var EyHierarchy = require('EyHierarchy');
var FirmActionSave = require('FirmActionSave');
var FirmTitlesManager = require('FirmTitlesManager');
var IsPartOfGroup = _.find(EY.ISQC1.Resources.Enumeration, { type: 'IsPartOfGroupType', id: 'IsPartOfGroupType_Yes' }).id;
var IsNotPartOfGroup = _.find(EY.ISQC1.Resources.Enumeration, { type: 'IsPartOfGroupType', id: 'IsPartOfGroupType_No' }).id;
var Labels = EY.ISQC1.Labels;
var Marionette = require('marionette');
var Resources = EY.ISQC1.Resources;
var languageDropdownValues =  _.find(EY.ISQC1.Resources.Enumeration, { type: 'LanguageType'});
var TypeAheadDropdown = require('TypeAheadDropdown');
var MultiSelectTypeAheadDropdown = require('MultiSelectTypeAheadDropdown');
var Backbone = require('backbone');
var Validation = require('validation');
var toggleOn = 'fa-toggle-on';
var toggleOff = 'fa-toggle-off';
var Configuration = EY.ISQC1.Lookups.Configuration;

var getAssignedUsers = function (options) {
    var titleAssignmentModel = options.titleAssignments.find({ firmId: options.firmId, titleId: options.model.get('id') });
    var assignedUsersArray = titleAssignmentModel && _.pluck(titleAssignmentModel.get('assignments'), 'displayName');
    var assignedUsers = assignedUsersArray && assignedUsersArray.join("; ");
    assignedUsers = assignedUsers ? assignedUsers : Labels.NoAssignment;
    return assignedUsers;
};

var getSelectedTexts = function(models, abbreviation, options) {
    return _.map(models, function (m) {
        return m.get("name") + " (" + getAssignedUsers({
            model: m,
            firmId: abbreviation,
            titleAssignments: options.titleAssignments
        }) + ")";
    });
}

var MeberFirmProfileView = Marionette.View.extend({
    template: '#memberFirmProfile',
    behaviors: [{ behaviorClass: Authorization.RBAC, viewName: 'memberFirmProfileView' }],
    initialize: function () {
        Validation.bind(this);
    },
    ui: {
        ultimateResponsibilityIndicator: "#ultimateResponsibilityIndicator",
        operationalResponsibilitySqmIndicator: "#operationalResponsibilitySqmIndicator",
        orIndependenceRequirementIndicator: "#orIndependenceRequirementIndicator",
        orMonitoringRemediationIndicator: "#orMonitoringRemediationIndicator",
        ultimateResponsibility: '#ultimateResponsibility',
        operationalResponsibilitySqm: '#operationalResponsibilitySqm',
        orIndependenceRequirement: '#orIndependenceRequirement',
        orMonitoringRemediation: '#orMonitoringRemediation'
    },
    regions: {
        ultimateResponsibility: '#ultimateResponsibility',
        operationalResponsibilitySqm: '#operationalResponsibilitySqm',
        orIndependenceRequirement: '#orIndependenceRequirement',
        orMonitoringRemediation: '#orMonitoringRemediation'
    },
    modelEvents:{
        'change:firmGroupId':function(){
            this.ui.ultimateResponsibility.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.operationalResponsibilitySqm.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.orIndependenceRequirement.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.orMonitoringRemediation.find('#searchInput').attr("placeholder", Labels.Select);
        }
    },
    onAttach: function () {
        var model = this.model;
        var view = this;
        var options = this.options;
        var abbreviation = model.get('firmGroupId') ? model.get('firmGroupId') : model.get('abbreviation');
        options.titles.each(function (item) {
            item.set('titleId', _.isObject(item.get('_id')) ? item.get('_id').$oid : item.get('_id'));
        });

        options.titles.forEach(function(title){
            options.ultimateResponsibilityTitlesCollection.add(new Entities.Title(title.attributes, {
                titleAssignments: options.titleAssignments,
                firmId: abbreviation
            }));
            options.operationalResponsibilityTitlesCollection.add(new Entities.Title(title.attributes, {
                titleAssignments: options.titleAssignments,
                firmId: abbreviation
            }));

        });
        var orIndependenceRequirementModel = options.titles.findWhere({ titleId: model.get("orIndependenceRequirement") ? model.get("orIndependenceRequirement").$oid : '' });
        var orMonitoringRemediationModel = options.titles.findWhere({ titleId: model.get("orMonitoringRemediation") ? model.get("orMonitoringRemediation").$oid : '' });
        
        var ultimateResponsibilityIds = model.get("ultimateResponsibility") || [];
        var ultimateResponsibilityModels = options.titles.filter(function (t) {
            return _.contains(ultimateResponsibilityIds, t.get('titleId'));
        });

        MultiSelectTypeAheadDropdown.show({
            collection: options.ultimateResponsibilityTitlesCollection,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValues: ultimateResponsibilityIds,
            selectedTexts: getSelectedTexts(ultimateResponsibilityModels, abbreviation, options),
            region: this.getRegion('ultimateResponsibility'),
            tooltip: true,
            change: function (selectedValues) {
                model.set('ultimateResponsibility', selectedValues);
                _.isEmpty(selectedValues) ? view.ui.ultimateResponsibilityIndicator.show() : view.ui.ultimateResponsibilityIndicator.hide();
            }
        })

        var operationalResponsibilitySqmIds = model.get("operationalResponsibilitySqm") || [];
        var operationalResponsibilitySqmModels = options.titles.filter(function (t) {
            return _.contains(operationalResponsibilitySqmIds, t.get('titleId'));
        });


        MultiSelectTypeAheadDropdown.show({
            collection: options.operationalResponsibilityTitlesCollection,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValues: operationalResponsibilitySqmIds,
            selectedTexts: getSelectedTexts(operationalResponsibilitySqmModels, abbreviation, options),
            region: this.getRegion('operationalResponsibilitySqm'),
            tooltip: true,
            change: function (selectedValues) {
                model.set('operationalResponsibilitySqm', selectedValues);
                _.isEmpty(selectedValues) ? view.ui.operationalResponsibilitySqmIndicator.show() : view.ui.operationalResponsibilitySqmIndicator.hide();
            }
        });

        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (orIndependenceRequirementModel && orIndependenceRequirementModel.get("name") + " (" + getAssignedUsers({
                model: orIndependenceRequirementModel,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: orIndependenceRequirementModel,
            region: this.getRegion('orIndependenceRequirement'),
            tooltip: true,
            change: function (orIndependenceRequirement) {
                model.set('orIndependenceRequirement', orIndependenceRequirement.get('_id'));
                view.ui.orIndependenceRequirementIndicator.hide();
            }
        });

        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (orMonitoringRemediationModel && orMonitoringRemediationModel.get("name") + " (" + getAssignedUsers({
                model: orMonitoringRemediationModel,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: orMonitoringRemediationModel,
            region: this.getRegion('orMonitoringRemediation'),
            tooltip: true,
            change: function (orMonitoringRemediation) {
                model.set('orMonitoringRemediation', orMonitoringRemediation.get('_id'));
                view.ui.orMonitoringRemediationIndicator.hide();
            }
        });
    }
});
var PcaobMeberFirmProfileView = Marionette.View.extend({
    template: '#memberFirmPcaobProfile',
    behaviors: [{ behaviorClass: Authorization.RBAC, viewName: 'memberFirmPcaobProfileView' }],
    initialize: function () {
        Validation.bind(this);
    },
    ui: {
        pcaobUltimateResponsibilityIndicator: "#pcaobUltimateResponsibilityIndicator",
        pcaobOperationalResponsibilitySqmIndicator: "#pcaobOperationalResponsibilitySqmIndicator",
        pcaobOrIndependenceRequirementIndicator: "#pcaobOrIndependenceRequirementIndicator",
        pcaobOrMonitoringRemediationIndicator: "#pcaobOrMonitoringRemediationIndicator",
        pcaobOrMonitoringRemediationComponentsIndicator:'#pcaobOrMonitoringRemediationComponentsIndicator',
        pcaobUltimateResponsibility: '#pcaobUltimateResponsibility',
        pcaobOperationalResponsibilitySqm: '#pcaobOperationalResponsibilitySqm',
        pcaobOrIndependenceRequirement: '#pcaobOrIndependenceRequirement',
        pcaobOrMonitoringRemediation: '#pcaobOrMonitoringRemediation',
        pcaobOperationalResponsibilityGovernanceAndLeadership:'#pcaobOperationalResponsibilityGovernanceAndLeadership',
        pcaobOperationalResponsibilityAcceptanceAndContinuance:'#pcaobOperationalResponsibilityAcceptanceAndContinuance',
        pcaobOperationalResponsibilityEngagementPerformance:'#pcaobOperationalResponsibilityEngagementPerformance',
        pcaobOperationalResponsibilityResources:'#pcaobOperationalResponsibilityResources',
        pcaobOperationalResponsibilityInformationAndCommunication:'#pcaobOperationalResponsibilityInformationAndCommunication',
    },
    regions: {
        pcaobUltimateResponsibility: '#pcaobUltimateResponsibility',
        pcaobOperationalResponsibilitySqm: '#pcaobOperationalResponsibilitySqm',
        pcaobOrIndependenceRequirement: '#pcaobOrIndependenceRequirement',
        pcaobOrMonitoringRemediation: '#pcaobOrMonitoringRemediation',
        pcaobOperationalResponsibilityGovernanceAndLeadership:'#pcaobOperationalResponsibilityGovernanceAndLeadership',
        pcaobOperationalResponsibilityAcceptanceAndContinuance:'#pcaobOperationalResponsibilityAcceptanceAndContinuance',
        pcaobOperationalResponsibilityEngagementPerformance:'#pcaobOperationalResponsibilityEngagementPerformance',
        pcaobOperationalResponsibilityResources:'#pcaobOperationalResponsibilityResources',
        pcaobOperationalResponsibilityInformationAndCommunication:'#pcaobOperationalResponsibilityInformationAndCommunication',
    },
    modelEvents:{
        'change:firmGroupId':function(){
            this.ui.pcaobUltimateResponsibility.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOperationalResponsibilitySqm.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOrIndependenceRequirement.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOrMonitoringRemediation.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOperationalResponsibilityGovernanceAndLeadership.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOperationalResponsibilityAcceptanceAndContinuance.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOperationalResponsibilityEngagementPerformance.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOperationalResponsibilityResources.find('#searchInput').attr("placeholder", Labels.Select);
            this.ui.pcaobOperationalResponsibilityInformationAndCommunication.find('#searchInput').attr("placeholder", Labels.Select);
        }
    },
    onAttach: function () {
        var model = this.model;
        var view = this;
        var options = this.options;
        var abbreviation = model.get('firmGroupId') ? model.get('firmGroupId') : model.get('abbreviation');
        options.titles.each(function (item) {
            item.set('titleId', _.isObject(item.get('_id')) ? item.get('_id').$oid : item.get('_id'));
        });

        var pcaobUltimateResponsibilityModel = options.titles.findWhere({ titleId: model.get("pcaobUltimateResponsibility") ?  model.get("pcaobUltimateResponsibility").$oid : '' });
        var pcaobOperationalResponsibilitySqmModel = options.titles.findWhere({ titleId:model.get("pcaobOperationalResponsibilitySqm") ? model.get("pcaobOperationalResponsibilitySqm").$oid : '' });
        var pcaobOrIndependenceRequirementModel = options.titles.findWhere({ titleId: model.get("pcaobOrIndependenceRequirement") ? model.get("pcaobOrIndependenceRequirement").$oid : '' });
        var pcaobOrMonitoringRemediationModel = options.titles.findWhere({ titleId: model.get("pcaobOrMonitoringRemediation") ? model.get("pcaobOrMonitoringRemediation").$oid : '' });
        var pcaobOperationalResponsibilityGovernanceAndLeadership = options.titles.findWhere({ titleId: model.get("pcaobOperationalResponsibilityGovernanceAndLeadership") ? model.get("pcaobOperationalResponsibilityGovernanceAndLeadership").$oid : '' });
        var pcaobOperationalResponsibilityAcceptanceAndContinuance = options.titles.findWhere({ titleId: model.get("pcaobOperationalResponsibilityAcceptanceAndContinuance") ? model.get("pcaobOperationalResponsibilityAcceptanceAndContinuance").$oid : '' });
        var pcaobOperationalResponsibilityEngagementPerformance = options.titles.findWhere({ titleId: model.get("pcaobOperationalResponsibilityEngagementPerformance") ? model.get("pcaobOperationalResponsibilityEngagementPerformance").$oid : '' });
        var pcaobOperationalResponsibilityResources = options.titles.findWhere({ titleId: model.get("pcaobOperationalResponsibilityResources") ? model.get("pcaobOperationalResponsibilityResources").$oid : '' });
        var pcaobOperationalResponsibilityInformationAndCommunication = options.titles.findWhere({ titleId: model.get("pcaobOperationalResponsibilityInformationAndCommunication") ? model.get("pcaobOperationalResponsibilityInformationAndCommunication").$oid : '' });

        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobUltimateResponsibilityModel && pcaobUltimateResponsibilityModel.get("name") + " (" + getAssignedUsers({
                model: pcaobUltimateResponsibilityModel,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobUltimateResponsibilityModel,
            region: this.getRegion('pcaobUltimateResponsibility'),
            tooltip: true,
            change: function (pcaobUltimateResponsibility) {
                model.set('pcaobUltimateResponsibility', pcaobUltimateResponsibility.get('_id'));
                view.ui.pcaobUltimateResponsibilityIndicator.hide();
            }
        });

        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOperationalResponsibilitySqmModel && pcaobOperationalResponsibilitySqmModel.get("name") + " (" + getAssignedUsers({
                model: pcaobOperationalResponsibilitySqmModel,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOperationalResponsibilitySqmModel,
            region: this.getRegion('pcaobOperationalResponsibilitySqm'),
            tooltip: true,
            change: function (pcaobOperationalResponsibilitySqm) {
                model.set('pcaobOperationalResponsibilitySqm', pcaobOperationalResponsibilitySqm.get('_id'));
                view.ui.pcaobOperationalResponsibilitySqmIndicator.hide();
            }
        });

        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOrIndependenceRequirementModel && pcaobOrIndependenceRequirementModel.get("name") + " (" + getAssignedUsers({
                model: pcaobOrIndependenceRequirementModel,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOrIndependenceRequirementModel,
            region: this.getRegion('pcaobOrIndependenceRequirement'),
            tooltip: true,
            change: function (pcaobOrIndependenceRequirement) {
                model.set('pcaobOrIndependenceRequirement', pcaobOrIndependenceRequirement.get('_id'));
                view.ui.pcaobOrIndependenceRequirementIndicator.hide();
            }
        });

        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOrMonitoringRemediationModel && pcaobOrMonitoringRemediationModel.get("name") + " (" + getAssignedUsers({
                model: pcaobOrMonitoringRemediationModel,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOrMonitoringRemediationModel,
            region: this.getRegion('pcaobOrMonitoringRemediation'),
            tooltip: true,
            change: function (pcaobOrMonitoringRemediation) {
                model.set('pcaobOrMonitoringRemediation', pcaobOrMonitoringRemediation.get('_id'));
                view.ui.pcaobOrMonitoringRemediationIndicator.hide();
            }
        });

        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOperationalResponsibilityGovernanceAndLeadership && pcaobOperationalResponsibilityGovernanceAndLeadership.get("name") + " (" + getAssignedUsers({
                model: pcaobOperationalResponsibilityGovernanceAndLeadership,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOperationalResponsibilityGovernanceAndLeadership,
            region: this.getRegion('pcaobOperationalResponsibilityGovernanceAndLeadership'),
            tooltip: true,
            change: function (pcaobOrMonitoringRemediationComponents) {
                model.set('pcaobOperationalResponsibilityGovernanceAndLeadership', pcaobOrMonitoringRemediationComponents.get('_id'));
                view.ui.pcaobOrMonitoringRemediationComponentsIndicator.hide();
            }
        });
        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOperationalResponsibilityAcceptanceAndContinuance && pcaobOperationalResponsibilityAcceptanceAndContinuance.get("name") + " (" + getAssignedUsers({
                model: pcaobOperationalResponsibilityAcceptanceAndContinuance,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOperationalResponsibilityAcceptanceAndContinuance,
            region: this.getRegion('pcaobOperationalResponsibilityAcceptanceAndContinuance'),
            tooltip: true,
            change: function (pcaobOrMonitoringRemediationComponents) {
                model.set('pcaobOperationalResponsibilityAcceptanceAndContinuance', pcaobOrMonitoringRemediationComponents.get('_id'));
                view.ui.pcaobOrMonitoringRemediationComponentsIndicator.hide();
            }
        });
        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOperationalResponsibilityEngagementPerformance && pcaobOperationalResponsibilityEngagementPerformance.get("name") + " (" + getAssignedUsers({
                model: pcaobOperationalResponsibilityEngagementPerformance,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOperationalResponsibilityEngagementPerformance,
            region: this.getRegion('pcaobOperationalResponsibilityEngagementPerformance'),
            tooltip: true,
            change: function (pcaobOrMonitoringRemediationComponents) {
                model.set('pcaobOperationalResponsibilityEngagementPerformance', pcaobOrMonitoringRemediationComponents.get('_id'));
                view.ui.pcaobOrMonitoringRemediationComponentsIndicator.hide();
            }
        });
        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOperationalResponsibilityResources && pcaobOperationalResponsibilityResources.get("name") + " (" + getAssignedUsers({
                model: pcaobOperationalResponsibilityResources,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOperationalResponsibilityResources,
            region: this.getRegion('pcaobOperationalResponsibilityResources'),
            tooltip: true,
            change: function (pcaobOrMonitoringRemediationComponents) {
                model.set('pcaobOperationalResponsibilityResources', pcaobOrMonitoringRemediationComponents.get('_id'));
                view.ui.pcaobOrMonitoringRemediationComponentsIndicator.hide();
            }
        });
        TypeAheadDropdown.show({
            collection: options.titles,
            textAttribute: 'titleUserName',
            valueAttribute: 'titleId',
            selectedValue: (pcaobOperationalResponsibilityInformationAndCommunication && pcaobOperationalResponsibilityInformationAndCommunication.get("name") + " (" + getAssignedUsers({
                model: pcaobOperationalResponsibilityInformationAndCommunication,
                firmId: abbreviation,
                titleAssignments: options.titleAssignments
            }) + ")") || Labels.Select,
            selectedModel: pcaobOperationalResponsibilityInformationAndCommunication,
            region: this.getRegion('pcaobOperationalResponsibilityInformationAndCommunication'),
            tooltip: true,
            change: function (pcaobOrMonitoringRemediationComponents) {
                model.set('pcaobOperationalResponsibilityInformationAndCommunication', pcaobOrMonitoringRemediationComponents.get('_id'));
                view.ui.pcaobOrMonitoringRemediationComponentsIndicator.hide();
            }
        });
    }
});
var MeberFirmGroupProfileView = Marionette.View.extend({
    template: '#memberFirmGroupProfile',
    initialize: function () {
        Validation.bind(this);
    },
    ui: {        
        sqmLeadershipViewContainer: "#sqmLeadershipViewContainer",
        sqmLeadershipViewTabSection: '#sqmLeadershipViewTabSection'
    },
    regions: {
        sqmLeadershipViewContainer: "#sqmLeadershipViewContainer",
        sqmLeadershipViewTabSection: '#sqmLeadershipViewTabSection'
    },
    onRender: function () {
        FirmTitlesManager.start(_.extend(this.options, {
           region: this.getRegion('sqmLeadershipViewTabSection'),
           firm: this.model,
           hideFooter: true,
           type: 'SQM'
       }));
            
    }
        
});
var EmptyMFGTitleList = Marionette.View.extend({
    template: '#emptyMFGTitleViewList'
});
var MemberFirmGroupProfileCollectionView = Marionette.CollectionView.extend({
    childView: MeberFirmGroupProfileView,
    emptyView: EmptyMFGTitleList,
    childViewOptions: function (item) {
        return _.extend(this.options, { model: item })
    }
    
});


var View = Marionette.View.extend({
    template:'#addMemberFirm',
    className: 'pad-right-1em',
    behaviors: [{ behaviorClass: Behaviors.TextFieldAutoGrow }, { behaviorClass: Authorization.RBAC, viewName: 'addMemberFirmView' }],
    regions: {
        countryNetworkDropDown:'#countryNetworkDropDown',
        languageTranslateDropDown:'#languageTranslateDropDown',
        eyHierarchy: '#eyHierarchy',
        entityGroupTabSection:'#entityGroupTabSection',
        memberFirmGroup: '#memberFirmGroup',
        type: '#type',
        memberFirmStatus:'#memberFirmStatus'
    },
    ui: {
        abbreviationSection: "#abbreviationSection",
        changeAnswer: "#changeAnswer",
        changeTypeAnswerMessage: "#changeTypeAnswerMessage",
        changeAnswerMessage: "#changeAnswerMessage",
        changeAnswerSection: "#changeAnswerSection",
        countryNetworkDropDown: "#countryNetworkDropDown",
        entityNameLabel:'#entityNameLabel',
        eyHierarchyContainer: '#eyHierarchyContainer',
        geoLocation: '#geoLocation',
        isEntityRadio: "#isEntityRadio",
        isGroup: '#isGroup',
        isTranslateNeed: '#isTranslateNeed',
        sliderForLanguage: '#sliderForLanguage',
        translateDropdown:'#translateDropdown',
        languageOptionsFieldset: '#languageOptionsFieldset',
        languageTranslateDropDown:'languageTranslateDropDown',
        isPartOfGroup: "[name=\"isPartOfGroupRadio\"]",
        isPartOfGroupSection: "#isPartOfGroup",
        memberFirmGroup: "#memberFirmGroupfieldset",
        memberFirmGroupDropDown: "#memberFirmGroup",
        typeDropdown: '#type',
        typeSection:"#typeSection",
        sliderForRapValidation:'#sliderForRapValidation',
        sliderForQC1000Profiling:'#sliderForQC1000Profiling',
        rapValidation:'#rapValidation',
        qc1000Profiling:'#qc1000Profiling',
        enableSaveFirm:'#enableSaveFirm',
        reviewSaveFirm:'#reviewSaveFirm',        
        memberFirmStatus:'#memberFirmStatus',
        memberFirmStatusSection: '#memberFirmStatusSection'
    },
    events: {
        'click #sliderForLanguage': 'toggle',
        'click #sliderForRapValidation' : 'toggleRapValidation',
        'click #sliderForQC1000Profiling' : 'toggleQC1000Profiling',
        'blur #abbreviation': function (e) {
            var val = e.target.value.replace(/\s/g, '');
            this.model.set("abbreviation", val);
            this.model.set("recordFirmAbbreviation", val);
            this.model.set("checkForAbbreviation", true);
        },
        'blur #name': function (e) {
            var val = e.target.value.trim();
            this.model.set("name", val);            
            },

        "change @ui.isPartOfGroup": function (e) {
            this.model.set('isPartOfGroup', e.target.value);
            this.ui.changeAnswerMessage.hide();
        },
        "click @ui.changeAnswer": function (e) {
             this.ui.changeAnswerSection.hide();
             this.ui.isPartOfGroupSection.show();
             if (this.model.get("isPartOfGroup") === IsNotPartOfGroup) {
                 this.ui.changeAnswerMessage.show();
             }
        },
        'click #reviewSaveFirm': function () {
            if (this.ui.reviewSaveFirm.prop("checked")) {
                this.model.set('isMFProfilingReviewed', true);
                this.triggerMethod('child:profilingTypeSelected', this);
            } else {
                this.model.set('isMFProfilingReviewed', false);
                this.triggerMethod('child:profilingTypeSelected', this);
            }
        }
    },
    modelEvents: {
        "change:isPartOfGroup": function () {
            var view = this;
            if (view.model.get("isPartOfGroup") === IsPartOfGroup) {
                view.model.id && (view.model.get('type') === 'EntityType_MemberFirm')
                    && view.ui.typeSection.prop("disabled", true);
            } else {
                view.model.id && (view.model.get('type') === 'EntityType_MemberFirm')
                    && view.ui.typeSection.prop("disabled", false);
            }
            if (view.model.get("type") === "EntityType_MemberFirm") {
                view.model.set("isPartOfGroup", IsPartOfGroup);
            }
            else {
                view.model.set("isPartOfGroup", IsNotPartOfGroup);
            }
            AddFirmRule.run({
                ruleName: "ShowFirmGroup",
                toBeHidden: this.ui.memberFirmGroup,
                model: this.model,
                resetElements: function () {
                    if (view.model.get("isPartOfGroup") === IsPartOfGroup) {
                        $('input:radio[name="isPartOfGroupRadio"][value=' + IsPartOfGroup + ']').prop('checked', true);
                    } else {
                        $('input:radio[name="isPartOfGroupRadio"][value=' + IsNotPartOfGroup + ']').prop('checked', true);
                        view.ui.memberFirmGroupDropDown.find('#searchInput').attr("placeholder", Labels.Select);
                    }
                },
            });
            AddFirmRule.run({
                ruleName: "ShowAbbreviation",
                toBeHidden: this.ui.abbreviationSection,
                model: this.model
            });

            AddFirmRule.run({
                ruleName: "EyHierarchy",
                toBeHidden: this.ui.eyHierarchyContainer,
                model: this.model,
                allFirms: view.options.allFirms
            });

            AddFirmRule.run({
                ruleName: "EnableEyHierarchy",
                toBeEnabled: this.ui.eyHierarchyContainer,
                model: this.model
            });
            view.triggerMethod('child:entityTypeSelected', view);
        },
        "change:type": function () {
            var view = this;
            this.model.validation = _.extend(this.model.validation, {
                languageCode: { required: true, msg: Labels.ErrorMsg }
            });
            if (view.model.get("type") === "EntityType_MemberFirm") {
                view.model.set("isPartOfGroup", IsPartOfGroup);
                delete this.model.validation.languageCode;  
            }
            else {
                view.model.set("isPartOfGroup", IsNotPartOfGroup);
                delete this.model.validation.languageCode;  
            }
            if (view.model.get('type') === "EntityType_Group") {
                var tabLayout = this.options.tabLayout;
                var mfs = new Entities.Firms(view.model.get("firmsUnderGroup"));
                tabLayout.getRegion('memberFirmProfileContainer').show(new MemberFirmGroupProfileCollectionView(_.extend(this.options, { collection: mfs })));
            }
            if (view.model.get('type') === "EntityType_MemberFirm") {
                tabLayout = this.options.tabLayout;
                tabLayout.getRegion('memberFirmProfileContainer').show(new MeberFirmProfileView(_.extend(this.options, { model: view.model })));
            }
            if (view.model.id && view.model.get("type") === "EntityType_Group") {
                view.model.get('country') && view.model.set('country', '');
                this.ui.changeTypeAnswerMessage.show();
            }
            else if(view.model.id){
                this.ui.changeTypeAnswerMessage.hide();
                delete this.model.validation.languageCode;  
            }
            AddFirmRule.run({
                ruleName: "EntityTypeFirm",
                toBeShown: [this.ui.geoLocation],
                model: this.model,
                resetElements: function () {
                    view.ui.countryNetworkDropDown.find('#searchInput').attr("placeholder", Labels.Select);
                    view.ui.memberFirmGroupDropDown.find('#searchInput').attr("placeholder", Labels.Select);
                    $('input:radio[name="isPartOfGroupRadio"][value=' + IsNotPartOfGroup + ']').prop('checked', false);
                    $('input:radio[name="isPartOfGroupRadio"][value=' + IsNotPartOfGroup + ']').prop('checked', true);
                }
            });
            if (!view.model.id){
                AddFirmRule.run({
                    ruleName: "EyHierarchy",
                    toBeHidden: this.ui.eyHierarchyContainer,
                    model: this.model,
                    allFirms: view.options.allFirms
                });
            }
            AddFirmRule.run({
                ruleName: "ShowAbbreviation",
                toBeHidden: this.ui.abbreviationSection,
                model: this.model
            });
            AddFirmRule.run({
                ruleName: "ShowFirmGroup",
                toBeHidden: this.ui.memberFirmGroup,
                model: this.model,
                resetElements: function () {
                    if (view.model.get("isPartOfGroup") === IsPartOfGroup) {
                        $('input:radio[name="isPartOfGroupRadio"][value=' + IsPartOfGroup + ']').prop('checked', true);
                    } else {
                        $('input:radio[name="isPartOfGroupRadio"][value=' + IsNotPartOfGroup + ']').prop('checked', true);
                        view.ui.memberFirmGroupDropDown.find('#searchInput').attr("placeholder", Labels.Select);
                    }
                },
            });
            AddFirmRule.run({
                ruleName: "hideIsPartOfGroupSection",
                toBeHidden: this.ui.isGroup,
                model: this.model
            });
            AddFirmRule.run({
                ruleName: "NeedAbilityToTranslate",
                toBeShown: this.ui.isTranslateNeed,
                model: this.model
            });
            AddFirmRule.run({
                ruleName: "isMemberFirmPCAOBRegistered",
                toBeShown: this.ui.qc1000Profiling,
                model: this.model
            });
            AddFirmRule.run({
                ruleName: "isRapValidationApplicable",
                toBeShown: this.ui.rapValidation,
                model: this.model
            });
        }
    },
    toggle: function () {
        if (this.ui.sliderForLanguage.hasClass(toggleOff)) {
            this.ui.sliderForLanguage.removeClass(toggleOff);
            this.ui.sliderForLanguage.addClass(toggleOn);
            this.ui.languageOptionsFieldset.removeClass('d-none');
            this.ui.languageTranslateDropDown.removeClass('d-none');
            this.ui.languageOptionsFieldset.find('#searchInput').attr("placeholder",Labels.Select);
            this.model.set('languageCode', '');
            this.model.set('isAltContentEnabled',true);
            this.model.validation = _.extend(this.model.validation, {
                languageCode: { required: true, msg: Labels.ErrorMsg }
            });
        }
        else {
            this.ui.sliderForLanguage.removeClass(toggleOn);
            this.ui.languageOptionsFieldset.addClass('d-none');
            this.ui.languageTranslateDropDown.addClass('d-none');
            this.ui.sliderForLanguage.addClass(toggleOff);
            this.model.set('isAltContentEnabled',false);
            delete this.model.validation.languageCode;
        }
    },
    toggleRapValidation: function(){
        if(this.ui.sliderForRapValidation.hasClass(toggleOff)){
           this.ui.sliderForRapValidation.removeClass(toggleOff);
           this.ui.sliderForRapValidation.addClass(toggleOn);
           this.model.set('isRapValidationEnabled',true);
    }
    else{
           this.ui.sliderForRapValidation.removeClass(toggleOn);
           this.ui.sliderForRapValidation.addClass(toggleOff);
           this.model.set('isRapValidationEnabled',false);
    }
    },
    toggleQC1000Profiling: function(){
    var allMFsUnderLoc = this.options.allFirms.find({abbreviation: this.model.get('firmGroupId')}).get('firmsUnderGroup');
    var memberFirmIdModel = this.model.get('memberFirmId');
    var categoryOfSelectedInMFs = allMFsUnderLoc.find(function(item){ return item.memberFirmId === memberFirmIdModel});
        if(this.ui.sliderForQC1000Profiling.hasClass(toggleOff)){
           this.ui.sliderForQC1000Profiling.removeClass(toggleOff);
           this.ui.sliderForQC1000Profiling.addClass(toggleOn);
           this.model.set('categoryType','EntityType_PCAOB'); /*To set the categoryType into MF model attribute. */
           if(categoryOfSelectedInMFs){categoryOfSelectedInMFs.categoryType = 'EntityType_PCAOB'}; /*To set the categoryType into firmsUnderGroup attribute of firm model. */
    }
    else{
           this.ui.sliderForQC1000Profiling.removeClass(toggleOn);
           this.ui.sliderForQC1000Profiling.addClass(toggleOff);
           this.model.set('categoryType','EntityType_NonPCAOB');
           if(categoryOfSelectedInMFs){categoryOfSelectedInMFs.categoryType = 'EntityType_NonPCAOB'};
        }
    },
    initialize: function () {
        Validation.bind(this);
    },
    templateContext: function () {   
        var isPartOfGroupTypeYes = _.find(Resources.Enumeration, {
            type: 'IsPartOfGroupType',
            id: 'IsPartOfGroupType_Yes'
        }).id;
        var isPartOfGroupTypeNo = _.find(Resources.Enumeration, {
            type: 'IsPartOfGroupType',
            id: 'IsPartOfGroupType_No'
        }).id;

        var modelid = "";
        var editDisabled="";
        var disabled="";
        var hidden="";
        var show ="";
        var showChangeAnswer ="";
        var editFirmTypeDisabled="";
        if(this.model.id){
         editDisabled="disabled";
         modelid="disabled";
         if(this.model.get("isPartOfGroup") === IsNotPartOfGroup && this.model.get('type') === 'EntityType_MemberFirm'){
            editFirmTypeDisabled="";
        }else{
            editFirmTypeDisabled=modelid;
        }
        if(this.model.get("isPartOfGroup") === IsNotPartOfGroup && this.model.get('type') !== 'EntityType_Network'){
            showChangeAnswer="";
        }else{
            showChangeAnswer="d-none";
        }
         if(this.model.get("isPartOfGroup") === IsNotPartOfGroup){
            disabled="disabled";
            hidden= "d-none";
            show="";
         }else{
            show= "d-none";
         }     
        }else{
            editFirmTypeDisabled=modelid;
            showChangeAnswer="d-none";
            show="d-none";
        }
        return {
            modalHeight: this.model.modalHeight,
            editFirmTypeDisabled: editFirmTypeDisabled,
            editDisabled: editDisabled,
            disabled: disabled,
            disableLanguageEdit: this.model.get("languageDropdownValues") ? "disableLanguageEdit" : "" ,
            hidden:hidden,
            show: show,
            showChangeAnswer: showChangeAnswer,
            isPartOfGroupTypeYes: isPartOfGroupTypeYes,
            isPartOfGroupTypeNo: isPartOfGroupTypeNo,
            isPartOfGroupText: this.model.get("isPartOfGroup") === isPartOfGroupTypeYes ? Labels.Boolean_Yes : Labels.Boolean_No,
            isPartOfGroupTypeYesChecked: this.model.get("isPartOfGroup") === isPartOfGroupTypeYes ? "checked" : "",
            isPartOfGroupTypeNoChecked: this.model.get("isPartOfGroup") === isPartOfGroupTypeNo ? "checked" : "",
            isMemberFirm: this.model.get('type') === 'EntityType_MemberFirm' ? "d-none" : "" 
        };
    },
    onRender: function () {
        var countries = new Entities.Countries(Resources.Country);
        var countryModel = countries.findWhere({ id: this.model.get("country") });
        var memberGroupModel = this.options.allFirms.findWhere({ type:"EntityType_Group",abbreviation: this.model.get("firmGroupId") });
        var languageCountry = new Entities.Languages(_.filter(Resources.Enumeration,{type:'LanguageType'}));
        var languageModel = languageCountry.findWhere({languageCode : this.model.get("languageCode")});
        var showConfigurationalPCAOB = false;
        var model = this.model;
        this.options.entityTypesFiltered = this.options.entityTypes.clone();
        if (model.isNew() && _.any(this.options.allFirms.findWhere({ type: "EntityType_Network" }))) {
            model.set("type", "EntityType_MemberFirm");
            model.get("type") === "EntityType_MemberFirm" ? model.set("isPartOfGroup", IsPartOfGroup) : model.set("isPartOfGroup", IsNotPartOfGroup);
            this.options.entityTypesFiltered = new Backbone.Collection(this.options.entityTypesFiltered.reject(function (type) {
                return type.get('_id') === "EntityType_Network"
            }));
        }
        AddFirmRule.run({
            ruleName: "NeedAbilityToTranslate",
            toBeShown: this.ui.isTranslateNeed,
            model: this.model
        });
        AddFirmRule.run({
            ruleName: "isMemberFirmPCAOBRegistered",
            toBeShown: this.ui.qc1000Profiling,
            model: this.model
        });
        var typeModel = this.options.entityTypes.findWhere({ id: this.model.get("type") }) || new Backbone.Model();
        var view = this;
        if(!this.ui.sliderForLanguage.hasClass(toggleOn)){
            this.ui.languageOptionsFieldset.addClass('d-none');
            this.ui.languageTranslateDropDown.addClass('d-none');
        }
        if(!this.model.isNew() && model.get('isAltContentEnabled')){
            this.ui.sliderForLanguage.addClass(toggleOn);
            this.ui.languageOptionsFieldset.removeClass('d-none');
            this.ui.languageTranslateDropDown.removeClass('d-none');
        }
        if(!this.model.isNew() && model.get('isRapValidationEnabled')){
            this.ui.sliderForRapValidation.addClass(toggleOn);
            this.ui.sliderForRapValidation.removeClass(toggleOff);
        }
        if(this.model.isNew()){
            this.ui.sliderForRapValidation.addClass(toggleOn);
            this.model.set('isRapValidationEnabled',true);
            this.ui.sliderForRapValidation.removeClass(toggleOff);
        }
        if(!this.model.isNew() && this.ui.sliderForLanguage.hasClass(toggleOff)){
            delete this.model.validation.languageCode;
        }
        if(!this.model.isNew() && model.get('categoryType') === 'EntityType_PCAOB'){
            this.ui.sliderForQC1000Profiling.addClass(toggleOn);
            this.ui.sliderForQC1000Profiling.removeClass(toggleOff);
        }
        if(!this.model.isNew() && this.model.get('type') === 'EntityType_MemberFirm'){
            this.ui.rapValidation.addClass('d-none');
            if(this.model.get('categoryType') === 'EntityType_PCAOB' && this.model.get("pcaobMemberFirmsCount") && this.model.get("pcaobMemberFirmsCount") === 1 )
                {
                    showConfigurationalPCAOB = true;                   
                }
        }

        /*Message Acknowledge Checkbox/Message for PCAOB Profiling */
        if(model.get('categoryType') !== 'EntityType_PCAOB' || (!this.model.isNew())){
            this.ui.enableSaveFirm.hide();
        }
        var updateFirmProfiling = function () {
            if(this.model.get('categoryType') === 'EntityType_PCAOB'){
                this.ui.enableSaveFirm.show()
            }else{
                this.ui.enableSaveFirm.hide();
                this.model.set('isMFProfilingReviewed', false);
                this.model.set('categoryType', 'EntityType_NonPCAOB');
                this.ui.reviewSaveFirm.prop("checked", false);
                this.ui.sliderForQC1000Profiling.addClass(toggleOff) && this.ui.sliderForQC1000Profiling.removeClass(toggleOn);
            }
            this.triggerMethod('child:profilingTypeSelected', this);
        }
        this.listenTo(model, 'change:categoryType', updateFirmProfiling, this);
        TypeAheadDropdown.show({
            collection: new Entities.Countries(countries.filter({isRetired:false})),
            textAttribute: 'name',
            valueAttribute: 'id',
            selectedValue: (countryModel && countryModel.get("name"))  || Labels.Select,
            selectedModel: countryModel,
            region: this.getRegion('countryNetworkDropDown'),
            tooltip: true,
            change: function (country) {
                if (model.id && model.get("country") !== country.id) {
                    model.set("changedCountry", true);
                }
                model.set('country', country.id);
            }
        });
        TypeAheadDropdown.show({
            collection: languageCountry,
            textAttribute: 'name',
            valueAttribute: 'languageCode',
            selectedValue:languageModel ? languageModel.get("name") : Labels.Select,
            selectedModel: languageModel,
            region: this.getRegion('languageTranslateDropDown'),
            tooltip: true,
            change: function (language) {
                model.set('languageCode', language.get('languageCode'));
            }
        })  
        TypeAheadDropdown.show({
            collection: new Entities.Firms(this.options.allFirms.where({fiscalYear:Configuration.FiscalYear, type: "EntityType_Group"})),
            textAttribute: 'name',
            valueAttribute: 'abbreviation',
            selectedValue: (memberGroupModel && memberGroupModel.get("name")) || Labels.Select,
            selectedModel: memberGroupModel,
            region: this.getRegion('memberFirmGroup'),
            tooltip: true,
            change: function (firmGroup) {
                if (model.get('firmGroupId') !== firmGroup.get("abbreviation")) {
                    model.set('firmGroupId', firmGroup.get("abbreviation"));
                    !model.isNew() && model.set({
                        ultimateResponsibility: '',
                        operationalResponsibilitySqm: '',
                        orIndependenceRequirement: '',
                        orMonitoringRemediation: ''
                    }) && model.set({
                        pcaobUltimateResponsibility: '',
                        pcaobOperationalResponsibilitySqm: '',
                        pcaobOrIndependenceRequirement: '',
                        pcaobOrMonitoringRemediation: '',
                        pcaobOperationalResponsibilityGovernanceAndLeadership:''
                    });
                    view.options.titleAssignments.fetch({
                        filter: { fiscalYear: Configuration.FiscalYear, firmId: firmGroup.get("abbreviation") },
                        select: { titleId: 1, assignments: 1, firmId: 1 }
                    }).done(function (res) {
                        view.options.titleAssignments.reset(res);
                        view.options.clonedTitles = new Entities.Titles(view.options.clonedTitles.clone().toJSON(), { firmId: firmGroup.get("abbreviation"), titleAssignments: view.options.titleAssignments });
                        var filterTitles = view.options.clonedTitles.filter(function (title) {
                            return (title.get("global") || title.get('firmId') === firmGroup.get("abbreviation"))
                                && (title.set('titleId', _.isObject(title.get('_id')) ? title.get('_id').$oid : title.get('_id')));
                        });
                        view.options.titles.reset(filterTitles);
                    });
                }
            }
        });
        this.options.entityTypesFiltered.comparator = 'order';
        this.options.entityTypesFiltered.sort();
        TypeAheadDropdown.show({
            collection: model.id ? new Backbone.Collection(this.options.entityTypesFiltered.filter(function (et) {
                return et.id === "EntityType_MemberFirm" || et.id === "EntityType_Group";
            })) : this.options.entityTypesFiltered,
            resourceKeyAttribute:'id',
            valueAttribute: 'id',
            selectedValue: (typeModel && Labels[typeModel.get("id")]) || Labels.Select,
            selectedModel: typeModel,
            tooltip: false,
            region: this.getRegion('type'),
            change: function (type) {
                model.get('type') !== type.get('id') && model.set('type', type.get("id"));
                if (type.get("id") === 'EntityType_MemberFirm' || type.get("id") === 'EntityType_Group') {
                    var abbreviation = model.get('firmGroupId') ? model.get('firmGroupId') : model.get('abbreviation');
                    var filterTitles = view.options.clonedTitles.filter(function (title) {
                        return (title.get("global") || title.get('firmId') === abbreviation)
                            && (title.set('titleId', _.isObject(title.get('_id')) ? title.get('_id').$oid : title.get('_id')));
                    });
                    view.options.titles.reset(filterTitles);
                }
                view.triggerMethod('child:entityTypeSelected', view);
            }
        });

        EyHierarchy.show(_.extend(this.options, {
            region: this.getRegion('eyHierarchy')
        }))

        if(showConfigurationalPCAOB){
            this.ui.memberFirmStatusSection.removeClass('d-none');
            var memberFirmStatuses = new Backbone.Collection(_.filter(Resources.Enumeration, {
                        type: 'BooleanStatus'
                    }));
            var memberFirmStatusModel = model.get('memberFirmStatus') && memberFirmStatuses.find({ id: model.get('memberFirmStatus') });
            
            TypeAheadDropdown.show({
                collection: memberFirmStatuses,
                valueAttribute: 'id',
                resourceKeyAttribute: 'id',
                selectedValue: Labels[(memberFirmStatusModel && memberFirmStatusModel.get("id"))] || Labels.Select,
                selectedModel: memberFirmStatusModel,
                region: this.getRegion('memberFirmStatus'),
                tooltip: true,
                change: function (selectedValue) {
                    model.set('memberFirmStatus', selectedValue.id)
                }
            });
        }
    },
    onAttach: function () {
        var view = this;
        if (view.model.get("type") === "EntityType_MemberFirm") {
            view.model.set("isPartOfGroup", IsPartOfGroup);
        }
        else {
            view.model.set("isPartOfGroup", IsNotPartOfGroup);
        }
        AddFirmRule.run({
            ruleName: "ShowFirmGroup",
            toBeHidden: this.ui.memberFirmGroup,
            model: this.model,
            resetElements: function () {
                if (view.model.get("isPartOfGroup") === IsNotPartOfGroup) {
                    view.ui.memberFirmGroupDropDown.find('#searchInput').attr("placeholder", Labels.Select);
                }
            }
        });
        AddFirmRule.run({
            ruleName: "ShowAbbreviation",
            toBeHidden: this.ui.abbreviationSection,
            model: this.model
        });
        AddFirmRule.run({
            ruleName: "EntityTypeFirm",
            toBeShown: [this.ui.geoLocation],
            model: this.model,
            resetElements: function () {
                view.ui.countryNetworkDropDown.find('#searchInput').attr("placeholder", Labels.Select);
                $('input:radio[name="isPartOfGroupRadio"][value=' + IsNotPartOfGroup + ']').prop('checked', false);
                $('input:radio[name="isPartOfGroupRadio"][value=' + IsNotPartOfGroup + ']').prop('checked', true);
            }
        });
        AddFirmRule.run({
            ruleName: "EyHierarchy",
            toBeHidden: this.ui.eyHierarchyContainer,
            model: this.model,
            allFirms: view.options.allFirms
        });
    }
});

var EntityTabLayout = Marionette.View.extend({
    template: '#memberFirmTabLayout',
    ui: {
        tabs: '.tab-group>a',
        addMemberFirmTab: "#addMemberFirmTab",
        addMemberFirmAttachmentsTab: "#addMemberFirmAttachmentsTab",
        addMemberFirmTabContainer: "#addMemberFirmTabContainer",
        addMemberFirmAttachmentsTabContainer: "#addMemberFirmAttachmentsTabContainer",
        disableFirmTab: ".disableFirmTab",
        wizardTabs:'.wizard-tab',
        memberFirmProfileContainer:'#memberFirmProfileContainer',
        // pcaobMemberFirmProfileContainer:'#pcaobMemberFirmProfileContainer',
        memberFirmProfileTabContainer:'#memberFirmProfileTab',
        pcaobMemberFirmProfileTabContainer:'#pcaobMemberFirmProfileTab',
        memberFirmProfileLabel:'#memberFirmProfileLabel',
        pcaobMemberFirmProfileLabel:'#pcaobMemberFirmProfileLabel',
        entityDetailsTabOuter: '#entityDetailsTab',
        entityDetailsTab:'#entityDetailsTab .inner-round-tab span',
        memberFirmProfileTab: '#memberFirmProfileTab .inner-round-tab span',
        pcaobMemberFirmProfileTab:'#pcaobMemberFirmProfileTab .inner-round-tab span',
        attachmentsTab:'#attachmentsTab .inner-round-tab span',
        sliderForLanguage:'#sliderForLanguage',
        horizontalline:'#horizontal-line'
    },
    regions: {
        addMemberFirmTabContainer: "#addMemberFirmTabContainer",
        memberFirmProfileContainer:'#memberFirmProfileContainer',
        // pcaobMemberFirmProfileContainer:'#pcaobMemberFirmProfileContainer'
    },
    initialize:function(){
        var view = this;
        this.listenTo(this.model, "change", function () {
            if (!this.model.isNew()) {
                this.model.isValid(true);
            }
            view.onErrorWizard();
        });
    },
    events: {
        'click @ui.tabs': 'onTabClick',
        'click .wizard-tab':'wizardClick'
    },
    onErrorWizard: function () {
        if(!_.all(Object.values(this.ui),function(elem){return elem instanceof $})){
            return
        }
        this.ui.entityDetailsTab.removeClass('fa-exclamation error-msg');
        this.ui.memberFirmProfileTab.removeClass('fa-exclamation error-msg');
        this.ui.entityDetailsTab.parent().removeClass('error');
        this.ui.memberFirmProfileTab.parent().removeClass('error');
        if(this.ui.addMemberFirmTabContainer.find('.fa-exclamation').length > 0){
            this.ui.entityDetailsTab.addClass('fa-exclamation  error-msg');
            this.ui.entityDetailsTab.removeClass('fa-check');
            this.ui.entityDetailsTab.parent().addClass('error');
        }else{
           !this.model.isNew() && this.ui.entityDetailsTab.addClass('fa-check');
        }
        if(this.ui.memberFirmProfileContainer.find('.fa-exclamation').length > 0){
            this.ui.memberFirmProfileTab.addClass('fa-exclamation  error-msg');
            this.ui.memberFirmProfileTab.removeClass('fa-check');
            this.ui.memberFirmProfileTab.parent().addClass('error');
        }else {
            !this.model.isNew() && this.ui.memberFirmProfileTab.addClass('fa-check');
        }
        if(this.ui.addMemberFirmAttachmentsTabContainer.find('.fa-exclamation').length > 0){
            this.ui.attachmentsTab.addClass('fa-exclamation  error-msg');
            this.ui.attachmentsTab.removeClass('fa-check');
            this.ui.attachmentsTab.parent().addClass('error');
        }else{
            !this.model.isNew() && this.ui.attachmentsTab.addClass('fa-check');
        }
    },
    childViewEvents:{
        'child:entityTypeSelected':function(childView){
            if ((childView.model.get('type') === 'EntityType_MemberFirm' || childView.model.get('type') === 'EntityType_Group')){
                this.ui.memberFirmProfileTabContainer.addClass('wizard-tab');
                this.ui.memberFirmProfileTabContainer.removeClass('invisible');
                this.ui.memberFirmProfileLabel.removeClass('invisible');
                this.ui.horizontalline.removeClass('invisible');
                this.ui.entityDetailsTabOuter.addClass('mar-right-25em');
                this.triggerMethod('child:click:firstwizard',this);
                this.triggerMethod('child:click:back:off',this);

            }else{
                this.ui.memberFirmProfileTabContainer.removeClass('wizard-tab');
                this.ui.memberFirmProfileTabContainer.addClass('invisible');
                this.ui.pcaobMemberFirmProfileTabContainer.removeClass('wizard-tab');
                this.ui.pcaobMemberFirmProfileTabContainer.addClass('d-none');
                this.ui.memberFirmProfileLabel.addClass('invisible');
                this.ui.pcaobMemberFirmProfileLabel.addClass('invisible');
                this.ui.horizontalline.addClass('invisible');
                this.ui.entityDetailsTabOuter.removeClass('mar-right-25em');
                this.triggerMethod('child:click:lastwizard',this);
                this.triggerMethod('child:click:back:off',this);
                this.model.set({
                    ultimateResponsibility: '', operationalResponsibilitySqm: '',
                    orIndependenceRequirement: '', orMonitoringRemediation: ''
                });
                /*Reset all the values responsible for MF -> PCAOB changes, only when the model is new, i.e if the user switches the entity type while crwation. */
                if(this.model.isNew()){
                    this.model.set('categoryType', 'EntityType_NonPCAOB');
                    this.model.set('isMFProfilingReviewed', false);
                    this.model.set({
                        pcaobUltimateResponsibility: '', pcaobOperationalResponsibilitySqm: '',
                        pcaobOrIndependenceRequirement: '', pcaobOrMonitoringRemediation: '',
                        pcaobOperationalResponsibilityGovernanceAndLeadership: ''
                    });
                }
            }
            this.bindUIElements();
        },
        'child:profilingTypeSelected' : function(childView){
            var tabLayout = childView.options.tabLayout;
            if (childView.model.get('type') === 'EntityType_MemberFirm'){
                if(childView.model.get('categoryType') === 'EntityType_PCAOB'){
                    if(!childView.model.get('isMFProfilingReviewed')){
                        this.triggerMethod('child:click:save:unchecked', this);
                        this.ui.pcaobMemberFirmProfileTabContainer.removeClass('wizard-tab');
                        this.ui.pcaobMemberFirmProfileTabContainer.addClass('d-none');
                        this.ui.pcaobMemberFirmProfileTab.addClass('d-none');
                        this.ui.pcaobMemberFirmProfileLabel.addClass('invisible');
                    }else{
                        this.triggerMethod('child:click:save:checked', this);
                        this.ui.pcaobMemberFirmProfileTabContainer.addClass('wizard-tab');
                        this.ui.pcaobMemberFirmProfileTabContainer.removeClass('d-none');
                        this.ui.pcaobMemberFirmProfileTab.removeClass('d-none');
                        this.ui.pcaobMemberFirmProfileLabel.removeClass('invisible');
                        // tabLayout.getRegion('pcaobMemberFirmProfileContainer').show(new PcaobMeberFirmProfileView(_.extend(childView.options, { model: childView.options.model })));
                    }
                }
                else{
                    this.triggerMethod('child:click:save:checked', this);
                    this.ui.pcaobMemberFirmProfileTabContainer.removeClass('wizard-tab');
                    this.ui.pcaobMemberFirmProfileTabContainer.addClass('d-none');
                    this.ui.pcaobMemberFirmProfileTab.addClass('d-none');
                    this.ui.pcaobMemberFirmProfileLabel.addClass('invisible');
                }
            }
        }
    },
    onTabClick: function (e) {
        this.ui.tabs.removeClass('active');
        $(e.target).addClass('active');
    },
    wizardNext:function(){
        var view = this;
        return function(){
            var activeIndex = view.ui.wizardTabs.index(view.$el.find('.active-wizard'));
            if(activeIndex < view.ui.wizardTabs.length -1){
                var nextWizardTab = $(view.ui.wizardTabs[activeIndex + 1]);
                nextWizardTab.trigger('click');
            }
        }
    },
    wizardBack:function(){
        var view = this;
        return function(){
            var activeIndex = view.ui.wizardTabs.index(view.$el.find('.active-wizard'));
            if(activeIndex > 0){
                var previousWizardTab = $(view.ui.wizardTabs[activeIndex - 1]);
                previousWizardTab.trigger('click');
            }
        }
    },
    wizardClick:function(e){
        this.ui.wizardTabs.removeClass('active-wizard');
        this.ui.wizardTabs.removeClass('active');
        $(e.currentTarget).addClass('active-wizard');
        if(this.model.get('isMFProfilingReviewed')){
            if(this.ui.wizardTabs.index(e.currentTarget) === this.ui.wizardTabs.length -1){
                this.options.currentWizardStep = 'lastwizard';
                this.triggerMethod('child:click:lastwizard',this)
            }
            else if(this.ui.wizardTabs.index(e.currentTarget) === 0){
                this.options.currentWizardStep = 'firstwizard';
                this.triggerMethod('child:click:firstwizard',this)
            }
            else {
                this.options.currentWizardStep = 'middlewizard';
                this.triggerMethod('child:click:middlewizard',this)
            }
        }
        else if (this.ui.wizardTabs.index(e.currentTarget) === 0) {
            this.options.currentWizardStep = 'firstwizard';
            this.triggerMethod('child:click:firstwizard', this)
        }
        else {
            this.options.currentWizardStep = 'lastwizard';
            this.triggerMethod('child:click:lastwizard', this)
        }
    },
    onNavigateRequiredTab: function () {
        this.ui.addMemberFirmTab.trigger('click');
        this.ui.addMemberFirmTab.find('.error-group').addClass('fa fa-exclamation-triangle');
    },
    onAttach:function(){
        if(this.model.get('type') === 'EntityType_MemberFirm'){
            this.ui.memberFirmProfileTabContainer.addClass('wizard-tab');
            this.ui.memberFirmProfileTabContainer.removeClass('invisible');
            this.ui.memberFirmProfileLabel.removeClass('invisible');
            this.bindUIElements();
        }
        if(this.model.get('isMFProfilingReviewed')){
            this.ui.pcaobMemberFirmProfileTabContainer.addClass('wizard-tab');
            this.ui.pcaobMemberFirmProfileTabContainer.removeClass('d-none');
            this.ui.pcaobMemberFirmProfileLabel.removeClass('invisible');
        }
        if(!this.model.isNew()){
            this.ui.wizardTabs.addClass('completed');
            this.ui.entityDetailsTab.addClass('fa fa-check');
            this.ui.memberFirmProfileTab.addClass('fa fa-check');
            this.ui.pcaobMemberFirmProfileTab.addClass('fa fa-check');
        }
    }
});

module.exports.start = function(options){
    var model = options.firm || (options.admin.readonly ? new Entities.Firm({isReadOnly:true}) : new Entities.Firm());
    var abbreviation = model.get('firmGroupId') ? model.get('firmGroupId') : model.get('abbreviation');
    options.allFirms = new Entities.Firms();
    options.entityTypes = new Backbone.Collection(EntityTypes);
    options.addedAttachments = new Entities.Attachments();
    options.deletedAttachments = new Entities.Attachments();
    options.titles = new Entities.Titles();
    options.ultimateResponsibilityTitlesCollection = new Entities.Titles();
    options.operationalResponsibilityTitlesCollection = new Entities.Titles();
    options.globalAdmins = new Entities.Users();
    options.titleAssignments = new Entities.TitleAssignments();
    options.referenceObjectsUniqueIds = new Entities.ReferenceObjectsUniqueIds();
    options.getMaxCountMemberFirm= new Entities.ReferenceObjectsUniqueIds();
    var allFirmsFetch = options.allFirms.fetch({
        aggregate: [
            {
                $match: {
                    fiscalYear: Configuration.FiscalYear
                }
            },
            {
                $lookup: {
                    from: "firm",
                    let: {
                        abbreviation: "$abbreviation"
                    },
                    pipeline: [{
                        $match: {
                            fiscalYear: Configuration.FiscalYear,
                            $expr: {
                                $and: [{
                                    $ne: ["$$abbreviation", null]
                                }, {
                                    $eq: ["$$abbreviation", "$firmGroupId"]
                                }]
                            }
                        }
                    }],
                    as: "firmsUnderGroup"
                }
            },
            {
                $project: {
                    id: 1,
                    _id: 1,
                    name: 1,
                    abbreviation: 1,
                    geographyId: 1,
                    isPartOfGroup: 1,
                    parentFirmId: 1,
                    type: 1,
                    ultimateResponsibility: 1,
                    operationalResponsibilitySqm: 1,
                    orIndependenceRequirement: 1,
                    orMonitoringRemediation: 1,
                    pcaobUltimateResponsibility: 1,
                    pcaobOperationalResponsibilitySqm: 1,
                    pcaobOrIndependenceRequirement: 1,
                    pcaobOrMonitoringRemediation: 1,
                    pcaobOperationalResponsibilityGovernanceAndLeadership: 1,
                    fiscalYear: 1,
                    isReadOnly:1,
                    publishedDate: 1,
                    isPublishQueryRun: 1,
                    isRapValidationEnabled:1,
                    publishedBy: 1,
                    publishedUserDisplayName: 1,
                    country: 1,
                    isAltContentEnabled:1,
                    isArchivalComplete:1,
                    categoryType:1,
                    firmsUnderGroup:1,
                    languageCode:1,
                    isRollForwardedFromPreFY:1,
                    rollForwardStatus: 1,
                    isCreatedInCurrentFY: 1,
                    prevId:1,
                    isRollForwardTriggered:1,
                    modifiedBy:1,
                    modifiedOn:1,
                    isRollFwdComplete:1,
                    rollForwardByDisplayName:1,
                    rollForwardByEmail:1,
                    rollForwardDate:1, 
                    firstPublishedDate:1,
                    createdBy:1,
                    createdOn:1
                }
            }
        ]
    });
    options.globalAdmins.fetch({
        filter: { 
            superAdminScopeYear: { $regex: "RoleType_GlobalAdmin_" + Configuration.FiscalYear }
        }
    });
    var titlesFetch = options.titles.fetch({
        filter: { fiscalYear: Configuration.FiscalYear },
        select: {
            id: 1,
            _id: 1,
            name: 1,
            global: 1,
            firmId: 1,
            assignments: 1,
            fiscalYear: 1
        }
    });
    var titleAssignmentsFetch = options.titleAssignments.fetch({
        filter: { fiscalYear: Configuration.FiscalYear, firmId: abbreviation },
        select: { titleId: 1, assignments: 1, firmId: 1, fiscalYear: 1 }
    });

    var uniqueFirms = options.referenceObjectsUniqueIds.fetch({
        filter: { 'type': { $eq: 'firm' } },
        select: {
            name: 1,
            firmId: 1            
        }
    });

    var maxCountOfMemberFirmFetch=options.getMaxCountMemberFirm.fetch({
        aggregate: [{
            $match: { type: 'firm' }
        }, {
            $sort: { memberFirmId: -1 }
        }, {
            $limit: 1
        }]

    })

    

    var complete = [allFirmsFetch, titlesFetch, titleAssignmentsFetch, uniqueFirms,maxCountOfMemberFirmFetch];
    !model.isNew() && complete.push(model.fetch());
    $.when.apply(this, complete).done(function () {

        //this is to check if only one PCAOB member firm exists in the collection
        if (options.model && options.model.collection && Array.isArray(options.model.collection.models)) {
            const count = options.model.collection.models.reduce((acc, model) => {
                const categoryType = model.get && model.get('categoryType');
                return categoryType === 'EntityType_PCAOB' ? acc + 1 : acc;
            }, 0);
            if (count === 1) {
                 model.set({pcaobMemberFirmsCount: count});
            }
        }
        
        var tabLayout = new EntityTabLayout(_.extend(options, { model: model }));
        options.memberFirmAttachments = new Entities.Attachments(model.get('attachments'));
        options.clonedTitles = new Entities.Titles(options.titles.clone().toJSON(), { firmId: abbreviation, titleAssignments: options.titleAssignments });
        tabLayout.getRegion('addMemberFirmTabContainer').show(new View(_.extend(options, { model: model }, { tabLayout: tabLayout })));
        if(model.get('type') === "EntityType_MemberFirm") {
            tabLayout.getRegion('memberFirmProfileContainer').show(new MeberFirmProfileView(_.extend(options, { model: model })));
        }
        if (model.get('type') === "EntityType_Group") {
            var mfs = new Entities.Firms(model.get("firmsUnderGroup"));
            tabLayout.getRegion('memberFirmProfileContainer').show(new MemberFirmGroupProfileCollectionView(_.extend(options, { collection: mfs })));
        }
        // if(model.get('isMFProfilingReviewed')) {
            //tabLayout.getRegion('pcaobMemberFirmProfileContainer').show(new PcaobMeberFirmProfileView(_.extend(options, { model: model })));
        // }
        Dialog.show({
            headerLabel: (!options.firm || options.firm.isNew()) ? Labels.AddFirm : Labels.EditMemberFirm,
            cancelLabel: Labels.Cancel,
            saveLabel: Labels.Save,
            bodyHeaderLabel: Labels.PIADisclaimer,
            nextLabel: Labels.Next,
            backLabel: Labels.Back,
            cancelClass: 'float-left',
            validate: model,
            admin: options.admin,
            hideScroll:true,
            wizardOptions: {
                next: tabLayout.wizardNext(),
                back: tabLayout.wizardBack()
            },
            serverValidation: function () {
                var deferred = $.Deferred();
                var promise = deferred.promise();
                var firms = new Entities.Firms();
                firms.fetch({
                    select: {
                        abbreviation: 1,
                        geographyId: 1
                    }
                }).done(function () {
                    model.set("firms", firms);
                    model.set("uniqueFirms", options.referenceObjectsUniqueIds);
                    deferred.resolve();
                });
                return promise;
            },
            view: tabLayout,
          modalHeight: "modal-sqm-lg-ht",
            size: "modal-sqm-lg",
            validateMethod:function(){
                if(!options.model.isValid(true)){
                    tabLayout.triggerMethod('errorWizard');
                    return false;
                }
                return true;
            },
            onDialogOpen: function () {
                if(!options.model.isNew()){
                    this.validateMethod();
                }
            }
        }).done(function () {
            var isqc = require('ISQC');
            var isNew = model.isNew();
            if (model.get("isPartOfGroup") === _.find(Resources.Enumeration, {
                type: 'IsPartOfGroupType',
                id: 'IsPartOfGroupType_Yes'
            }).id) {
                model.set("abbreviation", null);
            }
            if (model.get("type") === "EntityType_MemberFirm") {
                model.set("isPartOfGroup", IsPartOfGroup);
            }
            else {
                model.set("isPartOfGroup", IsNotPartOfGroup);
            }
            if(options.model.isNew())
            {
                model.set('isCreatedInCurrentFY',true);
            }
            options.firm = model;
            var recordFirmAbbreviation = model.get("recordFirmAbbreviation");
            if (model.get("type") === "EntityType_Network") {
                var netWorkFirms = new Entities.Firms();
                netWorkFirms.fetch({
                    aggregate: [{
                                    $match: 
                                    {
                                        $and: [{ fiscalYear: Configuration.FiscalYear }, { type: { $eq: "EntityType_Network" } }]
                                    }
                                }]
                }).done(function (items) {
                    var networkFirm = netWorkFirms.first();
                    if (networkFirm && networkFirm.get("abbreviation") !== model.get("abbreviation")) {
                        isqc.showToast({
                            result: "danger",
                            text: Labels.NetworkExist
                        });
                    }
                    else {
                        FirmActionSave.run(options).done(function () {
                            isqc.showToast({
                                result: "success",
                                text: Labels.EntitySaved
                            });
                            if (options.queries) {
                                options.queries.each(function (model) {
                                    model.get('type') == "Quicklink" && model.get('selected') && model.set('selected', false, { silent: true });
                                });

                                options.queries.findWhere({ type: "Quicklink", id: 'Firms' }).set({ selected: true });
                            }
                            if (isNew && model.get("isPartOfGroup") === 'IsPartOfGroupType_No') {

                                options.globalAdmins.slice().reduce(function (xhrArray, ga) {
                                    var geography = ga.get("geography") + "|" + "/" + model.get("abbreviation") + "/";
                                    // uniq is not working on geography hence we have used filter
                                    ga.set("geography", geography.split("|").filter((x, i, a) => a.indexOf(x) == i).sort().join("|"))
                                    ga.save();
                                    return xhrArray;
                                }, []);
                            }
                        });
                    }
                });
            } else {
                FirmActionSave.run(options).done(function () {
                    isqc.showToast({
                        result: "success",
                        text: Labels.EntitySaved
                    });
                    if (options.queries) {
                        options.queries.each(function (model) {
                            model.get('type') == "Quicklink" && model.get('selected') && model.set('selected', false, { silent: true });
                        });

                        options.queries.findWhere({ type: "Quicklink", id: 'Firms' }).set({ selected: true });
                    }
                    if (!model.isNew() && recordFirmAbbreviation && model.get("isPartOfGroup") === 'IsPartOfGroupType_Yes') {
                        DocumentationDelete.run({ firmId: recordFirmAbbreviation });
                    }
                    if (isNew && model.get("isPartOfGroup") === 'IsPartOfGroupType_No') {

                        options.globalAdmins.slice().reduce(function (xhrArray, ga) {
                            var geography = ga.get("geography") + "|" + "/" + model.get("abbreviation") + "/";
                            // uniq is not working on geography hence we have used filter
                            ga.set("geography", geography.split("|").filter((x, i, a) => a.indexOf(x) == i).sort().join("|"))
                            ga.save();
                            return xhrArray;
                        }, []);
                    }
                });
            }
            if(!model.isNew()){
                if (options.firms) {
                    options.firms.add(model, { merge: true });
                }
                if (options.allFirms)
                    options.allFirms.add(model, { merge: true });
            }
        });
    });



}
