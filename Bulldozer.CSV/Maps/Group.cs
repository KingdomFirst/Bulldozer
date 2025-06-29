﻿// <copyright>
// Copyright 2023 by Kingdom First Solutions
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>
//
using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Globalization;
using System.Linq;
using static Bulldozer.CSV.CSVInstance;
using static Bulldozer.Utility.CachedTypes;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the People import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Processes the group type list
        /// </summary>
        private void ImportGroupTypes()
        {
            var importDateTime = RockDateTime.Now;
            var rockContext = new RockContext();
            var groupTypeService = new GroupTypeService( rockContext );
            var groupTypeErrors = string.Empty;

            if ( this.GroupTypeDict == null )
            {
                LoadGroupTypeDict( rockContext );
            }

            GroupTypeCache.Clear();

            this.ReportProgress( 0, $"Preparing GroupType data for import..." );

            var csvMissingGroupTypes = this.GroupTypeCsvList.Where( t => !GroupTypeDict.ContainsKey( string.Format( "{0}^{1}", ImportInstanceFKPrefix, t.Id ) ) ).ToList();

            // First check for GroupTypes that don't exist by foreign key match, but do match by name and add foreign key info to them.

            var csvMissingGroupTypeNameList = csvMissingGroupTypes.Select( t => t.Name ).ToList();
            var groupTypesToUpdate = new GroupTypeService( rockContext ).Queryable()
                                        .Where( t => ( t.ForeignKey == null || t.ForeignKey.Trim() == "" ) && csvMissingGroupTypeNameList.Any( n => n == t.Name ) )
                                        .GroupBy( t => t.Name )
                                        .ToList()
                                        .Select( t => new { GroupType = t.FirstOrDefault(), GroupTypeCsv = csvMissingGroupTypes.FirstOrDefault( gt => t.FirstOrDefault().Name == gt.Name ) } )
                                        .ToList();

            if ( groupTypesToUpdate.Count() > 0 )
            {
                var updatedGroupTypeCsvList = new List<GroupTypeCsv>();
                foreach ( var groupTypeObj in groupTypesToUpdate )
                {
                    groupTypeObj.GroupType.ForeignKey = $"{ImportInstanceFKPrefix}^{groupTypeObj.GroupTypeCsv.Id}";
                    groupTypeObj.GroupType.ForeignId = groupTypeObj.GroupTypeCsv.Id.AsIntegerOrNull();
                    groupTypeObj.GroupType.ForeignGuid = groupTypeObj.GroupTypeCsv.Id.AsGuidOrNull();
                    updatedGroupTypeCsvList.Add( groupTypeObj.GroupTypeCsv );
                }
                rockContext.SaveChanges();
                foreach ( var groupTypeCsv in updatedGroupTypeCsvList )
                {
                    csvMissingGroupTypes.Remove( groupTypeCsv );
                }
            }
            this.ReportProgress( 0, $"{GroupTypeCsvList.Count() - csvMissingGroupTypes.Count} already exist and will be skipped." );

            // Now process new group types

            this.ReportProgress( 0, $"Begin processing {csvMissingGroupTypes.Count} GroupType Records..." );
            if ( csvMissingGroupTypes.Count() > 0 )
            {
                var locationTypeValues = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE ).DefinedValues.ToList();
                var groupTypesToCreate = new List<GroupType>();
                foreach ( var importGroupType in csvMissingGroupTypes )
                {
                    var newGroupType = new GroupType()
                    {
                        ForeignId = importGroupType.Id.AsIntegerOrNull(),
                        ForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, importGroupType.Id ),
                        Name = importGroupType.Name,
                        Guid = Guid.NewGuid(),
                        ShowInGroupList = importGroupType.ShowInGroupList.GetValueOrDefault(),
                        ShowInNavigation = importGroupType.ShowInNav.GetValueOrDefault(),
                        GroupTerm = "Group",
                        GroupMemberTerm = "Member",
                        Description = importGroupType.Description,
                        TakesAttendance = importGroupType.TakesAttendance.GetValueOrDefault(),
                        AllowMultipleLocations = importGroupType.AllowMultipleLocations.GetValueOrDefault(),
                        EnableLocationSchedules = importGroupType.EnableLocationSchedules.GetValueOrDefault(),
                        IsSchedulingEnabled = importGroupType.IsSchedulingEnabled.GetValueOrDefault(),
                        AttendanceCountsAsWeekendService = importGroupType.WeekendService.GetValueOrDefault(),
                        IsSystem = false,
                        CreatedDateTime = importGroupType.CreatedDateTime.HasValue ? importGroupType.CreatedDateTime.ToSQLSafeDate() : importDateTime,
                        ModifiedDateTime = importDateTime,
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ModifiedByPersonAliasId = ImportPersonAliasId
                    };

                    if ( importGroupType.GroupTypePurpose.IsNotNullOrWhiteSpace() )
                    {
                        var purposeDVId = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.GROUPTYPE_PURPOSE ).DefinedValues.FirstOrDefault( v => v.Value.Equals( importGroupType.GroupTypePurpose ) || v.Id.Equals( importGroupType.GroupTypePurpose.AsIntegerOrNull() ) || v.Guid.ToString().ToLower().Equals( importGroupType.GroupTypePurpose.ToLower() ) )?.Id;
                        if ( purposeDVId.HasValue )
                        {
                            newGroupType.GroupTypePurposeValueId = purposeDVId;
                        }
                        else
                        {
                            groupTypeErrors += $"GroupType,Invalid Purpose ({importGroupType.GroupTypePurpose}) provided for GroupTypeId {importGroupType.Id}. Purpose not set for this Group Type.,{DateTime.Now}\r\n";
                        }
                    }

                    if ( importGroupType.InheritedGroupTypeGuid.IsNotNullOrWhiteSpace() )
                    {
                        var inheritedGroupTypeId = LoadGroupTypeId( rockContext, importGroupType.InheritedGroupTypeGuid, this.ImportInstanceFKPrefix, false );
                        if ( !inheritedGroupTypeId.HasValue )
                        {
                            groupTypeErrors += $"GroupType, Invalid InheritedGroupTypeGuid ({importGroupType.InheritedGroupTypeGuid}) provided for GroupTypeId {importGroupType.Id}. InheritedGroupType not set for this Group Type.,{DateTime.Now}\r\n";
                        }
                        else
                        {
                            newGroupType.InheritedGroupTypeId = inheritedGroupTypeId;
                        }
                    }

                    if ( importGroupType.SelfReference.GetValueOrDefault() )
                    {
                        var selfReferenceList = new List<GroupType>();
                        selfReferenceList.Add( newGroupType );
                        newGroupType.ChildGroupTypes = selfReferenceList;
                    }

                    if ( importGroupType.AllowWeeklySchedule.GetValueOrDefault() )
                    {
                        newGroupType.AllowedScheduleTypes = ScheduleType.Weekly;
                    }

                    if ( importGroupType.IsValidLocationSelectionMode )
                    {
                        newGroupType.LocationSelectionMode = ( GroupLocationPickerMode ) importGroupType.LocationSelectionModeEnum.Value;
                    }          

                    // Add default role of Member
                    var defaultRoleGuid = Guid.NewGuid();
                    var memberRole = new GroupTypeRole { Guid = defaultRoleGuid, Name = "Member", ForeignKey = $"{this.ImportInstanceFKPrefix}^{importGroupType.Id}_Member" };
                    newGroupType.Roles.Add( memberRole );

                    groupTypesToCreate.Add( newGroupType );
                }

                groupTypeService.AddRange( groupTypesToCreate );
                rockContext.SaveChanges();
                
                foreach ( var groupType in groupTypesToCreate )
                {
                    groupType.DefaultGroupRole = groupType.Roles.FirstOrDefault();
                }

                List<GroupTypeCsv> groupTypesWithParents = csvMissingGroupTypes.Where( gt => !string.IsNullOrWhiteSpace( gt.ParentGroupTypeId ) ).ToList();

                var importedGroupTypes = new GroupTypeService( rockContext ).Queryable().Where( t => t.Id != FamilyGroupTypeId && t.ForeignKey != null && t.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) ).ToDictionary( k => k.ForeignKey, v => v );

                foreach ( var groupTypeCsv in groupTypesWithParents )
                {
                    GroupType groupType = importedGroupTypes.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupTypeCsv.Id ) );
                    if ( groupType != null )
                    {
                        var parentGroupType = importedGroupTypes.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupTypeCsv.ParentGroupTypeId ) );
                        if ( parentGroupType != null && !groupType.ParentGroupTypes.Any( t => t.Id == parentGroupType.Id ) )
                        {
                            if ( groupType.ParentGroupTypes == null )
                            {
                                groupType.ParentGroupTypes = new List<GroupType>();
                            }
                            groupType.ParentGroupTypes.Add( parentGroupType );
                        }
                        else if ( groupType.ParentGroupTypes.Any( t => t.Id == parentGroupType.Id ) )
                        {
                            // Parent GroupType is already attached to the group type. Ignore.
                        }
                        else
                        {
                            groupTypeErrors += $"GroupType,ParentGroupTypeId {groupTypeCsv.ParentGroupTypeId} not found. GroupType {groupTypeCsv.Name} ({groupTypeCsv.Id}) has been added as a root group type,{DateTime.Now.ToString()}\r\n";
                        }
                    }
                }

                List<GroupTypeCsv> groupTypesWithLocationTypes = csvMissingGroupTypes.Where( gt => !string.IsNullOrWhiteSpace( gt.LocationTypes ) ).ToList();
                var groupTypeLocationTypes = new List<GroupTypeLocationType>();
                foreach ( var groupTypeCsv in groupTypesWithLocationTypes )
                {
                    GroupType groupType = importedGroupTypes.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupTypeCsv.Id ) );
                    if ( groupType != null )
                    {
                        foreach ( var locType in groupTypeCsv.LocationTypes.Split( ',' ).ToList() )
                        {
                            var locationTypeDV = locationTypeValues.FirstOrDefault( v => v.Value.ToLower() == locType.ToLower() );
                            if ( locationTypeDV != null )
                            {
                                groupTypeLocationTypes.Add(
                                    new GroupTypeLocationType
                                    {
                                        LocationTypeValueId = locationTypeDV.Id,
                                        GroupTypeId = groupType.Id
                                    }
                                );
                            }
                            else
                            {
                                groupTypeErrors += $"GroupType,LocationType {locType} not found. It has not been added to Location Types for GroupType {groupTypeCsv.Name} ({groupTypeCsv.Id}),{DateTime.Now.ToString()}\r\n";
                            }
                        }
                    }
                }
                if ( groupTypeLocationTypes.Count > 0 )
                {
                    rockContext.GroupTypeLocationTypes.AddRange( groupTypeLocationTypes );
                }

                rockContext.SaveChanges();

                if ( groupTypeErrors.IsNotNullOrWhiteSpace() )
                {
                    LogException( null, groupTypeErrors, hasMultipleErrors: true );
                }
            }
            ReportProgress( 0, $"Finished GroupType import: {csvMissingGroupTypes.Count} GroupTypes added." );
            LoadGroupTypeDict( rockContext );
        }

        /// <summary>
        /// Processes the group list.
        /// </summary>
        private int ImportGroups( List<GroupCsv> groupCsvList = null, string groupTerm = "Group" )
        {
            ReportProgress( 0, $"Preparing {groupTerm} data for import..." );

            if ( this.GroupTypeDict == null )
            {
                LoadGroupTypeDict();
            }
            if ( this.LocationsDict == null )
            {
                LoadLocationDict();
            }

            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }

            var groupImportList = new List<GroupImport>();
            var invalidGroups = new List<string>();
            var invalidGroupTypes = new List<string>();
            var invalidCampuses = new List<string>();
            var invalidCampusGroups = new List<string>();

            if ( groupCsvList == null )
            {
                groupCsvList = this.GroupCsvList;
            }

            var groupCsvsToProcess = groupCsvList.Where( g => !this.GroupDict.ContainsKey( $"{this.ImportInstanceFKPrefix}^{g.Id}" ) );

            if ( groupCsvsToProcess.Count() < groupCsvList.Count() )
            {
                this.ReportProgress( 0, $"{groupCsvList.Count() - groupCsvsToProcess.Count()} {groupTerm}(s) from import already exist and will be skipped." );
            }

            var locationTypeValues = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE ).DefinedValues.ToList();

            foreach ( var groupCsv in groupCsvsToProcess )
            {
                var groupCsvName = groupCsv.Name;
                if ( groupCsv.Name.IsNullOrWhiteSpace() )
                {
                    groupCsvName = $"Unnamed {groupTerm}";
                }
                int? groupTypeId = 0;
                if ( groupTerm == "FundraisingGroup" )
                {
                    groupTypeId = FundRaisingGroupTypeId;
                }
                else
                {
                    groupTypeId = this.GroupTypeDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{groupCsv.GroupTypeId}" )?.Id;
                }
                if ( !groupTypeId.HasValue )
                {
                    invalidGroups.Add( groupCsv.Id );
                    invalidGroupTypes.Add( groupCsv.GroupTypeId );
                    continue;
                }
                var groupImport = new GroupImport()
                {
                    GroupForeignId = groupCsv.Id.AsIntegerOrNull(),
                    GroupForeignKey = $"{this.ImportInstanceFKPrefix}^{groupCsv.Id}",
                    GroupTypeId = groupTypeId.Value,
                    Name = groupCsvName,
                    Description = groupCsv.Description,
                    IsActive = groupCsv.IsActive.GetValueOrDefault(),
                    IsPublic = groupCsv.IsPublic.GetValueOrDefault(),
                    Capacity = groupCsv.Capacity,
                    CreatedDate = groupCsv.CreatedDate,
                    MeetingDay = groupCsv.MeetingDay,
                    MeetingTime = groupCsv.MeetingTime,
                    Order = groupCsv.Order,
                    ParentGroupForeignId = groupCsv.ParentGroupId.AsIntegerOrNull(),
                    ParentGroupForeignKey = string.IsNullOrWhiteSpace( groupCsv.ParentGroupId ) ? null : string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupCsv.ParentGroupId )
                };

                if ( groupCsv.CampusId.IsNotNullOrWhiteSpace() )
                {
                    var campusIdInt = groupCsv.CampusId.AsIntegerOrNull();
                    Campus campus = null;
                    if ( this.UseExistingCampusIds && campusIdInt.HasValue )
                    {
                        campus = this.CampusesDict.GetValueOrNull( campusIdInt.Value );
                    }
                    else
                    {
                        campus = this.CampusImportDict.GetValueOrNull( $"{ImportInstanceFKPrefix}^{groupCsv.CampusId}" );
                    }

                    groupImport.CampusId = campus?.Id;
                    if ( !groupImport.CampusId.HasValue )
                    {
                        invalidCampusGroups.Add( groupCsv.Id );
                        invalidCampuses.Add( groupCsv.CampusId );
                    }
                }

                if ( groupCsv.LocationId.IsNotNullOrWhiteSpace() )
                {
                    var location = LocationsDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{groupCsv.LocationId}" );
                    if ( location != null )
                    {
                        groupImport.Location = location;
                    }
                }

                if ( groupCsv.GroupLocationType.IsNotNullOrWhiteSpace() )
                {
                    groupImport.GroupLocationTypeValueId = locationTypeValues.FirstOrDefault( v => v.Value.ToLower() == groupCsv.GroupLocationType.ToLower() )?.Id;
                }

                groupImportList.Add( groupImport );
            }
            if ( invalidGroupTypes.Count > 0 && invalidGroups.Count > 0 )
            {
                LogException( $"{groupTerm}Import", $"The following invalid GroupType(s) in the {groupTerm} csv resulted in {invalidGroups.Count} group(s) being skipped:\r\n{string.Join( ", ", invalidGroupTypes )}\r\nSkipped GroupId(s):\r\n{string.Join( ", ", invalidGroups )}." );
            }
            if ( invalidCampuses.Count > 0 && invalidCampusGroups.Count > 0 )
            {
                LogException( $"{groupTerm}Import", $"The following invalid Campus(es) in the {groupTerm} csv resulted in {invalidCampusGroups.Count} group(s) not having a campus set:\r\n{string.Join( ", ", invalidCampuses )}\r\nMissing Campus GroupId(s):\r\n{string.Join( ", ", invalidCampusGroups )}." );
            }

            this.ReportProgress( 0, $"Begin processing {groupImportList.Count} {groupTerm} Records..." );

            var rockContext = new RockContext();

            // Slice data into chunks and process
            var workingGroupImportList = groupImportList.ToList();
            var groupsRemainingToProcess = groupImportList.Count;
            var groupsWithParents = groupImportList.Where( g => g.ParentGroupForeignKey.IsNotNullOrWhiteSpace() ).ToList();
            var completedGroups = 0;
            var insertedGroups = new List<Group>();

            while ( groupsRemainingToProcess > 0 )
            {
                if ( completedGroups > 0 && completedGroups % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroups} {groupTerm}s processed." );
                }

                if ( completedGroups % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupImportList.Take( Math.Min( this.DefaultChunkSize, workingGroupImportList.Count ) ).ToList();
                    var imported = BulkGroupImport( rockContext, csvChunk, insertedGroups );
                    completedGroups += imported;
                    groupsRemainingToProcess -= csvChunk.Count;
                    workingGroupImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            var groupLookup = new GroupService( rockContext )
                                            .Queryable()
                                            .Where( g => g.GroupTypeId != FamilyGroupTypeId && g.GroupTypeId != KnownRelationshipGroupType.Id && g.ForeignKey != null && g.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                            .ToList()
                                            .ToDictionary( k => k.ForeignKey, v => v );

            // Process any new Schedules and GroupLocations needed
            BulkInsertGroupSchedules( insertedGroups, groupLookup );
            BulkInsertGroupLocations( insertedGroups, groupLookup );

            this.ReportProgress( 0, $"Begin updating {groupsWithParents.Count} Parent {groupTerm} Records..." );

            var workingGroupsWithParents = groupsWithParents.ToList();
            var groupsWithParentsRemainingToProcess = groupsWithParents.Count;
            var completedGroupsWithParents = 0;

            while ( groupsWithParentsRemainingToProcess > 0 )
            {
                if ( completedGroupsWithParents > 0 && completedGroupsWithParents % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroupsWithParents} {groupTerm} updated." );
                }

                if ( completedGroupsWithParents % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupsWithParents.Take( Math.Min( this.DefaultChunkSize, workingGroupsWithParents.Count ) ).ToList();
                    BulkUpdateParentGroup( rockContext, csvChunk, groupLookup );
                    completedGroupsWithParents += csvChunk.Count;
                    groupsWithParentsRemainingToProcess -= csvChunk.Count;
                    workingGroupsWithParents.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            // Update GroupTypes' Allowed Child GroupTypes based on groups that became child groups
            rockContext.Database.ExecuteSqlCommand( @"
INSERT INTO GroupTypeAssociation (
	GroupTypeId
	,ChildGroupTypeId
	)
SELECT DISTINCT pg.GroupTypeId [ParentGroupTypeId]
	,g.GroupTypeId [ChildGroupTypeId]
FROM [Group] g
INNER JOIN [Group] pg ON g.ParentGroupId = pg.id
INNER JOIN [GroupType] pgt ON pg.GroupTypeId = pgt.Id
INNER JOIN [GroupType] cgt ON g.GroupTypeId = cgt.Id
OUTER APPLY (
	SELECT *
	FROM GroupTypeAssociation
	WHERE GroupTypeId = pg.GroupTypeId
		AND ChildGroupTypeId = g.GroupTypeid
	) gta
WHERE gta.GroupTypeId IS NULL" );

            // make sure grouptype caches get updated in case 'allowed group types' changed
            foreach ( var groupTypeId in groupsWithParents.Select( g => g.GroupTypeId ) )
            {
                GroupTypeCache.UpdateCachedEntity( groupTypeId, EntityState.Detached );
            }

            LoadGroupTypeDict();
            LoadGroupDict();

            return completedGroups;
        }

        /// <summary>
        /// Bulk import of GroupImports.
        /// </summary>
        /// <param name="rockContext">The RockContext.</param>
        /// <param name="groupImports">The group imports.</param>
        /// <param name="insertedGroups">The list of inserted groups.</param>
        /// <returns></returns>
        public int BulkGroupImport( RockContext rockContext, List<GroupImport> groupImports, List<Group> insertedGroups )
        {
            var importedDateTime = RockDateTime.Now;
            var groupsToInsert = new List<Group>();

            foreach ( var groupImport in groupImports )
            {
                var newGroup = new Group();
                InitializeGroupFromGroupImport( newGroup, groupImport, importedDateTime );

                if ( groupImport.Location != null )
                {
                    newGroup.GroupLocations.Add( new GroupLocation
                    {
                        Group = newGroup,
                        CreatedDateTime = importedDateTime,
                        ModifiedDateTime = importedDateTime,
                        LocationId = groupImport.Location.Id,
                        ForeignKey = groupImport.Location.ForeignKey,
                        GroupLocationTypeValueId = groupImport.GroupLocationTypeValueId
                    } );
                }

                // set weekly schedule for newly created groups
                DayOfWeek meetingDay;
                if ( !string.IsNullOrWhiteSpace( groupImport.MeetingDay ) && Enum.TryParse( groupImport.MeetingDay, out meetingDay ) )
                {
                    TimeSpan.TryParse( groupImport.MeetingTime, out TimeSpan meetingTime );
                    newGroup.Schedule = new Schedule()
                    {
                        IsActive = newGroup.IsActive,
                        WeeklyDayOfWeek = meetingDay,
                        WeeklyTimeOfDay = meetingTime,
                        ForeignId = groupImport.GroupForeignId,
                        ForeignKey = groupImport.GroupForeignKey,
                        CreatedDateTime = importedDateTime,
                        ModifiedDateTime = importedDateTime
                    };
                };
                groupsToInsert.Add( newGroup );
            }

            rockContext.BulkInsert( groupsToInsert );
            insertedGroups.AddRange( groupsToInsert );

            return groupImports.Count;
        }

        /// <summary>
        /// Processes the Fundraising Group list.
        /// </summary>
        private int ImportFundraisingGroups()
        {
            ReportProgress( 0, "Preparing Fundraising Group data for import..." );

            if ( this.ImportedAccounts == null )
            {
                LoadImportedAccounts();
            }

            var completedGroups = 0;
            var groupCsvs = new List<GroupCsv>();
            var attributeValues = new List<GroupAttributeValueCsv>();
            var tripDefinedValue = DefinedValueCache.Get( "3BB5607B-8A77-434D-8AEF-F10D513BE963" );  // Rock "Trip" opportunity type defined value

            foreach ( var fundraisingGroupCsv in this.FundraisingGroupCsvList )
            {
                groupCsvs.Add( new GroupCsv
                {
                    Id = fundraisingGroupCsv.Id,
                    Name = fundraisingGroupCsv.Name,
                    Description = fundraisingGroupCsv.Description,
                    Order = fundraisingGroupCsv.Order,
                    GroupTypeId = fundraisingGroupCsv.GroupTypeId,
                    Capacity = fundraisingGroupCsv.Capacity,
                    IsPublic = fundraisingGroupCsv.IsPublic,
                    IsActive = fundraisingGroupCsv.IsActive,
                    CreatedDate = fundraisingGroupCsv.CreatedDate,
                    ParentGroupId = fundraisingGroupCsv.ParentGroupId
                } );

                attributeValues.Add( new GroupAttributeValueCsv
                {
                    GroupId = fundraisingGroupCsv.Id,
                    AttributeKey = "OpportunityTitle",
                    AttributeValue = fundraisingGroupCsv.Name,
                    AttributeValueId = "OpportunityTitle_" + fundraisingGroupCsv.Id
                } );

                attributeValues.Add( new GroupAttributeValueCsv
                {
                    GroupId = fundraisingGroupCsv.Id,
                    AttributeKey = "OpportunityType",
                    AttributeValue = tripDefinedValue.Value,
                    AttributeValueId = "OpportunityType_" + fundraisingGroupCsv.Id
                } );

                var financialAccount = this.ImportedAccounts.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{fundraisingGroupCsv.AccountId}" );
                if ( financialAccount != null )
                {
                    attributeValues.Add( new GroupAttributeValueCsv
                    {
                        GroupId = fundraisingGroupCsv.Id,
                        AttributeKey = "FinancialAccount",
                        AttributeValue = financialAccount.Guid.ToString(),
                        AttributeValueId = "FinancialAccount_" + fundraisingGroupCsv.Id
                    } );
                }

                if ( fundraisingGroupCsv.IndividualFundraisingGoal.HasValue && fundraisingGroupCsv.IndividualFundraisingGoal > 0 )
                {
                    attributeValues.Add( new GroupAttributeValueCsv
                    {
                        GroupId = fundraisingGroupCsv.Id,
                        AttributeKey = "IndividualFundraisingGoal",
                        AttributeValue = fundraisingGroupCsv.IndividualFundraisingGoal.Value.ToString( "0.00"),
                        AttributeValueId = "IndividualFundraisingGoal_" + fundraisingGroupCsv.Id
                    } );
                }

                attributeValues.Add( new GroupAttributeValueCsv
                {
                    GroupId = fundraisingGroupCsv.Id,
                    AttributeKey = "ShowPublic",
                    AttributeValue = fundraisingGroupCsv.IsPublic.HasValue ? fundraisingGroupCsv.IsPublic.Value.ToString() : bool.FalseString,
                    AttributeValueId = "ShowPublic_" + fundraisingGroupCsv.Id
                } );
            }

            completedGroups += ImportGroups( groupCsvs, "FundraisingGroup" );

            ImportGroupAttributeValues( attributeValues );

            return completedGroups;
        }

        public void BulkUpdateParentGroup( RockContext rockContext, List<GroupImport> groupImports, Dictionary<string, Group> groupLookup )
        {
            var groupsUpdated = false;
            var parentGroupErrors = string.Empty;

            foreach ( var groupImport in groupImports )
            {
                var group = groupLookup.GetValueOrNull( groupImport.GroupForeignKey );

                if ( group != null )
                {
                    int? parentGroupId = groupLookup.GetValueOrNull( groupImport.ParentGroupForeignKey )?.Id;
                    if ( parentGroupId.HasValue && group.ParentGroupId != parentGroupId )
                    {
                        group.ParentGroupId = parentGroupId;
                        groupsUpdated = true;
                    }
                    else if ( group.ParentGroupId == parentGroupId )
                    {
                        // The group's ParentGroupId is already set correctly, so ignore this.
                    }
                    else
                    {
                        parentGroupErrors += $"{DateTime.Now}, GroupImport, Invalid ParentGroup {groupImport.ParentGroupForeignId} for Group {groupImport.Name}:{groupImport.GroupForeignId}.\r\n";
                    }
                }
                else
                {
                    parentGroupErrors += $"{DateTime.Now}, GroupImport, Invalid ParentGroup {groupImport.ParentGroupForeignId} for Group {groupImport.Name}:{groupImport.GroupForeignId}.\r\n";
                }
            }

            if ( groupsUpdated )
            {
                rockContext.SaveChanges( true );
            }

            if ( parentGroupErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, parentGroupErrors, hasMultipleErrors: true );
            }
        }

        public void BulkInsertGroupSchedules( List<Group> insertedGroups, Dictionary<string, Group> groupLookup )
        {
            var rockContext = new RockContext();

            var groupSchedulesToInsert = new List<Schedule>();
            foreach ( var groupWithSchedule in insertedGroups.Where( v => v.Schedule != null && v.Schedule.Id == 0 ).ToList() )
            {
                var groupId = groupLookup.GetValueOrNull( groupWithSchedule.ForeignKey )?.Id;
                if ( groupId.HasValue )
                {
                    groupSchedulesToInsert.Add( groupWithSchedule.Schedule );
                }
            }

            rockContext.BulkInsert( groupSchedulesToInsert );

            if ( groupSchedulesToInsert.Any() )
            {
                // manually update Group.ScheduleId since BulkInsert doesn't
                rockContext.Database.ExecuteSqlCommand( string.Format( @"
UPDATE [Group]
SET ScheduleId = [Schedule].[Id]
FROM [Group]
JOIN [Schedule]
ON [Group].[ForeignKey] = [Schedule].[ForeignKey]
AND [Group].[Name] = [Schedule].[Name]
AND [Group].[ForeignKey] LIKE '{0}^%'
AND [Schedule].[ForeignKey] LIKE '{0}^%'
                ", ImportInstanceFKPrefix ) );
            }
        }

        public void BulkInsertGroupLocations( List<Group> insertedGroups, Dictionary<string, Group> groupLookup )
        {
            var rockContext = new RockContext();
            var groupTypesToUpdate = new List<int>();
            var groupLocationLookup = new GroupLocationService( rockContext).Queryable()
                                                            .AsNoTracking()
                                                            .Where( l => !string.IsNullOrEmpty( l.ForeignKey ) && l.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                                            .Select( a => new
                                                            {
                                                                GroupLocation = a,
                                                                a.ForeignKey
                                                            } )
                                                            .ToDictionary( k => k.ForeignKey, v => v.GroupLocation );

            var groupLocationsToInsert = new List<GroupLocation>();
            foreach ( var groupWithLocation in insertedGroups.Where( g => g.GroupLocations.Count > 0 && g.GroupLocations.Any( gl => gl.Id == 0 ) ).ToList() )
            {
                var group = groupLookup.GetValueOrNull( groupWithLocation.ForeignKey );
                var groupId = group?.Id;
                if ( groupId.HasValue )
                {
                    var groupLocationList = groupWithLocation.GroupLocations.Where( gl => gl.Id == 0 ).ToList();
                    foreach ( var groupLocation in groupLocationList )
                    {
                        groupLocation.GroupId = groupId.Value;
                        groupLocation.ForeignKey += $"_{groupId.Value}";
                        if ( !groupLocationLookup.ContainsKey( groupLocation.ForeignKey ) )
                        {
                            groupLocationsToInsert.AddRange( groupLocationList );
                        }
                    }

                    var locationMode = GroupLocationPickerMode.Named;
                    var groupType = this.GroupTypeDict.Values.FirstOrDefault( gt => gt.Id == group.GroupTypeId );
                    if ( ( groupType.LocationSelectionMode & locationMode ) != locationMode )
                    {
                        groupTypesToUpdate.Add( groupType.Id );
                    }
                }
            }

            rockContext.BulkInsert( groupLocationsToInsert );

            if ( groupTypesToUpdate.Count > 0 )
            {
                groupTypesToUpdate = groupTypesToUpdate.Distinct().ToList();
                var groupTypes = new GroupTypeService( rockContext ).Queryable().Where( gt => groupTypesToUpdate.Contains( gt.Id ) );
                foreach ( var groupType in groupTypes )
                {
                    var locationSelectionMode = groupType.LocationSelectionMode | GroupLocationPickerMode.Named;
                    groupType.LocationSelectionMode = locationSelectionMode;
                }
                rockContext.SaveChanges();
                LoadGroupTypeDict();
            }
        }

        /// <summary>
        /// Processes the GroupAddress list.
        /// </summary>
        private int ImportGroupAddresses()
        {
            this.ReportProgress( 0, "Preparing Group Address data for import..." );
            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }
            if ( this.GroupTypeDict == null )
            {
                LoadGroupTypeDict();
            }
            if ( this.GroupLocationTypeDVDict == null )
            {
                this.GroupLocationTypeDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE.AsGuid() );
            }

            var groupAddressImports = new List<GroupAddressImport>();
            var groupTypesToUpdate = new List<int>();
            var groupAddressErrors = string.Empty;
            var rockContext = new RockContext();

            foreach ( var groupAddressCsv in GroupAddressCsvList )
            {
                if ( string.IsNullOrEmpty( groupAddressCsv.Street1 ) )
                {
                    groupAddressErrors += $"{DateTime.Now}, GroupAddress, Blank Street Address for GroupId {groupAddressCsv.GroupId}, Address Type {groupAddressCsv.AddressTypeEnum}. Group Address was skipped.\r\n";
                    continue;
                }
                var group = this.GroupDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupAddressCsv.GroupId ) );
                if ( group == null )
                {
                    groupAddressErrors += $"{DateTime.Now}, GroupAddress, GroupId {groupAddressCsv.GroupId} not found. Group Address was skipped.\r\n";
                    continue;
                }

                var groupLocationTypeValueId = GetGroupLocationTypeDVId( groupAddressCsv.AddressTypeEnum.Value );

                if ( groupAddressCsv.IsValidAddressType && groupLocationTypeValueId.HasValue )
                {
                    // Ensure group type has Address set for LocationSelectionMode, otherwise addresses will not show in Group Viewer.
                    // We will collect their grouptype id here, then process them all at once at the end.
                    var groupType = this.GroupTypeDict.Values.FirstOrDefault( gt => gt.Id == group.GroupTypeId );
                    var addressMode = GroupLocationPickerMode.Address;
                    if ( ( groupType.LocationSelectionMode & addressMode ) != addressMode )
                    {
                        groupTypesToUpdate.Add( groupType.Id );
                    }
                    var newGroupAddress = new GroupAddressImport()
                    {
                        GroupId = group.Id,
                        GroupLocationTypeValueId = groupLocationTypeValueId.Value,
                        IsMailingLocation = groupAddressCsv.IsMailing,
                        IsMappedLocation = groupAddressCsv.AddressTypeEnum == AddressType.Home,
                        Street1 = groupAddressCsv.Street1.Left( 100 ),
                        Street2 = groupAddressCsv.Street2.Left( 100 ),
                        City = groupAddressCsv.City.Left( 50 ),
                        State = groupAddressCsv.State.Left( 50 ),
                        Country = groupAddressCsv.Country.Left( 50 ),
                        PostalCode = groupAddressCsv.PostalCode.Left( 50 ),
                        Latitude = groupAddressCsv.Latitude.AsDoubleOrNull(),
                        Longitude = groupAddressCsv.Longitude.AsDoubleOrNull(),
                        AddressForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupAddressCsv.AddressId.IsNotNullOrWhiteSpace() ? groupAddressCsv.AddressId : string.Format( "{0}_{1}", groupAddressCsv.GroupId, groupAddressCsv.AddressTypeEnum.ToString() ) )
                    };

                    groupAddressImports.Add( newGroupAddress );
                }
                else
                {
                    groupAddressErrors += $"{DateTime.Now}, GroupAddress, Unexpected Address Type ({groupAddressCsv.AddressType}) encountered for Group \"{groupAddressCsv.GroupId}\". Group Address was skipped.\r\n";
                }
            }

            var groupLocationService = new GroupLocationService( rockContext );
            var groupLocationLookup = groupLocationService.Queryable()
                                                            .AsNoTracking()
                                                            .Where( l => !string.IsNullOrEmpty( l.ForeignKey ) && l.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                                            .Select( a => new
                                                            {
                                                                GroupLocation = a,
                                                                a.ForeignKey
                                                            } )
                                                            .ToDictionary( k => k.ForeignKey, v => v.GroupLocation );
            var groupLocationsToInsert = new List<GroupLocation>();
            this.ReportProgress( 0, string.Format( "Begin processing {0} Group Address Records...", groupAddressImports.Count ) );

            // Slice data into chunks and process
            var groupAddressesRemainingToProcess = groupAddressImports.Count;
            var workingGroupAddressImportList = groupAddressImports.ToList();
            var completedGroupAddresses = 0;

            while ( groupAddressesRemainingToProcess > 0 )
            {
                if ( completedGroupAddresses > 0 && completedGroupAddresses % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroupAddresses} GroupAddress - Locations processed." );
                }

                if ( completedGroupAddresses % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupAddressImportList.Take( Math.Min( this.DefaultChunkSize, workingGroupAddressImportList.Count ) ).ToList();
                    var imported = BulkGroupAddressImport( rockContext, csvChunk, groupLocationLookup, groupLocationsToInsert );
                    completedGroupAddresses += imported;
                    groupAddressesRemainingToProcess -= csvChunk.Count;
                    workingGroupAddressImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            if ( groupAddressErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, groupAddressErrors, hasMultipleErrors: true );
            }

            // Slice data into chunks and process
            var groupLocationRemainingToProcess = groupLocationsToInsert.Count;
            var workingGroupLocationList = groupLocationsToInsert.ToList();
            var completedGroupLocations = 0;
            var locationDict = new LocationService( rockContext ).Queryable().Select( a => new { a.Id, a.Guid } ).ToList().ToDictionary( k => k.Guid, v => v.Id );

            while ( groupLocationRemainingToProcess > 0 )
            {
                if ( completedGroupLocations > 0 && completedGroupLocations % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroupLocations} GroupAddress - GroupLocations processed." );
                }

                if ( completedGroupLocations % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupLocationList.Take( Math.Min( this.DefaultChunkSize, workingGroupLocationList.Count ) ).ToList();
                    var imported = BulkInsertGroupLocation( rockContext, csvChunk, locationDict );
                    completedGroupLocations += imported;
                    groupLocationRemainingToProcess -= csvChunk.Count;
                    workingGroupLocationList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            if ( groupTypesToUpdate.Count > 0 )
            {
                groupTypesToUpdate = groupTypesToUpdate.Distinct().ToList();
                var groupTypes = new GroupTypeService( rockContext ).Queryable().Where( gt => groupTypesToUpdate.Contains( gt.Id ) );
                foreach ( var groupType in groupTypes )
                {
                    var locationSelectionMode = groupType.LocationSelectionMode | GroupLocationPickerMode.Address;
                    groupType.LocationSelectionMode = locationSelectionMode;
                }
                rockContext.SaveChanges();
                LoadGroupTypeDict();
            }

            return completedGroupAddresses;
        }

        public int BulkGroupAddressImport( RockContext rockContext, List<GroupAddressImport> groupAddressImports, Dictionary<string, GroupLocation> groupLocationLookup, List<GroupLocation> groupLocationsToInsert )
        {
            var locationService = new LocationService( rockContext );

            var importedDateTime = RockDateTime.Now;

            var locationsToInsert = new List<Location>();

            // get the distinct addresses for each group in our import
            var groupAddresses = groupAddressImports.Where( a => a.GroupId.HasValue && a.GroupId.Value > 0 && !groupLocationLookup.ContainsKey( a.AddressForeignKey ) ).DistinctBy( a => new { a.GroupLocationTypeValueId, a.Street1, a.Street2, a.City, a.County, a.State } ).ToList();

            foreach ( var address in groupAddresses )
            {
                var newLocation = new Location
                {
                    Street1 = address.Street1.Left( 100 ),
                    Street2 = address.Street2.Left( 100 ),
                    City = address.City.Left( 50 ),
                    County = address.County.Left( 50 ),
                    State = address.State.Left( 50 ),
                    Country = address.Country.Left( 50 ),
                    PostalCode = address.PostalCode.Left( 50 ),
                    CreatedDateTime = importedDateTime,
                    ModifiedDateTime = importedDateTime,
                    ForeignKey = address.AddressForeignKey,
                    Guid = Guid.NewGuid() // give the Location a Guid, and store a reference to which Location is associated with the GroupLocation record. Then we'll match them up later and do the bulk insert
                };

                if ( address.Latitude.HasValue && address.Longitude.HasValue )
                {
                    newLocation.SetLocationPointFromLatLong( address.Latitude.Value, address.Longitude.Value );
                }

                var groupLocation = new GroupLocation
                {
                    GroupLocationTypeValueId = address.GroupLocationTypeValueId,
                    GroupId = address.GroupId.Value,
                    IsMailingLocation = address.IsMailingLocation,
                    IsMappedLocation = address.IsMappedLocation,
                    CreatedDateTime = importedDateTime,
                    ModifiedDateTime = importedDateTime,
                    Location = newLocation,
                    ForeignKey = address.AddressForeignKey
                };

                groupLocationsToInsert.Add( groupLocation );
                locationsToInsert.Add( groupLocation.Location );
            }

            rockContext.BulkInsert( locationsToInsert );

            return groupAddressImports.Count;
        }

        public int BulkInsertGroupLocation( RockContext rockContext, List<GroupLocation> groupLocationsToInsert, Dictionary<Guid, int> locationIdLookup )
        {
            foreach ( var groupLocation in groupLocationsToInsert )
            {
                groupLocation.LocationId = locationIdLookup[groupLocation.Location.Guid];
            }

            rockContext.BulkInsert( groupLocationsToInsert );
            return groupLocationsToInsert.Count;
        }

        private int? GetGroupLocationTypeDVId( AddressType addressType )
        {
            switch ( addressType )
            {
                case AddressType.Home:
                    return this.GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_HOME.AsGuid()].Id;

                case AddressType.Previous:
                    return this.GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_PREVIOUS.AsGuid()].Id;

                case AddressType.Work:
                    return this.GroupLocationTypeDVDict[Rock.SystemGuid.DefinedValue.GROUP_LOCATION_TYPE_WORK.AsGuid()].Id;

                default:
                    return this.GroupLocationTypeDVDict.Values.FirstOrDefault( d => !string.IsNullOrEmpty( d.ForeignKey ) && d.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) && d.Value == addressType.ToString() )?.Id;
            }
        }

        /// <summary>
        /// Processes the Group AttributeValue list.
        /// </summary>
        private int ImportGroupAttributeValues( List<GroupAttributeValueCsv> groupAttributeValues = null )
        {
            this.ReportProgress( 0, "Preparing Group Attribute Value data for import..." );
            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }
            if ( this.GroupAttributeDict == null )
            {
                LoadGroupAttributeDict();
            }

            if ( groupAttributeValues == null )
            {
                groupAttributeValues = this.GroupAttributeValueCsvList;
            }

            var rockContext = new RockContext();
            var groupAVImports = new List<AttributeValueImport>();
            var groupAVErrors = string.Empty;

            var groupAttributeValuesCount = groupAttributeValues.Count;

            groupAttributeValues = groupAttributeValues.Where( gv => gv.AttributeValue.IsNotNullOrWhiteSpace() ).DistinctBy( av => new { av.AttributeKey, av.GroupId } ).OrderBy( av => av.AttributeKey ).ToList();  // Protect against duplicates in import data

            if ( groupAttributeValues.Count <  groupAttributeValuesCount )
            {
                LogException( $"GroupAttributValue", $"{groupAttributeValuesCount - groupAttributeValues.Count} duplicate and/or empty AttributeValues were found and will be skipped." );
            }

            var attributeDefinedValuesDict = GetAttributeDefinedValuesDictionary( rockContext, GroupEntityTypeId );
            var attributeValueLookup = GetAttributeValueLookup( rockContext, GroupEntityTypeId );

            foreach ( var attributeValueCsv in groupAttributeValues )
            {
                var group = this.GroupDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.GroupId ) );
                if ( group == null )
                {
                    groupAVErrors += $"{DateTime.Now}, GroupAttributeValue, GroupId {attributeValueCsv.GroupId} not found. Group AttributeValue for {attributeValueCsv.AttributeKey} attribute was skipped.\r\n";
                    continue;
                }

                var attribute = this.GroupAttributeDict.GetValueOrNull( $"{attributeValueCsv.AttributeKey}_{group.GroupTypeId}" );
                
                if ( attribute == null )
                {
                    groupAVErrors += $"{DateTime.Now}, GroupAttributeValue, AttributeKey {attributeValueCsv.AttributeKey} not found. AttributeValue for GroupId {attributeValueCsv.GroupId} was skipped.\r\n";
                    continue;
                }

                if ( attributeValueLookup.Any( l => l.Item1 == attribute.Id && l.Item2 == group.Id ) )
                {
                    groupAVErrors += $"{DateTime.Now}, GroupAttributeValue, AttributeValue for AttributeKey {attributeValueCsv.AttributeKey} and GroupId {attributeValueCsv.GroupId} already exists. AttributeValueId {attributeValueCsv.AttributeValueId} was skipped.\r\n";
                    continue;
                }

                var newAttributeValue = new AttributeValueImport()
                {
                    AttributeId = attribute.Id,
                    AttributeValueForeignId = attributeValueCsv.AttributeValueId.AsIntegerOrNull(),
                    EntityId = group.Id,
                    AttributeValueForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.AttributeValueId.IsNotNullOrWhiteSpace() ? attributeValueCsv.AttributeValueId : string.Format( "{0}_{1}", attributeValueCsv.GroupId, attributeValueCsv.AttributeKey ) )
                };

                newAttributeValue.Value = GetAttributeValueStringByAttributeType( rockContext, attributeValueCsv.AttributeValue, attribute, attributeDefinedValuesDict );
                
                groupAVImports.Add( newAttributeValue );
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Group Attribute Value Records...", groupAVImports.Count ) );
            if ( groupAVErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, groupAVErrors, hasMultipleErrors: true );
            }
            return ImportAttributeValues( groupAVImports );
        }

        /// <summary>
        /// Processes the Group Member list.
        /// </summary>
        private int ImportGroupMembers()
        {
            this.ReportProgress( 0, "Preparing Group Member data for import..." );

            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }

            if ( this.GroupMemberDict == null )
            {
                LoadGroupMemberDict();
            }

            if ( this.GroupTypeDict == null )
            {
                LoadGroupTypeDict();
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Group Member Records...", this.GroupMemberCsvList.Count ) );

            var rockContext = new RockContext();
            var groupMemberLookup = this.GroupMemberDict.ToDictionary( k => k.Key, v => v.Value );

            // Slice data into chunks and process
            var groupMembersRemainingToProcess = this.GroupMemberCsvList.Count;
            var workingGroupMemberCsvList = this.GroupMemberCsvList.ToList();
            var completedGroupMembers = 0;

            while ( groupMembersRemainingToProcess > 0 )
            {
                if ( completedGroupMembers > 0 && completedGroupMembers % ( this.PersonChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroupMembers} Group Members processed." );
                }

                if ( completedGroupMembers % this.PersonChunkSize < 1 )
                {
                    var csvChunk = workingGroupMemberCsvList.Take( Math.Min( this.PersonChunkSize, workingGroupMemberCsvList.Count ) ).ToList();
                    var imported = BulkGroupMemberImport( rockContext, csvChunk, groupMemberLookup );
                    completedGroupMembers += imported;
                    groupMembersRemainingToProcess -= csvChunk.Count;
                    workingGroupMemberCsvList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            // Reload Group Member Dictionary to include all newly imported groupmembers
            LoadGroupMemberDict();

            return completedGroupMembers;
        }

        public int BulkGroupMemberImport( RockContext rockContext, List<GroupMemberCsv> groupMemberCsvs, Dictionary<string, GroupMember> groupMemberLookup )
        {
            var groupMemberImports = new List<GroupMemberImport>();
            var groupMemberErrors = string.Empty;

            foreach ( var groupMemberCsv in groupMemberCsvs )
            {
                var group = this.GroupDict.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberCsv.GroupId ) );
                if ( group == null )
                {
                    groupMemberErrors += $"{DateTime.Now}, GroupMember, GroupId {groupMemberCsv.GroupId} not found. Group Member record for {groupMemberCsv.PersonId} was skipped.\r\n";
                    continue;
                }

                var person = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberCsv.PersonId ) );
                if ( person == null )
                {
                    groupMemberErrors += $"{DateTime.Now}, GroupMember, PersonId {groupMemberCsv.PersonId} not found. Group Member for GroupId {groupMemberCsv.GroupId} was skipped.\r\n";
                    continue;
                }
                if ( !groupMemberCsv.IsValidGroupMemberStatus )
                {
                    groupMemberErrors += $"{DateTime.Now}, GroupMember, Unexpected GroupMemberStatus ({groupMemberCsv.GroupMemberStatus}) encountered for PersonId \"{groupMemberCsv.PersonId}\" in GroupId \"{groupMemberCsv.GroupId}\". Group Member Status was defaulted to {groupMemberCsv.GroupMemberStatusEnum}.\r\n";
                    continue;
                }

                TextInfo textInfo = new CultureInfo( "en-US", false ).TextInfo;

                var newGroupMember = new GroupMemberImport()
                {
                    PersonId = person.Id,
                    GroupId = group.Id,
                    GroupTypeId = group.GroupTypeId,
                    RoleName = textInfo.ToTitleCase( groupMemberCsv.Role.Left( 100 ) ),
                    GroupMemberStatus = groupMemberCsv.GroupMemberStatusEnum.Value,
                    GroupMemberForeignKey = groupMemberCsv.GroupMemberId.IsNotNullOrWhiteSpace() ? string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberCsv.GroupMemberId ) : string.Format( "{0}^{1}_{2}", this.ImportInstanceFKPrefix, groupMemberCsv.GroupId, groupMemberCsv.PersonId ),
                    Note = groupMemberCsv.Note,
                    DateTimeAdded = groupMemberCsv.DateTimeAdded
                };

                if ( groupMemberCsv.CreatedDate.HasValue )
                {
                    newGroupMember.CreatedDate = groupMemberCsv.CreatedDate.Value;
                }

                groupMemberImports.Add( newGroupMember );
            }

            // Add GroupType Roles if needed
            BulkInsertGroupTypeRoles( rockContext, groupMemberImports, groupMemberLookup );

            var importedDateTime = RockDateTime.Now;
            var groupMembersToInsert = new List<GroupMember>();

            var groupMembers = groupMemberImports.Where( v => !groupMemberLookup.ContainsKey( v.GroupMemberForeignKey ) ).ToList();

            var groupMemberImportByGroupType = groupMembers.GroupBy( a => a.GroupTypeId.Value )
                                                .Select( a => new
                                                {
                                                    GroupTypeId = a.Key,
                                                    GroupMembers = a.Select( x => x ).ToList()
                                                } );
            var groupTypeLookup = new GroupTypeService( rockContext ).Queryable().Include( "Roles" ).ToDictionary( k => k.Id, v => v );
            foreach ( var groupMemberImportObj in groupMemberImportByGroupType )
            {
                var groupType = groupTypeLookup[groupMemberImportObj.GroupTypeId];
                var groupTypeRoleLookup = groupType.Roles.ToDictionary( k => k.Name, v => v.Id, StringComparer.OrdinalIgnoreCase );

                foreach ( var groupMemberImport in groupMemberImportObj.GroupMembers )
                {
                    var groupRoleId = groupTypeRoleLookup.GetValueOrNull( groupMemberImport.RoleName );
                    if ( !groupRoleId.HasValue || groupRoleId.Value <= 0 )
                    {
                        groupMemberErrors += $"{DateTime.Now}, GroupMember, Group Role {groupMemberImport.RoleName} not found in Group Type. Group Member for Rock GroupId {groupMemberImport.GroupId}, Rock PersonId {groupMemberImport.PersonId} was set to default group type role.\"Member\".\r\n";
                        groupRoleId = groupType.DefaultGroupRoleId;
                    }
                    groupMembers.FirstOrDefault( gm => gm.GroupMemberForeignKey == groupMemberImport.GroupMemberForeignKey ).RoleId = groupRoleId;
                }

            }
            foreach ( var groupMember in groupMembers )
            {
                var newGroupMember = new GroupMember
                {
                    GroupId = groupMember.GroupId.Value,
                    GroupRoleId = groupMember.RoleId.Value,
                    PersonId = groupMember.PersonId.Value,
                    CreatedDateTime = groupMember.CreatedDate.HasValue ? groupMember.CreatedDate.Value : importedDateTime,
                    ModifiedDateTime = importedDateTime,
                    Note = groupMember.Note,
                    ForeignKey = groupMember.GroupMemberForeignKey,
                    GroupMemberStatus = groupMember.GroupMemberStatus,
                    DateTimeAdded = groupMember.DateTimeAdded
                };
                if ( groupMember.GroupTypeId.HasValue )
                {
                    newGroupMember.GroupTypeId = groupMember.GroupTypeId.Value;
                }
                groupMembersToInsert.Add( newGroupMember );
            }

            rockContext.BulkInsert( groupMembersToInsert );

            if ( groupMemberErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, groupMemberErrors, hasMultipleErrors: true );
            }

            return groupMemberCsvs.Count;
        }

        public void BulkInsertGroupTypeRoles( RockContext rockContext, List<GroupMemberImport> groupMemberImports, Dictionary<string, GroupMember> groupMemberLookup )
        {
            var importedDateTime = RockDateTime.Now;
            var groupMemberErrors = string.Empty;
            var groupMembers = groupMemberImports.Where( v => !groupMemberLookup.ContainsKey( v.GroupMemberForeignKey ) ).ToList();
            var importedGroupTypeRoleNames = groupMembers.GroupBy( a => a.GroupTypeId.Value ).Select( a => new
            {
                GroupTypeId = a.Key,
                RoleNames = a.Select( x => x.RoleName ).Distinct().Where( r => !string.IsNullOrWhiteSpace( r ) ).ToList()
            } );

            // Create any missing roles on the GroupType
            var groupTypeRolesToInsert = new List<GroupTypeRole>();

            foreach ( var importedGroupTypeRoleName in importedGroupTypeRoleNames )
            {
                var groupTypeCache = GroupTypeCache.Get( importedGroupTypeRoleName.GroupTypeId, rockContext );
                foreach ( var roleName in importedGroupTypeRoleName.RoleNames )
                {
                    if ( !groupTypeCache.Roles.Any( a => a.Name.Equals( roleName, StringComparison.OrdinalIgnoreCase ) ) )
                    {
                        var newGroupTypeRole = new GroupTypeRole
                        {
                            GroupTypeId = groupTypeCache.Id,
                            Name = roleName,
                            CreatedDateTime = importedDateTime,
                            ModifiedDateTime = importedDateTime,
                            ForeignKey = $"{this.ImportInstanceFKPrefix}^{groupTypeCache.Id}_{roleName.Left(50)}"
                        };

                        groupTypeRolesToInsert.Add( newGroupTypeRole );
                    }
                }
            }

            if ( groupTypeRolesToInsert.Any() )
            {
                rockContext.BulkInsert( groupTypeRolesToInsert );
                GroupTypeCache.Clear();
            }
        }
    }
}
