// <copyright>
// Copyright 2022 by Kingdom First Solutions
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
using System;
using System.Collections.Generic;
using System.Linq;
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.F1
{
    /// <summary>
    /// Partial of F1Component that imports Ministries
    /// </summary>
    public partial class F1Component
    {
        /// <summary>
        /// Maps the volunteer assignment data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapActivityAssignment( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var excludedGroupTypes = new List<int> { FamilyGroupTypeId, SmallGroupTypeId, GeneralGroupTypeId };
            var newGroupMembers = new List<GroupMember>();
            var assignmentTerm = "Member";

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying participant assignment import ({0:N0} found).", totalRows ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                // get the group and role data
                var ministryId = row["Ministry_ID"] as int?;
                var activityId = row["Activity_ID"] as int?;
                var rlcId = row["RLC_ID"] as int?;
                var individualId = row["Individual_ID"] as int?;
                var assignmentDate = row["AssignmentDateTime"] as DateTime?;
                var membershipStart = row["Activity_Start_Time"] as DateTime?;
                var membershipStop = row["Activity_End_Time"] as DateTime?;
                var activityTimeName = row["Activity_Time_Name"] as string;

                var groupLookupId = rlcId ?? activityId ?? ministryId;
                var assignmentGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( groupLookupId.ToString() ) && !excludedGroupTypes.Contains( g.GroupTypeId ) );
                if ( assignmentGroup != null )
                {
                    var personKeys = GetPersonKeys( individualId, null );
                    if ( personKeys != null )
                    {
                        var isActive = membershipStop.HasValue ? membershipStop > RockDateTime.Now : true;
                        var groupTypeRole = GetGroupTypeRole( lookupContext, assignmentGroup.GroupTypeId, assignmentTerm, string.Format( "{0} imported {1}", activityTimeName, ImportDateTime ), false, 0, true, null, string.Format( "{0} {1}", activityTimeName, assignmentTerm ), ImportPersonAliasId );

                        newGroupMembers.Add( new GroupMember
                        {
                            IsSystem = false,
                            DateTimeAdded = membershipStart,
                            GroupId = assignmentGroup.Id,
                            PersonId = personKeys.PersonId,
                            GroupRoleId = groupTypeRole.Id,
                            CreatedDateTime = assignmentDate,
                            ModifiedDateTime = membershipStop,
                            GroupMemberStatus = isActive != false ? GroupMemberStatus.Active : GroupMemberStatus.Inactive,
                            ForeignKey = string.Format( "Membership imported {0}", ImportDateTime )
                        } );

                        completedItems++;
                    }
                }

                if ( completedItems % percentage < 1 )
                {
                    var percentComplete = completedItems / percentage;
                    ReportProgress( percentComplete, string.Format( "{0:N0} assignments imported ({1}% complete).", completedItems, percentComplete ) );
                }

                if ( completedItems % DefaultChunkSize < 1 )
                {
                    SaveGroupMembers( newGroupMembers );
                    ReportPartialProgress();

                    // Reset lists and context
                    lookupContext = new RockContext();
                    newGroupMembers.Clear();
                }
            }

            if ( newGroupMembers.Any() )
            {
                SaveGroupMembers( newGroupMembers );
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished participant assignment import: {0:N0} assignments imported.", completedItems ) );
        }

        /// <summary>
        /// Maps the activity ministry data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapActivityMinistry( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var newGroups = new List<Group>();
            var newCheckinGroupForeignKeys = new List<string>();

            const string attendanceTypeName = "Attendance History";
            var groupTypeHistory = ImportedGroupTypes.FirstOrDefault( t => t.ForeignKey.Equals( attendanceTypeName ) );
            if ( groupTypeHistory == null )
            {
                groupTypeHistory = AddGroupType( lookupContext, attendanceTypeName, string.Format( "{0} imported {1}", attendanceTypeName, ImportDateTime ), null,
                    null, GroupTypeCheckinTemplateId, true, true, true, true, typeForeignKey: attendanceTypeName );
                ImportedGroupTypes.Add( groupTypeHistory );
            }

            const string groupsParentName = "Archived Groups";
            var archivedGroupsParent = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( groupsParentName.RemoveWhitespace() ) );
            if ( archivedGroupsParent == null )
            {
                archivedGroupsParent = AddGroup( lookupContext, GeneralGroupTypeId, null, groupsParentName, true, null, ImportDateTime, groupsParentName.RemoveWhitespace(), true, ImportPersonAliasId );
                ImportedGroups.Add( archivedGroupsParent );
            }

            const string servingGroupsParentName = "Archived Serving Groups";
            var archivedServingGroupsParent = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( servingGroupsParentName.RemoveWhitespace() ) );
            if ( archivedServingGroupsParent == null )
            {
                archivedServingGroupsParent = AddGroup( lookupContext, ServingTeamGroupType.Id, ServingTeamsParentGroup.Id, servingGroupsParentName, true, null, ImportDateTime, servingGroupsParentName.RemoveWhitespace(), true, ImportPersonAliasId );
                ImportedGroups.Add( archivedServingGroupsParent );
            }

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying ministry import ({0:N0} found, {1:N0} already exist).", totalRows, ImportedGroupTypes.Count ) );

            foreach ( var row in tableData.OrderBy( r => r["Ministry_ID"] as int? ).ThenBy( r => r["Activity_ID"] as int? ) )
            {
                // get the ministry data
                var ministryId = row["Ministry_ID"] as int?;
                var activityId = row["Activity_ID"] as int?;
                var ministryName = row["Ministry_Name"] as string;
                var activityName = row["Activity_Name"] as string;
                var ministryActive = row["Ministry_Active"] as string;
                var activityActive = row["Activity_Active"] as string;
                var hasCheckin = row["Has_Checkin"] as bool?;
                int? campusId = null;
                var isServingMinistry = !string.IsNullOrWhiteSpace( ministryName ) && ministryName.RemoveSpaces().StartsWith( "SERV:", StringComparison.OrdinalIgnoreCase );
                var isServingActivity = !string.IsNullOrWhiteSpace( activityName ) && activityName.RemoveSpaces().StartsWith( "SERV:", StringComparison.OrdinalIgnoreCase );
                var activityHasPrefix = isServingActivity;

                if ( ministryId.HasValue && !string.IsNullOrWhiteSpace( ministryName ) && !ministryName.Equals( "Delete", StringComparison.OrdinalIgnoreCase ) )
                {
                    var ministryServingGroupFK = "SERV_" + ministryId.Value.ToString();
                    var activityServingGroupFK = "SERV_" + activityId.Value.ToString();
                    var ministryServingGroupCascadeFK = "SERVT_" + ministryId.Value.ToString();
                    var activityServingGroupCascadeFK = "SERVT_" + activityId.Value.ToString();

                    if ( isServingMinistry )
                    {
                        // strip SERV: prefix off
                        ministryName = StripPrefix( ministryName, null );
                    }
                    // check for a ministry group campus context
                    campusId = campusId ?? GetCampusId( ministryName );
                    if ( ministryName.Any( n => ValidDelimiters.Contains( n ) ) )
                    {
                        if ( campusId.HasValue )
                        {
                            // strip the campus from the ministry name to use for grouptype (use the original name on groups though)
                            ministryName = StripPrefix( ministryName, campusId );
                        }
                    }

                    // add the new grouptype if it doesn't exist
                    int? currentGroupTypeId = null;
                    if ( isServingActivity || isServingMinistry )
                    {
                        currentGroupTypeId = ServingTeamGroupType.Id;
                    }
                    else
                    {
                        currentGroupTypeId = ImportedGroupTypes.Where( t => t.ForeignKey.Equals( ministryName ) ).Select( t => t.Id ).FirstOrDefault();
                    }
                    if ( !currentGroupTypeId.HasValue || currentGroupTypeId.Value == 0 )
                    {
                        // save immediately so we can use the grouptype for a group
                        {
                            var currentGroupType = AddGroupType( lookupContext, ministryName.Trim(), string.Format( "{0} imported {1}", ministryName.Trim(), ImportDateTime ), groupTypeHistory.Id,
                                null, null, true, true, true, true, typeForeignKey: ministryName.Trim() );
                            ImportedGroupTypes.Add( currentGroupType );
                            currentGroupTypeId = currentGroupType.Id;
                        }
                    }

                    // create a campus level parent for the ministry group

                    var parentGroup = isServingMinistry || isServingActivity ? archivedServingGroupsParent : archivedGroupsParent;
                    var parentGroupId = parentGroup.Id;
                    if ( campusId.HasValue )
                    {
                        var campus = CampusList.FirstOrDefault( c => c.Id == campusId );
                        var campusGroupFK = isServingMinistry || isServingActivity ? "SERV_" + campus.ShortCode : campus.ShortCode;
                        var campusGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( campusGroupFK ) && g.ParentGroupId == parentGroupId );
                        if ( campusGroup == null )
                        {
                            campusGroup = AddGroup( lookupContext, parentGroup.GroupTypeId, parentGroupId, campus.Name, true, campus.Id, ImportDateTime, campusGroupFK, true, ImportPersonAliasId );
                            ImportedGroups.Add( campusGroup );
                        }

                        parentGroup = campusGroup;
                        parentGroupId = campusGroup.Id;
                    }

                    // add a ministry group level if it doesn't exist
                    var ministryGroupFKString = ministryId.ToString();
                    var ministryGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( ministryServingGroupCascadeFK ) );
                    if ( ministryGroup != null )
                    {
                        ministryGroupFKString = "SERVT_" + ministryGroupFKString;
                        isServingMinistry = true;
                        isServingActivity = true;
                    }
                    else if ( isServingMinistry || isServingActivity )
                    {
                        ministryGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( ministryServingGroupFK ) );
                        ministryGroupFKString = "SERV_" + ministryGroupFKString;
                        isServingActivity = true;
                    }
                    else
                    {
                        ministryGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( ministryGroupFKString ) );
                    }

                    if ( ministryGroup == null )
                    {
                        // save immediately so we can use the group as a parent
                        ministryGroup = AddGroup( lookupContext, parentGroup.GroupTypeId, parentGroupId, ministryName.Trim(), ministryActive.AsBoolean(), campusId, null, ministryGroupFKString, true, ImportPersonAliasId );
                        ImportedGroups.Add( ministryGroup );
                    }

                    // check for an activity group campus context
                    if ( !string.IsNullOrWhiteSpace( activityName ) && activityName.Any( n => ValidDelimiters.Contains( n ) ) )
                    {
                        if ( activityHasPrefix )
                        {
                            activityName = StripPrefix( activityName, null );
                        }
                        campusId = campusId ?? GetCampusId( activityName );
                        if ( campusId.HasValue || activityHasPrefix )
                        {
                            activityName = StripPrefix( activityName, campusId );
                        }
                    }

                    // add the child activity group if it doesn't exist
                    Group activityGroup = null;
                    var activityGroupFKString = activityId.ToString();
                    if ( isServingActivity )
                    {
                        activityGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( activityServingGroupCascadeFK ) || g.ForeignKey.Equals( activityServingGroupFK ) );
                        activityGroupFKString = "SERVT_" + activityGroupFKString;
                    }
                    else
                    {
                        activityGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( activityId.ToString() ) );
                    }

                    if ( activityGroup == null && activityId.HasValue && !string.IsNullOrWhiteSpace( activityName ) && !activityName.Equals( "Delete", StringComparison.OrdinalIgnoreCase ) )
                    {
                        // don't save immediately, we'll batch add later
                        activityGroup = AddGroup( lookupContext, currentGroupTypeId, ministryGroup.Id, activityName.Trim(), activityActive.AsBoolean(), campusId, null, activityGroupFKString, false, ImportPersonAliasId );
                        newGroups.Add( activityGroup );
                        if ( hasCheckin.Value )
                        {
                            newCheckinGroupForeignKeys.Add( activityGroup.ForeignKey );
                        }
                    }

                    completedItems++;

                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} ministries imported ({1}% complete).", completedItems, percentComplete ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        SaveGroups( newGroups );
                        ReportPartialProgress();
                        ImportedGroups.AddRange( newGroups );
                        ImportedCheckinActivityGroups.AddRange( newGroups.Where( g => newCheckinGroupForeignKeys.Contains( g.ForeignKey ) ) );

                        // Reset lists and context
                        lookupContext = new RockContext();
                        newGroups.Clear();
                        newCheckinGroupForeignKeys.Clear();
                    }
                }
            }

            if ( newGroups.Any() )
            {
                SaveGroups( newGroups );
                ImportedGroups.AddRange( newGroups );
            }

            if ( newCheckinGroupForeignKeys.Any() )
            {
                ImportedCheckinActivityGroups.AddRange( newGroups.Where( g => newCheckinGroupForeignKeys.Contains( g.ForeignKey ) ) );
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished ministry import: {0:N0} ministries imported.", completedItems ) );
        }

        /// <summary>
        /// Maps the activity group data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapActivityGroup( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var newGroups = new List<Group>();

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var archivedScheduleName = "Archived Attendance";
            var archivedScheduleId = new ScheduleService( lookupContext ).Queryable()
                .Where( s => s.Name.Equals( archivedScheduleName, StringComparison.OrdinalIgnoreCase ) )
                .Select( s => ( int? ) s.Id ).FirstOrDefault();
            if ( !archivedScheduleId.HasValue )
            {
                var archivedSchedule = AddNamedSchedule( lookupContext, archivedScheduleName, null, null, null,
                    ImportDateTime, archivedScheduleName.RemoveSpecialCharacters(), true, ImportPersonAliasId );
                archivedScheduleId = archivedSchedule.Id;
            }

            var servingGroupsParentName = "Archived Serving Groups";
            var archivedServingGroupsParent = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( servingGroupsParentName.RemoveWhitespace() ) );
            if ( archivedServingGroupsParent == null )
            {
                archivedServingGroupsParent = AddGroup( lookupContext, ServingTeamGroupType.Id, ServingTeamsParentGroup.Id, servingGroupsParentName, true, null, ImportDateTime, servingGroupsParentName.RemoveWhitespace(), true, ImportPersonAliasId );
                ImportedGroups.Add( archivedServingGroupsParent );
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying activity import ({0:N0} found, {1:N0} already exist).", totalRows, ImportedGroups.Count ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                // get the group data
                var activityId = row["Activity_ID"] as int?;
                var activityGroupId = row["Activity_Group_ID"] as int?;
                var superGroupId = row["Activity_Super_Group_ID"] as int?;
                var activityGroupName = row["Activity_Group_Name"] as string;
                var superGroupName = row["Activity_Super_Group"] as string;
                var balanceType = row["CheckinBalanceType"] as string;
                var scheduleId = archivedScheduleId;

                // get the top-level activity group
                if ( activityId.HasValue && !activityGroupName.Equals( "Delete", StringComparison.OrdinalIgnoreCase ) )
                {
                    var activityServingGroupFK = "SERV_" + activityId.Value.ToString();
                    var activityServingGroupCascadeFK = "SERVT_" + activityId.Value.ToString();
                    var isActivityGroupServing = ( !string.IsNullOrWhiteSpace( activityGroupName ) && activityGroupName.RemoveSpaces().StartsWith( "SERV:", StringComparison.OrdinalIgnoreCase ) );
                    if ( isActivityGroupServing )
                    {
                        // remove SERV: prefix
                        activityGroupName = StripPrefix( activityGroupName, null );
                    }
                    string activitySuperGroupServingGroupFK = string.Empty;
                    string activitySuperGroupServingGroupCascadeFK = string.Empty;
                    var isSuperGroupServing = false;
                    if ( superGroupId.HasValue )
                    {
                        activitySuperGroupServingGroupFK = "SERV_" + superGroupId.Value.ToString();
                        activitySuperGroupServingGroupCascadeFK = "SERVT_" + superGroupId.Value.ToString();
                        isSuperGroupServing = ( !string.IsNullOrWhiteSpace( superGroupName ) && superGroupName.RemoveSpaces().StartsWith( "SERV:", StringComparison.OrdinalIgnoreCase ) );
                        if ( isSuperGroupServing )
                        {
                            // remove SERV: prefix
                            superGroupName = StripPrefix( superGroupName, null );
                        }
                    }
                    string activityGroupServingGroupFK = string.Empty;
                    string activityGroupServingGroupCascadeFK = string.Empty;
                    if ( activityGroupId.HasValue )
                    {
                        activityGroupServingGroupFK = "SERV_" + activityGroupId.Value.ToString();
                        activityGroupServingGroupCascadeFK = "SERVT_" + activityGroupId.Value.ToString();
                    }

                    // check for parent group first
                    Group parentGroup = null;
                    Group servingParentGroup = null;
                    var parentServingGroupCascade = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( "SERVT_" + activityId.Value.ToString() ) );
                    if ( parentServingGroupCascade != null )
                    {
                        isActivityGroupServing = true;
                        isSuperGroupServing = true;
                        servingParentGroup = parentServingGroupCascade;
                    }
                    else if ( isSuperGroupServing || isActivityGroupServing )
                    {
                        servingParentGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( "SERV_" + activityId.Value.ToString() ) );
                    }
                    if ( servingParentGroup == null )
                    {
                        parentGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( activityId.ToString() ) );
                    }
                    else
                    {
                        parentGroup = servingParentGroup;
                    }
                    
                    if ( ( isSuperGroupServing || isActivityGroupServing ) && servingParentGroup == null && parentGroup != null )
                    {
                        // We do not have a matching serving parent group, but we do have a matching non-serving group. 
                        // This means we have no serving group hierarchy to add our new serving group to.
                        // We need to build one out by copying the non-serving hierarchy that does exist.

                        var newServingGroupHierarchy = BuildParentServingGroupHierarchy( lookupContext, archivedServingGroupsParent, parentGroup, copyCampus: true, creatorPersonAliasId: ImportPersonAliasId );
                        if ( newServingGroupHierarchy.Count > 0 )
                        {
                            ImportedGroups.AddRange( newServingGroupHierarchy );
                            parentGroup = newServingGroupHierarchy[newServingGroupHierarchy.Count - 1];     // The last group created is the parent group for this new group.
                        }
                    }

                    if ( parentGroup != null )
                    {
                        // add a level for the super group activity if it exists
                        int? parentGroupId = parentGroup.Id;
                        if ( superGroupId.HasValue && !string.IsNullOrWhiteSpace( superGroupName ) )
                        {
                            Group superGroup = null;
                            var activitySuperGroupFKString = superGroupId.ToString();
                            if ( isSuperGroupServing || isActivityGroupServing )
                            {
                                superGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( activitySuperGroupServingGroupCascadeFK ) || g.ForeignKey.Equals( activitySuperGroupServingGroupFK ) );
                                activitySuperGroupFKString = isSuperGroupServing ? activitySuperGroupServingGroupCascadeFK : activitySuperGroupServingGroupFK;
                            }
                            else
                            {
                                superGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( superGroupId.ToString() ) );
                            }
                            if ( superGroup == null )
                            {
                                superGroup = AddGroup( lookupContext, parentGroup.GroupTypeId, parentGroupId, superGroupName.Trim(), parentGroup.IsActive, parentGroup.CampusId, null, activitySuperGroupFKString, true, ImportPersonAliasId, scheduleId );
                                ImportedGroups.Add( superGroup );
                                // set parent guid to super group
                                parentGroupId = superGroup.Id;
                            }
                        }

                        // add the child activity group
                        if ( activityGroupId.HasValue && !string.IsNullOrWhiteSpace( activityGroupName ) )
                        {
                            Group activityGroup = null;
                            var activityGroupFKString = activityGroupId.ToString();
                            if ( isActivityGroupServing || isActivityGroupServing )
                            {
                                activityGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( activityGroupServingGroupCascadeFK ) || g.ForeignKey.Equals( activityGroupServingGroupFK ) );
                                activityGroupFKString = "SERVT_" + activityGroupFKString;
                            }
                            else
                            {
                                activityGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( activityGroupId.ToString() ) );
                            }
                            
                            if ( activityGroup == null )
                            {
                                // don't save immediately, we'll batch add later
                                activityGroup = AddGroup( null, parentGroup.GroupTypeId, parentGroupId, activityGroupName.Trim(), parentGroup.IsActive, parentGroup.CampusId, null, activityGroupFKString, false, ImportPersonAliasId, scheduleId );
                                newGroups.Add( activityGroup );
                            }
                        }

                        // #TODO: if Rock ever supports room balancing, check the F1 BalanceType

                        completedItems++;
                        if ( completedItems % percentage < 1 )
                        {
                            var percentComplete = completedItems / percentage;
                            ReportProgress( percentComplete, string.Format( "{0:N0} activities imported ({1}% complete).", completedItems, percentComplete ) );
                        }

                        if ( completedItems % DefaultChunkSize < 1 )
                        {
                            SaveGroups( newGroups );
                            ReportPartialProgress();
                            ImportedGroups.AddRange( newGroups );

                            // Reset lists and context
                            lookupContext = new RockContext();
                            newGroups.Clear();
                        }
                    }
                }
            }

            if ( newGroups.Any() )
            {
                SaveGroups( newGroups );
                ImportedGroups.AddRange( newGroups );
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished activity group import: {0:N0} activities imported.", completedItems ) );
        }

        /// <summary>
        /// Maps the home group membership data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapGroups( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var newGroupMembers = new List<GroupMember>();
            var importedGroupMembers = lookupContext.GroupMembers.Count( gm => gm.ForeignKey != null && gm.Group.GroupTypeId == GeneralGroupTypeId );
            var groupRoleMember = GroupTypeCache.Get( GeneralGroupTypeId ).Roles.FirstOrDefault( r => r.Name.Equals( "Member" ) );
            var servingGroupRoleMember = ServingTeamGroupType.Roles.FirstOrDefault( r => r.Name.Equals( "Member" ) );
            var servingGroupRoleLeader = ServingTeamGroupType.Roles.FirstOrDefault( r => r.Name.Equals( "Leader" ) );

            var archivedScheduleName = "Archived Attendance";
            var archivedScheduleId = new ScheduleService( lookupContext ).Queryable()
                .Where( s => s.Name.Equals( archivedScheduleName, StringComparison.OrdinalIgnoreCase ) )
                .Select( s => ( int? ) s.Id ).FirstOrDefault();
            if ( !archivedScheduleId.HasValue )
            {
                var archivedSchedule = AddNamedSchedule( lookupContext, archivedScheduleName, null, null, null,
                    ImportDateTime, archivedScheduleName.RemoveSpecialCharacters(), true, ImportPersonAliasId );
                archivedScheduleId = archivedSchedule.Id;
            }

            var groupsParentName = "Archived Groups";
            var archivedGroups = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( groupsParentName.RemoveWhitespace() ) );
            if ( archivedGroups == null )
            {
                archivedGroups = AddGroup( lookupContext, GeneralGroupTypeId, null, groupsParentName, true, null, ImportDateTime, groupsParentName.RemoveWhitespace(), true, ImportPersonAliasId );
                ImportedGroups.Add( archivedGroups );
            }

            var servingGroupsParentName = "Archived Serving Groups";
            var archivedServingGroupsParent = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( servingGroupsParentName.RemoveWhitespace() ) );
            if ( archivedServingGroupsParent == null )
            {
                archivedServingGroupsParent = AddGroup( lookupContext, ServingTeamGroupType.Id, ServingTeamsParentGroup.Id, servingGroupsParentName, true, null, ImportDateTime, servingGroupsParentName.RemoveWhitespace(), true, ImportPersonAliasId );
                ImportedGroups.Add( archivedServingGroupsParent );
            }


            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying people groups import ({0:N0} found, {1:N0} already exist).", totalRows, importedGroupMembers ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                var scheduleId = archivedScheduleId;
                var groupId = row["Group_ID"] as int?;
                var groupName = row["Group_Name"] as string;
                var individualId = row["Individual_ID"] as int?;
                var groupMemberRole = row["Group_Member_Type"] as string;
                var groupCreated = row["Created_Date"] as DateTime?;
                var groupType = row["Group_Type_Name"] as string;
                string campusName = null;
                try
                {
                    campusName = row["CampusName"] as string;
                }
                catch
                {
                }

                // require at least a group id and name
                if ( groupId.HasValue && !string.IsNullOrWhiteSpace( groupName ) && !groupName.Equals( "Delete", StringComparison.OrdinalIgnoreCase ) )
                {
                    var isGroupServing = !string.IsNullOrWhiteSpace( groupName ) && groupName.RemoveSpaces().StartsWith( "SERV:", StringComparison.OrdinalIgnoreCase );
                    var isGroupTypeServing = !string.IsNullOrWhiteSpace( groupType ) && groupType.RemoveSpaces().StartsWith( "SERV:", StringComparison.OrdinalIgnoreCase );
                    var peopleGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( groupId.ToString() ) );

                    // Remove SERV: prefix
                    if ( isGroupServing )
                    {
                        groupName = StripPrefix( groupName, null );
                    }
                    if ( isGroupTypeServing )
                    {
                        groupType = StripPrefix( groupType, null );
                    }

                    if ( peopleGroup == null )
                    {
                        int? campusId = null;
                        var parentGroup = isGroupServing || isGroupTypeServing ? archivedServingGroupsParent : archivedGroups;
                        var parentGroupId = parentGroup.Id;
                        int? currentGroupTypeId = isGroupServing || isGroupTypeServing ? ServingTeamGroupType.Id : GeneralGroupTypeId;
                        if ( !string.IsNullOrWhiteSpace( groupType ) )
                        {
                            // check for a campus on the grouptype
                            campusId = GetCampusId( groupType, true, SearchDirection.Ends );
                            if ( campusId.HasValue )
                            {
                                groupType = StripSuffix( groupType, campusId );
                            }
                            groupType = groupType.Trim();

                            // add the grouptype if it doesn't exist

                            if ( !isGroupServing && !isGroupTypeServing )
                            {
                                if ( ImportedGroupTypes.Any( t => t.ForeignKey.Equals( groupType, StringComparison.OrdinalIgnoreCase ) ) )
                                {
                                    currentGroupTypeId = ImportedGroupTypes.Where( t => t.ForeignKey.Equals( groupType, StringComparison.OrdinalIgnoreCase ) ).Select( t => t.Id ).FirstOrDefault();
                                }
                                if ( !currentGroupTypeId.HasValue || currentGroupTypeId.Value == 0 )
                                {
                                    // save immediately so we can use the grouptype for a group
                                    {
                                        var currentGroupType = AddGroupType( lookupContext, groupType, string.Format( "{0} imported {1}", groupType, ImportDateTime ), null,
                                        null, null, true, true, true, true, typeForeignKey: groupType );
                                        currentGroupTypeId = currentGroupType.Id;
                                        ImportedGroupTypes.Add( currentGroupType );
                                    }
                                }
                            }

                            // create a placeholder group for the grouptype if it doesn't exist
                            var placeholderGroupFK = groupType.RemoveWhitespace();
                            Group groupTypePlaceholder = null;
                            if ( isGroupServing || isGroupTypeServing )
                            {
                                groupTypePlaceholder = ImportedGroups.FirstOrDefault( g => g.GroupTypeId == currentGroupTypeId && ( g.ForeignKey.Equals( "SERV_" + placeholderGroupFK ) || g.ForeignKey.Equals( "SERVT_" + placeholderGroupFK ) ) );
                                placeholderGroupFK = ( isGroupTypeServing ? "SERVT_" : "SERV_" ) + placeholderGroupFK;
                            }
                            else
                            {
                                groupTypePlaceholder = ImportedGroups.FirstOrDefault( g => g.GroupTypeId == currentGroupTypeId && g.ForeignKey.Equals( placeholderGroupFK ) );
                            }
                            if ( groupTypePlaceholder == null )
                            {
                                groupTypePlaceholder = AddGroup( lookupContext, currentGroupTypeId, parentGroup.Id, groupType, true, null, ImportDateTime,
                                    placeholderGroupFK, true, ImportPersonAliasId );
                                ImportedGroups.Add( groupTypePlaceholder );
                            }
                            parentGroupId = groupTypePlaceholder.Id;
                        }

                        // put the current group under a campus parent if it exists
                        campusId = campusId ?? GetCampusId( groupName, possibleCampusName: campusName );
                        if ( campusId.HasValue )
                        {
                            // create a campus level parent for the home group
                            var campus = CampusList.FirstOrDefault( c => c.Id == campusId );
                            var campusGroupFK = isGroupServing || isGroupTypeServing ? "SERV_" + campus.ShortCode : campus.ShortCode;
                            var campusGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( campusGroupFK ) && g.ParentGroupId == parentGroupId );
                            if ( campusGroup == null )
                            {
                                campusGroup = AddGroup( lookupContext, parentGroup.GroupTypeId, parentGroupId, campus.Name, true, campus.Id, ImportDateTime, campus.ShortCode, true, ImportPersonAliasId );
                                ImportedGroups.Add( campusGroup );
                            }

                            parentGroupId = campusGroup.Id;
                        }

                        // look to see if this group has a schedule already imported
                        var groupSchedule = ImportedSchedules.FirstOrDefault( s => s.ForeignKey == "F1GD_" + groupId.Value.ToString() );

                        if ( groupSchedule != null )
                        {
                            scheduleId = groupSchedule.Id;
                            var currentGroupType = lookupContext.GroupTypes.FirstOrDefault( t => t.Id == currentGroupTypeId );
                            if ( currentGroupType.AllowedScheduleTypes != ScheduleType.Weekly )
                            {
                                currentGroupType.AllowedScheduleTypes = ScheduleType.Weekly;
                            }
                        }

                        // add the group, finally
                        peopleGroup = AddGroup( lookupContext, currentGroupTypeId, parentGroupId, groupName.Trim(), true, campusId, null, groupId.ToString(), true, ImportPersonAliasId, scheduleId );
                        ImportedGroups.Add( peopleGroup );
                    }

                    // add the group member
                    var personKeys = GetPersonKeys( individualId, null );
                    var groupMemberRoleId = groupRoleMember.Id;
                    if ( isGroupServing || isGroupTypeServing )
                    {
                        var isLeaderRole = !string.IsNullOrWhiteSpace( groupMemberRole ) ? groupMemberRole.ToStringSafe().EndsWith( "Leader" ) : false;
                        groupMemberRoleId = isLeaderRole ? servingGroupRoleLeader.Id : servingGroupRoleMember.Id;
                    }
                    if ( personKeys != null )
                    {
                        newGroupMembers.Add( new GroupMember
                        {
                            IsSystem = false,
                            GroupId = peopleGroup.Id,
                            PersonId = personKeys.PersonId,
                            GroupRoleId = groupMemberRoleId,
                            GroupMemberStatus = GroupMemberStatus.Active,
                            ForeignKey = string.Format( "Membership imported {0}", ImportDateTime )
                        } );

                        completedItems++;
                    }

                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} group members imported ({1}% complete).", completedItems, percentComplete ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        SaveGroupMembers( newGroupMembers );
                        ReportPartialProgress();

                        // Reset lists and context
                        lookupContext = new RockContext();
                        newGroupMembers.Clear();
                    }
                }
            }

            if ( newGroupMembers.Any() )
            {
                SaveGroupMembers( newGroupMembers );
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished people groups import: {0:N0} members imported.", completedItems ) );
        }

        /// <summary>
        /// Maps the RLC data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapRLC( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var importedLocations = lookupContext.Locations.AsNoTracking().Where( l => l.ForeignKey != null ).ToList();
            var newGroups = new List<Group>();

            var archivedScheduleName = "Archived Attendance";
            var archivedScheduleId = new ScheduleService( lookupContext ).Queryable()
                .Where( s => s.Name.Equals( archivedScheduleName, StringComparison.OrdinalIgnoreCase ) )
                .Select( s => ( int? ) s.Id ).FirstOrDefault();
            if ( !archivedScheduleId.HasValue )
            {
                var archivedSchedule = AddNamedSchedule( lookupContext, archivedScheduleName, null, null, null,
                    ImportDateTime, archivedScheduleName.RemoveSpecialCharacters(), true, ImportPersonAliasId );
                archivedScheduleId = archivedSchedule.Id;
            }

            var servingGroupsParentName = "Archived Serving Groups";
            var archivedServingGroupsParent = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( servingGroupsParentName.RemoveWhitespace() ) );
            if ( archivedServingGroupsParent == null )
            {
                archivedServingGroupsParent = AddGroup( lookupContext, ServingTeamGroupType.Id, ServingTeamsParentGroup.Id, servingGroupsParentName, true, null, ImportDateTime, servingGroupsParentName.RemoveWhitespace(), true, ImportPersonAliasId );
                ImportedGroups.Add( archivedServingGroupsParent );
            }

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying group location import ({0:N0} found, {1:N0} already exist).", totalRows, importedLocations.Count ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                // get the group and location data
                var rlcId = row["RLC_ID"] as int?;
                var activityId = row["Activity_ID"] as int?;
                var rlcName = row["RLC_Name"] as string;
                var activityGroupId = row["Activity_Group_ID"] as int?;
                var startAgeAttribute = row["Start_Age_Date"] as DateTime?;
                var endAgeAttribute = row["End_Age_Date"] as DateTime?;
                var rlcActive = row["Is_Active"] as Boolean?;
                var roomCode = row["Room_Code"] as string;
                var roomDescription = row["Room_Desc"] as string;
                var roomName = row["Room_Name"] as string;
                var roomCapacity = row["Max_Capacity"] as int?;
                var buildingName = row["Building_Name"] as string;
                var rlcServingGroupFK = "SERV_" + activityId.Value.ToString();
                var isServing = ( !string.IsNullOrWhiteSpace( rlcName ) && rlcName.RemoveSpaces().StartsWith( "SERV:", StringComparison.OrdinalIgnoreCase ) );
                var rlcHasPrefix = isServing;

                // get the parent group
                if ( activityId.HasValue && !rlcName.Equals( "Delete", StringComparison.OrdinalIgnoreCase ) )
                {
                    // get the mid-level activity if exists, otherwise the top-level activity
                    var lookupParentId = activityGroupId ?? activityId;

                    // add the child RLC group and locations


                    // check for parent group first
                    Group parentGroup = null;
                    Group servingParentGroup = null;
                    var parentServingGroupCascade = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( "SERVT_" + lookupParentId.Value.ToString() ) );
                    if ( parentServingGroupCascade != null )
                    {
                        isServing = true;
                        servingParentGroup = parentServingGroupCascade;
                    }
                    else if ( isServing )
                    {
                        servingParentGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( "SERV_" + lookupParentId.Value.ToString() ) );
                    }
                    if ( servingParentGroup == null )
                    {
                        parentGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( lookupParentId.ToString() ) );
                    }
                    else
                    {
                        parentGroup = servingParentGroup;
                    }

                    if ( isServing && servingParentGroup == null && parentGroup != null )
                    {
                        // We do not have a matching serving parent group, but we do have a matching non-serving group. 
                        // This means we have no serving group hierarchy to add our new serving group to.
                        // We need to build one out by copying the non-serving hierarchy that does exist.

                        var newServingGroupHierarchy = BuildParentServingGroupHierarchy( lookupContext, archivedServingGroupsParent, parentGroup, copyCampus: true, creatorPersonAliasId: ImportPersonAliasId );
                        if ( newServingGroupHierarchy.Count > 0 )
                        {
                            ImportedGroups.AddRange( newServingGroupHierarchy );
                            parentGroup = newServingGroupHierarchy[newServingGroupHierarchy.Count - 1];     // The last group created is the parent group for this new group.
                        }
                    }

                    if ( parentGroup != null )
                    {
                        if ( rlcId.HasValue && !string.IsNullOrWhiteSpace( rlcName ) )
                        {
                            int? parentLocationId = null;
                            Location campusLocation = null;
                            // get the campus from the room, building, or parent
                            var rlcCampusId = GetCampusId( rlcName, false ) ?? GetCampusId( buildingName, false ) ?? parentGroup.CampusId;
                            if ( rlcCampusId.HasValue )
                            {
                                var campus = lookupContext.Campuses.FirstOrDefault( c => c.Id == rlcCampusId );
                                if ( campus != null )
                                {
                                    campusLocation = campus.Location ?? importedLocations.FirstOrDefault( l => l.ForeignKey.Equals( campus.ShortCode ) );
                                    if ( campusLocation == null )
                                    {
                                        campusLocation = AddNamedLocation( lookupContext, null, campus.Name, campus.IsActive, null, ImportDateTime, campus.ShortCode, true, ImportPersonAliasId );
                                        importedLocations.Add( campusLocation );
                                        campus.LocationId = campusLocation.Id;
                                        lookupContext.SaveChanges();
                                    }

                                    parentLocationId = campusLocation.Id;
                                }
                            }

                            // set the location structure
                            Location roomLocation = null;
                            if ( !string.IsNullOrWhiteSpace( roomName ) )
                            {
                                // get the building if it exists
                                Location buildingLocation = null;
                                if ( !string.IsNullOrWhiteSpace( buildingName ) )
                                {
                                    buildingLocation = importedLocations.FirstOrDefault( l => l.ForeignKey.Equals( buildingName ) && l.ParentLocationId == parentLocationId );
                                    if ( buildingLocation == null )
                                    {
                                        buildingLocation = AddNamedLocation( lookupContext, parentLocationId, buildingName, rlcActive, null, ImportDateTime, buildingName, true, ImportPersonAliasId );
                                        importedLocations.Add( buildingLocation );
                                    }

                                    parentLocationId = buildingLocation.Id;
                                }

                                // get the room if it exists in the current structure
                                roomLocation = importedLocations.FirstOrDefault( l => l.ForeignKey.Equals( roomName ) && l.ParentLocationId == parentLocationId );
                                if ( roomLocation == null )
                                {
                                    roomLocation = AddNamedLocation( null, parentLocationId, roomName, rlcActive, roomCapacity, ImportDateTime, roomName, true, ImportPersonAliasId );
                                    importedLocations.Add( roomLocation );
                                }
                            }

                            if ( rlcHasPrefix )
                            {
                                rlcName = StripPrefix( rlcName, null );
                            }
                            var scheduleId = parentGroup.ScheduleId;

                            // check for possible schedules already imported, but only if they are not groups created from a "Has_Checkin" ActivityMinistry

                            if ( !isServing && !ImportedCheckinActivityGroups.Any( g => g.Id == parentGroup.Id ) )
                            {
                                var activitySchedule = ImportedSchedules.FirstOrDefault( s => s.ForeignKey.Substring( s.ForeignKey.IndexOf( "-" ) + 1 ) == activityId.ToString() );
                                if ( activitySchedule != null )
                                {
                                    // previously imported schedule was at Activity level. Clone a schedule specific to this group
                                    var newSchedule = new Schedule
                                    {
                                        Description = activitySchedule.Name,
                                        iCalendarContent = activitySchedule.iCalendarContent,
                                        WeeklyDayOfWeek = activitySchedule.WeeklyDayOfWeek,
                                        WeeklyTimeOfDay = activitySchedule.WeeklyTimeOfDay,
                                        CreatedDateTime = null,
                                        ForeignKey = "RLC_" + activitySchedule.ForeignKey,
                                        CreatedByPersonAliasId = ImportPersonAliasId,
                                        IsActive = activitySchedule.IsActive
                                    };
                                    lookupContext.Schedules.Add( newSchedule );
                                    lookupContext.SaveChanges( DisableAuditing );
                                    scheduleId = newSchedule.Id;
                                    var currentGroupType = lookupContext.GroupTypes.FirstOrDefault( t => t.Id == parentGroup.GroupTypeId );

                                    if ( currentGroupType.AllowedScheduleTypes != ScheduleType.Weekly )
                                    {
                                        currentGroupType.AllowedScheduleTypes = ScheduleType.Weekly;
                                        lookupContext.SaveChanges( true );
                                    }
                                }
                            }

                            // create the rlc group
                            var rlcGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( rlcId.ToString() ) );

                            if ( rlcGroup == null )
                            {
                                // don't save immediately, we'll batch add later
                                rlcGroup = AddGroup( null, parentGroup.GroupTypeId, parentGroup.Id, rlcName.Trim(), rlcActive ?? true, rlcCampusId, null, rlcId.ToString(), false, ImportPersonAliasId, scheduleId );
                                if ( roomLocation != null )
                                {
                                    rlcGroup.GroupLocations.Add( new GroupLocation { LocationId = roomLocation.Id } );
                                }

                                newGroups.Add( rlcGroup );
                            }
                        }

                        completedItems++;
                        if ( completedItems % percentage < 1 )
                        {
                            var percentComplete = completedItems / percentage;
                            ReportProgress( percentComplete, string.Format( "{0:N0} location groups imported ({1}% complete).", completedItems, percentComplete ) );
                        }

                        if ( completedItems % DefaultChunkSize < 1 )
                        {
                            SaveGroups( newGroups );
                            ImportedGroups.AddRange( newGroups );
                            ReportPartialProgress();

                            // Reset lists and context
                            lookupContext = new RockContext();
                            newGroups.Clear();
                        }
                    }
                }
            }

            if ( newGroups.Any() )
            {
                SaveGroups( newGroups );
                ImportedGroups.AddRange( newGroups );
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished group location import: {0:N0} locations imported.", completedItems ) );
        }

        /// <summary>
        /// Maps the volunteer assignment data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapStaffingAssignment( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var excludedGroupTypes = new List<int> { FamilyGroupTypeId, SmallGroupTypeId, GeneralGroupTypeId };
            var importedGroupMembers = lookupContext.GroupMembers.Count( gm => gm.ForeignKey != null && !excludedGroupTypes.Contains( gm.Group.GroupTypeId ) );
            var skippedGroups = new Dictionary<int, string>();
            var newGroupMembers = new List<GroupMember>();

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying volunteer assignment import ({0:N0} found, {1:N0} already exist).", totalRows, importedGroupMembers ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                // get the group and role data
                var individualId = row["Individual_ID"] as int?;
                var roleTitle = row["Job_Title"] as string;
                var isActive = row["Is_Active"] as bool?;
                var ministryId = row["Ministry_ID"] as int?;
                var activityId = row["Activity_ID"] as int?;
                var activityGroupId = row["Activity_Group_ID"] as int?;
                var activityTimeName = row["Activity_Time_Name"] as string;
                var rlcId = row["RLC_ID"] as int?;
                var jobId = row["JobID"] as int?;

                var groupLookupId = rlcId ?? activityGroupId ?? activityId ?? ministryId;
                var volunteerGroup = ImportedGroups.FirstOrDefault( g => ( g.ForeignKey.Equals( groupLookupId.ToString() ) || g.ForeignKey.Equals( "SERV_" + groupLookupId.ToString() ) ) && !excludedGroupTypes.Contains( g.GroupTypeId ) );
                if ( volunteerGroup != null )
                {
                    var personKeys = GetPersonKeys( individualId, null );
                    if ( personKeys != null )
                    {
                        var campusId = GetCampusId( roleTitle );
                        if ( campusId.HasValue )
                        {
                            // strip the campus from the role
                            roleTitle = StripPrefix( roleTitle, campusId );
                        }

                        var isLeaderRole = !string.IsNullOrWhiteSpace( roleTitle ) ? roleTitle.ToStringSafe().EndsWith( "Leader" ) : false;
                        var groupTypeRole = GetGroupTypeRole( lookupContext, volunteerGroup.GroupTypeId, roleTitle, string.Format( "{0} imported {1}", activityTimeName, ImportDateTime ), isLeaderRole, 0, true, null, jobId.ToStringSafe(), ImportPersonAliasId );

                        newGroupMembers.Add( new GroupMember
                        {
                            IsSystem = false,
                            GroupId = volunteerGroup.Id,
                            PersonId = personKeys.PersonId,
                            GroupRoleId = groupTypeRole.Id,
                            GroupMemberStatus = isActive != false ? GroupMemberStatus.Active : GroupMemberStatus.Inactive,
                            ForeignKey = string.Format( "Membership imported {0}", ImportDateTime )
                        } );

                        completedItems++;
                    }
                }
                else
                {
                    skippedGroups.AddOrIgnore( ( int ) groupLookupId, string.Empty );
                }

                if ( completedItems % percentage < 1 )
                {
                    var percentComplete = completedItems / percentage;
                    ReportProgress( percentComplete, string.Format( "{0:N0} assignments imported ({1}% complete).", completedItems, percentComplete ) );
                }

                if ( completedItems % DefaultChunkSize < 1 )
                {
                    SaveGroupMembers( newGroupMembers );
                    ReportPartialProgress();

                    // Reset lists and context
                    lookupContext = new RockContext();
                    newGroupMembers.Clear();
                }
            }

            if ( newGroupMembers.Any() )
            {
                SaveGroupMembers( newGroupMembers );
            }

            if ( skippedGroups.Any() )
            {
                ReportProgress( 0, "The following volunteer groups could not be found and were skipped:" );
                foreach ( var key in skippedGroups )
                {
                    ReportProgress( 0, string.Format( "{0}Assignments for group ID {1}.", key.Value, key ) );
                }
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished volunteer assignment import: {0:N0} assignments imported.", completedItems ) );
        }

        /// <summary>
        /// Maps the activity schedule data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapActivitySchedule( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var newSchedules = new List<Schedule>();

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying activity schedules import ({0:N0} found, {1:N0} already exist).", totalRows, ImportedSchedules.Count ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                // get the schedule data
                var activityScheduleId = row["Activity_Schedule_ID"] as int?;
                var activityId = row["Activity_ID"] as int?;
                var activityScheduleName = row["Activity_Time_Name"] as string;
                var startTime = row["Activity_Start_Time"] as DateTime?;
                var endTime = row["Activity_End_Time"] as DateTime?;

                // Only pull in schedules with valid values and "weekly" in the name 
                if ( activityScheduleId.HasValue && startTime.HasValue && activityScheduleName.ToLower().Contains( "weekly" ) )
                {
                    var currentSchedule = ImportedSchedules.FirstOrDefault( s => s.ForeignId.Equals( activityScheduleId.Value ) );
                    var scheduleFK = activityScheduleId.Value.ToString();
                    if ( activityId.HasValue )
                    {
                        scheduleFK = scheduleFK + "-" + activityId.Value.ToString();
                    }
                    if ( currentSchedule == null )
                    {
                        var day = startTime.Value.DayOfWeek;
                        var scheduleActive = endTime.HasValue && endTime.Value > DateTime.Today ? true : false;
                        // don't save immediately, we'll batch add later
                        currentSchedule = AddNamedSchedule( lookupContext, string.Empty, string.Empty, day, startTime, null, scheduleFK, instantSave: false, creatorPersonAliasId: ImportPersonAliasId, isActive: scheduleActive );
                        newSchedules.Add( currentSchedule );
                    }

                    completedItems++;
                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} activity schedules imported ({1}% complete).", completedItems, percentComplete ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        SaveSchedules( newSchedules );
                        ReportPartialProgress();
                        ImportedSchedules.AddRange( newSchedules );

                        // Reset lists and context
                        lookupContext = new RockContext();
                        newSchedules.Clear();
                    }
                }
            }

            if ( newSchedules.Any() )
            {
                SaveSchedules( newSchedules );
                ImportedSchedules.AddRange( newSchedules );
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished activity schedule import: {0:N0} schedules imported.", completedItems ) );
        }

        /// <summary>
        /// Maps the GroupsDescription data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapGroupsDescription( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext();
            var newSchedules = new List<Schedule>();

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completedItems = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, string.Format( "Verifying group schedules import ({0:N0} found, {1:N0} already exist).", totalRows, ImportedSchedules.Where( s => s.ForeignKey.Contains( "F1GD_" ) ).ToList().Count ) );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                // get the schedule data
                var recurrenceType = row["RecurrenceType"] as string;
                var scheduleDay = row["ScheduleDay"] as string;
                var startHour = row["StartHour"] as string;
                var groupId = row["Group_ID"] as int?;
                var scheduleForeignKey = "F1GD_" + groupId.Value.ToString();
                DateTime? startTime = null;
                DateTime startTimeWorker;
                if ( !string.IsNullOrWhiteSpace( startHour ) )
                {
                    startHour += ":00";   // StartHour comes in with HH:mm format. Add 00 seconds for full time format
                    if ( DateTime.TryParse( startHour, out startTimeWorker ) )
                    {
                        startTime = startTimeWorker;
                    }
                }

                // Only pull in valid schedules with RecurrenceType of weekly
                if ( !string.IsNullOrWhiteSpace( recurrenceType ) && recurrenceType.Trim().ToLower() == "weekly" && !string.IsNullOrWhiteSpace( scheduleDay ) && startTime != null )
                {
                    var currentSchedule = ImportedSchedules.FirstOrDefault( s => s.ForeignKey.Equals( scheduleForeignKey ) );
                    if ( currentSchedule == null )
                    {
                        DayOfWeek dayEnum;
                        if ( Enum.TryParse( scheduleDay.Trim(), true, out dayEnum ) )
                        {
                            var day = dayEnum;

                            // don't save immediately, we'll batch add later
                            currentSchedule = AddNamedSchedule( lookupContext, string.Empty, string.Empty, day, startTime, null, scheduleForeignKey, instantSave: false, creatorPersonAliasId: ImportPersonAliasId );
                            newSchedules.Add( currentSchedule );
                        }
                    }

                    completedItems++;
                    if ( completedItems % percentage < 1 )
                    {
                        var percentComplete = completedItems / percentage;
                        ReportProgress( percentComplete, string.Format( "{0:N0} group description schedules imported ({1}% complete).", completedItems, percentComplete ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        SaveSchedules( newSchedules );
                        ReportPartialProgress();
                        ImportedSchedules.AddRange( newSchedules );

                        // Reset lists and context
                        lookupContext = new RockContext();
                        newSchedules.Clear();
                    }
                }
            }

            if ( newSchedules.Any() )
            {
                SaveSchedules( newSchedules );
                ImportedSchedules.AddRange( newSchedules );
            }

            lookupContext.Dispose();
            ReportProgress( 100, string.Format( "Finished group description schedule import: {0:N0} schedules imported.", completedItems ) );
        }


        /// <summary>
        /// Saves the new groups.
        /// </summary>
        /// <param name="newGroups">The new groups.</param>
        private static void SaveGroups( List<Group> newGroups )
        {
            using ( var rockContext = new RockContext() )
            {
                // can't use bulk insert bc Group contains Members
                rockContext.Groups.AddRange( newGroups );
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Saves the new group members.
        /// </summary>
        /// <param name="newGroupMembers">The new group members.</param>
        private static void SaveGroupMembers( List<GroupMember> newGroupMembers )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.BulkInsert( newGroupMembers );
            }
        }


        /// <summary>
        /// Saves the new schedules.
        /// </summary>
        /// <param name="newSchedules">The new schedules.</param>
        private static void SaveSchedules( List<Schedule> newSchedules )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Schedules.AddRange( newSchedules );
                rockContext.SaveChanges( DisableAuditing );
            }
        }
    }
}