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
using System.Data.Entity;
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using static Bulldozer.Utility.Extensions;
using Bulldozer.Utility;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the GroupMember import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Main Methods

        /// <summary>
        /// Loads the group membership data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroupMember( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var groupTypeRoleService = new GroupTypeRoleService( lookupContext );
            var groupMemberService = new GroupMemberService( lookupContext );

            Dictionary<string, int> importedMembers = groupMemberService.Queryable( true ).AsNoTracking()
                .Where( m => m.ForeignKey != null && m.Group.GroupTypeId != CachedTypes.KnownRelationshipGroupType.Id )
                .ToDictionary( m => m.ForeignKey, m => m.Id );

            var groupTypeRoles = new Dictionary<int?, Dictionary<string, int>>();
            foreach ( var role in groupTypeRoleService.Queryable().AsNoTracking().GroupBy( r => r.GroupTypeId ) )
            {
                groupTypeRoles.Add( role.Key, role.ToDictionary( r => r.Name, r => r.Id, StringComparer.OrdinalIgnoreCase ) );
            }

            var currentGroup = new Group();
            var newMemberList = new List<GroupMember>();

            int completed = 0;
            int imported = 0;

            ReportProgress( 0, string.Format( "Starting group member import ({0:N0} already exist).", importedMembers.Count ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                string rowGroupMemberKey = row[GroupMemberId];
                string rowGroupKey = row[GroupMemberGroupId];
                string rowPersonKey = row[GroupMemberPersonId];
                string rowCreatedDate = row[GroupMemberCreatedDate];
                string rowMemberRole = row[GroupMemberRole];
                string rowMemberActive = row[GroupMemberActive];
                int? rowGroupMemberId = rowGroupMemberKey.AsType<int?>();

                //
                // Find this person in the database.
                //
                var personKeys = GetPersonKeys( rowPersonKey );
                if ( personKeys == null || personKeys.PersonId == 0 )
                {
                    LogException( "InvalidPersonKey", string.Format( "Person key {0} not found", rowPersonKey ) );
                    ReportProgress( 0, string.Format( "Person key {0} not found", rowPersonKey ) );
                }

                //
                // Check that this member isn't already in our data
                //
                bool memberExists = false;
                if ( importedMembers.Count > 0 )
                {
                    memberExists = importedMembers.ContainsKey( rowGroupMemberKey );
                }

                if ( !memberExists && ( personKeys != null && personKeys.PersonId != 0 ) )
                {
                    if ( currentGroup == null || rowGroupKey != currentGroup.ForeignKey )
                    {
                        currentGroup = ImportedGroups.FirstOrDefault( g => g.ForeignKey.Equals( rowGroupKey ) );
                    }
                    if ( currentGroup != null )
                    {
                        GroupMember groupMember = new GroupMember();
                        groupMember.PersonId = personKeys.PersonId;
                        groupMember.GroupId = currentGroup.Id;
                        groupMember.CreatedDateTime = ParseDateOrDefault( rowCreatedDate, ImportDateTime );
                        groupMember.ModifiedDateTime = ImportDateTime;
                        groupMember.CreatedByPersonAliasId = ImportPersonAliasId;
                        groupMember.ForeignKey = rowGroupMemberKey;
                        groupMember.ForeignId = rowGroupMemberId;
                        groupMember.GroupMemberStatus = GetGroupMemberStatus( rowMemberActive );
                        groupMember.GroupTypeId = currentGroup.GroupTypeId;

                        //
                        // Find and set the group role id.
                        //
                        if ( !string.IsNullOrEmpty( rowMemberRole ) )
                        {
                            var typeExists = groupTypeRoles.ContainsKey( currentGroup.GroupTypeId );
                            if ( typeExists && groupTypeRoles[currentGroup.GroupTypeId].ContainsKey( rowMemberRole ) )
                            {
                                groupMember.GroupRoleId = groupTypeRoles[currentGroup.GroupTypeId][rowMemberRole];
                            }
                            else
                            {
                                var newRoleId = AddGroupRole( lookupContext, currentGroup.GroupType.Guid.ToString(), rowMemberRole );
                                // check if adding an additional role for this grouptype or creating the first one
                                if ( typeExists )
                                {
                                    groupTypeRoles[currentGroup.GroupType.Id].Add( rowMemberRole, newRoleId );
                                }
                                else
                                {
                                    groupTypeRoles.Add( currentGroup.GroupType.Id, new Dictionary<string, int> { { rowMemberRole, newRoleId } } );
                                }

                                groupMember.GroupRoleId = newRoleId;
                            }
                        }
                        else
                        {
                            if ( currentGroup.GroupType.DefaultGroupRoleId != null )
                            {
                                groupMember.GroupRoleId = ( int ) currentGroup.GroupType.DefaultGroupRoleId;
                            }
                            else
                            {
                                groupMember.GroupRoleId = currentGroup.GroupType.Roles.First().Id;
                            }
                        }

                        //
                        // Add member to the group.
                        //
                        currentGroup.Members.Add( groupMember );
                        newMemberList.Add( groupMember );
                        imported++;
                    }
                    else
                    {
                        LogException( "InvalidGroupKey", string.Format( "Group key {0} not found", rowGroupKey ) );
                    }
                }

                //
                // Notify user of our status.
                //
                completed++;
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} rows processed, {1:N0} members imported.", completed, imported ) );
                }

                if ( completed % ReportingNumber < 1 )
                {
                    SaveGroupMembers( newMemberList );
                    lookupContext.SaveChanges();
                    ReportPartialProgress();

                    // Clear out variables
                    currentGroup = new Group();
                    newMemberList.Clear();
                }
            }

            //
            // Save any final changes to new groups
            //
            if ( newMemberList.Any() )
            {
                SaveGroupMembers( newMemberList );
            }

            //
            // Save any changes to existing groups
            //
            lookupContext.SaveChanges();
            lookupContext.Dispose();

            ReportProgress( 0, string.Format( "Finished group member import: {0:N0} members added.", imported ) );

            return completed;
        }

        /// <summary>
        /// Get the group member status from the given string CSV field.
        ///
        /// -blank-/A/ACTIVE/T/TRUE/Y/YES/1: Active; P/PENDING: Pending; Anything else: Inactive
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private GroupMemberStatus GetGroupMemberStatus( string value )
        {
            string upval = value.ToUpper();

            if ( upval == "" || upval == "A" || upval == "ACTIVE" || upval == "T" || upval == "TRUE" || upval == "Y" || upval == "YES" || upval == "1" )
            {
                return GroupMemberStatus.Active;
            }
            else if ( upval == "P" || upval == "PENDING" )
            {
                return GroupMemberStatus.Pending;
            }
            else
            {
                return GroupMemberStatus.Inactive;
            }
        }

        /// <summary>
        /// Saves all group changes.
        /// </summary>
        /// <param name="memberList">The member list.</param>
        private void SaveGroupMembers( List<GroupMember> memberList )
        {
            var rockContext = new RockContext();

            //
            // First save any unsaved groups
            //
            if ( memberList.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.GroupMembers.AddRange( memberList );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }

        #endregion Main Methods

        #region Relationship Groups

        /// <summary>
        /// Loads the group membership data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadRelationshipGroupMember( CSVInstance csvData )
        {
            AddMissingRelationshipGroups();

            var lookupContext = new RockContext();
            var groupTypeRoleService = new GroupTypeRoleService( lookupContext );
            var groupMemberService = new GroupMemberService( lookupContext );

            var newMemberList = new List<GroupMember>();

            int completed = 0;
            int imported = 0;

            ReportProgress( 0, "Starting relationship import." );
            var knownRelationshipGroupType = CachedTypes.KnownRelationshipGroupType;

            if ( knownRelationshipGroupType != null )
            {
                var relationshipGroupTypeRoles = groupTypeRoleService
                    .Queryable().AsNoTracking()
                    .Where( r => r.GroupTypeId == knownRelationshipGroupType.Id )
                    .ToDictionary( r => r.Name, r => r.Id );

                string[] row;
                // Uses a look-ahead enumerator: this call will move to the next record immediately
                while ( ( row = csvData.Database.FirstOrDefault() ) != null )
                {
                    string rowGroupMemberKey = row[GroupMemberId];
                    string rowGroupKey = row[GroupMemberGroupId];
                    string rowPersonKey = row[GroupMemberPersonId];
                    string rowCreatedDate = row[GroupMemberCreatedDate];
                    string rowMemberRole = row[GroupMemberRole];
                    string rowMemberActive = row[GroupMemberActive];
                    int? rowGroupMemberId = rowGroupMemberKey.AsType<int?>();

                    //
                    // Find Owner
                    //
                    var ownerKeys = GetPersonKeys( rowGroupKey );
                    if ( ownerKeys == null || ownerKeys.PersonId == 0 )
                    {
                        LogException( "InvalidGroupKey", string.Format( "Owner person key {0} not found", rowGroupKey ) );
                        ReportProgress( 0, string.Format( "Owner person key {0} not found", rowGroupKey ) );
                    }

                    //
                    // Find this person in the database.
                    //
                    var personKeys = GetPersonKeys( rowPersonKey );
                    if ( personKeys == null || personKeys.PersonId == 0 )
                    {
                        LogException( "InvalidPersonKey", string.Format( "Person key {0} not found", rowPersonKey ) );
                        ReportProgress( 0, string.Format( "Person key {0} not found", rowPersonKey ) );
                    }

                    if ( ownerKeys != null && ownerKeys.PersonId != 0 )
                    {
                        var knownRelationshipGroup = new GroupMemberService( lookupContext ).Queryable( true )
                                    .AsNoTracking()
                                    .Where(
                                        m => m.PersonId == ownerKeys.PersonId
                                        && m.GroupRoleId == CachedTypes.KnownRelationshipOwnerRoleId
                                        && m.Group.GroupTypeId == knownRelationshipGroupType.Id
                                    )
                                    .Select( m => m.Group )
                                    .FirstOrDefault();

                        if ( knownRelationshipGroup != null && knownRelationshipGroup.Id > 0 )
                        {
                            if ( personKeys != null && personKeys.PersonId != 0 )
                            {
                                GroupMember groupMember = new GroupMember();
                                groupMember.PersonId = personKeys.PersonId;
                                groupMember.GroupId = knownRelationshipGroup.Id;
                                groupMember.CreatedDateTime = ParseDateOrDefault( rowCreatedDate, ImportDateTime );
                                groupMember.ModifiedDateTime = ImportDateTime;
                                groupMember.CreatedByPersonAliasId = ImportPersonAliasId;
                                groupMember.ForeignKey = rowGroupMemberKey;
                                groupMember.ForeignId = rowGroupMemberId;
                                groupMember.GroupMemberStatus = GetGroupMemberStatus( rowMemberActive );
                                groupMember.GroupTypeId = knownRelationshipGroup.GroupTypeId;

                                //
                                // Find and set the group role id.
                                //
                                if ( !string.IsNullOrEmpty( rowMemberRole ) )
                                {
                                    if ( relationshipGroupTypeRoles.ContainsKey( rowMemberRole ) )
                                    {
                                        groupMember.GroupRoleId = relationshipGroupTypeRoles[rowMemberRole];
                                    }
                                    else
                                    {
                                        var newRoleId = AddGroupRole( lookupContext, knownRelationshipGroupType.Guid.ToString(), rowMemberRole );
                                        relationshipGroupTypeRoles.Add( rowMemberRole, newRoleId );
                                        groupMember.GroupRoleId = newRoleId;
                                    }
                                }
                                else
                                {
                                    if ( knownRelationshipGroupType.DefaultGroupRoleId != null )
                                    {
                                        groupMember.GroupRoleId = ( int ) knownRelationshipGroupType.DefaultGroupRoleId;
                                    }
                                    else
                                    {
                                        groupMember.GroupRoleId = knownRelationshipGroupType.Roles.First().Id;
                                    }
                                }

                                //
                                // Add member to the group.
                                //
                                knownRelationshipGroup.Members.Add( groupMember );
                                newMemberList.Add( groupMember );
                                imported++;
                            }
                            else
                            {
                                LogException( "InvalidPersonKey", string.Format( "Person with Foreign Id {0} not found", rowPersonKey ) );
                            }
                        }
                        else
                        {
                            LogException( "InvalidGroupKey", string.Format( "Relationship Group with Owner Person Foreign Id {0} not found", rowGroupKey ) );
                        }
                    }
                    else
                    {
                        LogException( "InvalidGroupKey", string.Format( "Relationship Group Owner with Person Foreign Id {0} not found", rowGroupKey ) );
                    }

                    //
                    // Notify user of our status.
                    //
                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} rows processed, {1:N0} relationships imported.", completed, imported ) );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        SaveGroupMembers( newMemberList );
                        lookupContext.SaveChanges();
                        ReportPartialProgress();

                        // Clear out variables
                        newMemberList.Clear();
                    }
                }
            }
            else
            {
                ReportProgress( 0, "Known Relationship Group Type Missing!" );
            }
            //
            // Save any final changes to new groups
            //
            if ( newMemberList.Any() )
            {
                SaveGroupMembers( newMemberList );
            }

            //
            // Save any changes to existing groups
            //
            lookupContext.SaveChanges();
            lookupContext.Dispose();

            ReportProgress( 0, string.Format( "Finished relationship import: {0:N0} relationships added.", imported ) );

            return completed;
        }

        /// <summary>
        /// Adds the missing relationship groups.
        /// Copied from Rock Cleanup Job
        /// </summary>
        /// <param name="relationshipGroupType">Type of the relationship group.</param>
        /// <param name="ownerRoleGuid">The owner role unique identifier.</param>
        private static void AddMissingRelationshipGroups()
        {
            var relationshipGroupType = CachedTypes.KnownRelationshipGroupType;
            var ownerRoleGuid = CachedTypes.KnownRelationshipOwnerRoleGuid;

            if ( relationshipGroupType != null )
            {
                var ownerRoleId = relationshipGroupType.Roles
                    .Where( r => r.Guid.Equals( ownerRoleGuid ) ).Select( a => ( int? ) a.Id ).FirstOrDefault();
                if ( ownerRoleId.HasValue )
                {
                    var rockContext = new RockContext();
                    var personService = new PersonService( rockContext );
                    var memberService = new GroupMemberService( rockContext );

                    var qryGroupOwnerPersonIds = memberService.Queryable( true )
                        .Where( m => m.GroupRoleId == ownerRoleId.Value ).Select( a => a.PersonId );

                    var personIdsWithoutKnownRelationshipGroup = personService.Queryable( true, true ).Where( p => !qryGroupOwnerPersonIds.Contains( p.Id ) ).Select( a => a.Id ).ToList();

                    var groupsToInsert = new List<Group>();
                    var groupGroupMembersToInsert = new Dictionary<Guid, GroupMember>();
                    foreach ( var personId in personIdsWithoutKnownRelationshipGroup )
                    {
                        var groupMember = new GroupMember();
                        groupMember.PersonId = personId;
                        groupMember.GroupRoleId = ownerRoleId.Value;
                        groupMember.GroupTypeId = relationshipGroupType.Id;

                        var group = new Group();
                        group.Name = relationshipGroupType.Name;
                        group.Guid = Guid.NewGuid();
                        group.GroupTypeId = relationshipGroupType.Id;

                        groupGroupMembersToInsert.Add( group.Guid, groupMember );

                        groupsToInsert.Add( group );
                    }

                    if ( groupsToInsert.Any() )
                    {
                        // use BulkInsert just in case there are a large number of groups and group members to insert
                        rockContext.BulkInsert( groupsToInsert );

                        Dictionary<Guid, int> groupIdLookup = new GroupService( rockContext ).Queryable().Where( a => a.GroupTypeId == relationshipGroupType.Id ).Select( a => new { a.Id, a.Guid } ).ToDictionary( k => k.Guid, v => v.Id );
                        var groupMembersToInsert = new List<GroupMember>();
                        foreach ( var groupGroupMember in groupGroupMembersToInsert )
                        {
                            var groupMember = groupGroupMember.Value;
                            groupMember.GroupId = groupIdLookup[groupGroupMember.Key];
                            groupMembersToInsert.Add( groupMember );
                        }

                        rockContext.BulkInsert( groupMembersToInsert );
                    }

                    rockContext.Dispose();
                }
            }
        }

        #endregion Relationship Groups
    }
}