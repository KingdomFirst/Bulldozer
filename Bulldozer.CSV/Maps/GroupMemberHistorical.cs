using Bulldozer.Model;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the GroupMemberHistorical import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Processes the person list.
        /// </summary>
        private int ImportGroupMemberHistorical()
        {
            this.ReportProgress( 0, $"Preparing {this.GroupMemberHistoricalCsvList.Count} GroupMemberHistorical records for processing..." );
            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }

            var rockContext = new RockContext();
            var groupMemberService = new GroupMemberService( rockContext );
            var groupMemberLookup = this.GroupDict.SelectMany( g => g.Value.Members )
                                                        .Where( gm => !string.IsNullOrEmpty( gm.ForeignKey ) && gm.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                        .Select( a => new
                                                        {
                                                            GroupMember = a,
                                                            a.ForeignKey
                                                        } )
                                                        .ToDictionary( k => k.ForeignKey, v => v.GroupMember );
            var groupMemberHistoricalService = new GroupMemberHistoricalService( rockContext );
            var groupMemHistLookup = groupMemberHistoricalService.Queryable()
                                                        .Where( h => !string.IsNullOrEmpty( h.ForeignKey ) && h.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                        .Select( a => new
                                                        {
                                                            GroupMemberHistorical = a,
                                                            a.ForeignKey
                                                        } )
                                                        .ToDictionary( k => k.ForeignKey, v => v.GroupMemberHistorical );

            // Create GroupMember records for those that do not exist yet. 

            var groupMembersToCreate = this.GroupMemberHistoricalCsvList.DistinctBy( gmh => new { gmh.GroupMemberId, gmh.GroupId, gmh.PersonId } )
                                                                        .ToList();

            this.ReportProgress( 0, "Creating GroupMember Records" );

            // Slice data into chunks and process
            var workingGroupMemberImportList = groupMembersToCreate.ToList();
            var groupMemberRemainingToProcess = groupMembersToCreate.Count;
            var groupMembersCompleted = 0;

            while ( groupMemberRemainingToProcess > 0 )
            {
                if ( groupMembersCompleted > 0 && groupMembersCompleted % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{groupMembersCompleted} GroupMember records processed." );
                }

                if ( groupMembersCompleted % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupMemberImportList.Take( Math.Min( this.DefaultChunkSize, workingGroupMemberImportList.Count ) ).ToList();
                    groupMembersCompleted += BulkInsertGroupMembersFromImport( rockContext, csvChunk, groupMemberLookup );
                    groupMemberRemainingToProcess -= csvChunk.Count;
                    workingGroupMemberImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            ReportProgress( 0, $"Finished creating GroupMember Records. {groupMembersCompleted} GroupMember records created." );

            LoadGroupDict();
            groupMemberLookup = this.GroupDict.SelectMany( g => g.Value.Members )
                                                        .Where( gm => !string.IsNullOrEmpty( gm.ForeignKey ) && gm.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                        .Select( a => new
                                                        {
                                                            GroupMember = a,
                                                            a.ForeignKey
                                                        } )
                                                        .ToDictionary( k => k.ForeignKey, v => v.GroupMember );

            this.ReportProgress( 0, "Creating GroupMemberHistorical Records" );

            // Slice data into chunks and process
            var workingGroupMemberHistoricalImportList = this.GroupMemberHistoricalCsvList.ToList();
            var groupMemberHistoricalRemainingToProcess = this.GroupMemberHistoricalCsvList.Count;
            var completed = 0;

            while ( groupMemberHistoricalRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} GroupMemberHistorical records processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupMemberHistoricalImportList.Take( Math.Min( this.DefaultChunkSize, workingGroupMemberHistoricalImportList.Count ) ).ToList();
                    completed += BulkGroupMemberHistoricalImport( rockContext, csvChunk, groupMemHistLookup, groupMemberLookup );
                    groupMemberHistoricalRemainingToProcess -= csvChunk.Count;
                    workingGroupMemberHistoricalImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            ReportProgress( 0, $"Finished creating GroupMemberHistorical Records. {completed} GroupMemberHistorical records created." );

            return completed;
        }

        /// <summary>
        /// Bulk import of GroupMemberHistoricalImports.
        /// </summary>
        /// <param name="groupMemberHistoricalCsvs">The group imports.</param>
        /// <returns></returns>
        public int BulkInsertGroupMembersFromImport( RockContext rockContext, List<GroupMemberHistoricalCsv> groupMemberHistoricalCsvs, Dictionary<string, GroupMember> groupMemberLookup )
        {
            var groupMemberImports = new List<GroupMemberImport>();
            var groupMemberErrors = string.Empty;

            foreach ( var groupMemberHistoricalCsv in groupMemberHistoricalCsvs )
            {
                var group = this.GroupDict.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberHistoricalCsv.GroupId ) );
                if ( group == null )
                {
                    groupMemberErrors += $"{DateTime.Now}, GroupMember, GroupId {groupMemberHistoricalCsv.GroupId} not found. Group Member record for {groupMemberHistoricalCsv.PersonId} was skipped.\r\n";
                    continue;
                }

                var person = this.PersonDict.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberHistoricalCsv.PersonId ) );
                if ( person == null )
                {
                    groupMemberErrors += $"{DateTime.Now}, GroupMember, PersonId {groupMemberHistoricalCsv.PersonId} not found. Group Member for GroupId {groupMemberHistoricalCsv.GroupId} was skipped.\r\n";
                    continue;
                }

                var newGroupMember = new GroupMemberImport()
                {
                    PersonId = person.Id,
                    GroupId = group.Id,
                    GroupTypeId = group.GroupTypeId,
                    RoleName = groupMemberHistoricalCsv.Role,
                    GroupMemberStatus = groupMemberHistoricalCsv.GroupMemberStatus.Value,
                    GroupMemberForeignKey = groupMemberHistoricalCsv.GroupMemberId.IsNotNullOrWhiteSpace() ? string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberHistoricalCsv.GroupMemberId ) : string.Format( "{0}^{1}_{2}", this.ImportInstanceFKPrefix, groupMemberHistoricalCsv.GroupId, groupMemberHistoricalCsv.PersonId )
                };

                groupMemberImports.Add( newGroupMember );
            }

            var groupMembers = groupMemberImports.Where( v => !groupMemberLookup.ContainsKey( v.GroupMemberForeignKey ) ).ToList();

            // Add GroupType Roles if needed
            BulkInsertGroupTypeRoles( rockContext, groupMemberImports, groupMemberLookup );

            var importedDateTime = RockDateTime.Now;
            var groupMembersToInsert = new List<GroupMember>();

            var groupMemberImportByGroupType = groupMembers.GroupBy( a => a.GroupTypeId.Value )
                                                .Select( a => new
                                                {
                                                    GroupTypeId = a.Key,
                                                    GroupMembers = a.Select( x => x ).ToList()
                                                } );

            foreach ( var groupMemberImportObj in groupMemberImportByGroupType )
            {
                var groupTypeCache = GroupTypeCache.Get( groupMemberImportObj.GroupTypeId );
                var groupTypeRoleLookup = groupTypeCache.Roles.ToDictionary( k => k.Name, v => v.Id );

                foreach ( var groupMemberImport in groupMemberImportObj.GroupMembers )
                {
                    var groupRoleId = groupTypeRoleLookup.GetValueOrNull( groupMemberImport.RoleName );
                    if ( !groupRoleId.HasValue || groupRoleId.Value <= 0 )
                    {
                        groupMemberErrors += $"{DateTime.Now}, GroupMember, Group Role {groupMemberImport.RoleName} not found in Group Type. Group Member for Rock GroupId {groupMemberImport.GroupId}, Rock PersonId {groupMemberImport.PersonId} was set to default group type role.\"Member\".\r\n";
                        groupRoleId = groupTypeCache.DefaultGroupRoleId;
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
                    GroupTypeId = groupMember.GroupTypeId.Value,
                    PersonId = groupMember.PersonId.Value,
                    CreatedDateTime = groupMember.CreatedDate.HasValue ? groupMember.CreatedDate.Value : importedDateTime,
                    ModifiedDateTime = importedDateTime,
                    ForeignKey = groupMember.GroupMemberForeignKey,
                    GroupMemberStatus = groupMember.GroupMemberStatus
                };
                groupMembersToInsert.Add( newGroupMember );
            }

            rockContext.BulkInsert( groupMembersToInsert );

            if ( groupMemberErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, groupMemberErrors, showMessage: false, hasMultipleErrors: true );
            }

            return groupMemberHistoricalCsvs.Count;
        }

        public int BulkGroupMemberHistoricalImport( RockContext rockContext, List<GroupMemberHistoricalCsv> groupMemberHistoricalCsvs, Dictionary<string, GroupMemberHistorical> groupMemHistLookup, Dictionary<string, GroupMember> groupMemberLookup )
        {
            var groupMemberHistoricalImports = new List<GroupMemberHistoricalImport>();
            var groupMembersToInsert = new List<GroupMember>();
            var groupMHErrors = string.Empty;
            var importedDateTime = RockDateTime.Now;
            var familyGroupTypeId = GroupTypeCache.GetFamilyGroupType().Id;
            var securityRoleGroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_SECURITY_ROLE.AsGuid() ).Id;
            var knownRelationshipGroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_KNOWN_RELATIONSHIPS.AsGuid() ).Id;
            var peerNetworkGroupTypeId = GroupTypeCache.Get( Rock.SystemGuid.GroupType.GROUPTYPE_PEER_NETWORK.AsGuid() ).Id;
            var groupTypeRoleLookup = new GroupTypeRoleService( rockContext ).Queryable()
                                                                             .GroupBy( gt => gt.GroupTypeId )
                                                                             .ToDictionary( k => k.Key, v => v.Select( x => x ).ToList() );

            foreach ( var groupMemberHistoricalCsv in groupMemberHistoricalCsvs )
            {
                GroupMember groupMember = null;
                if ( groupMemberHistoricalCsv.GroupMemberId.IsNotNullOrWhiteSpace() )
                {
                    groupMember = groupMemberLookup.GetValueOrNull( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberHistoricalCsv.GroupMemberId ) );
                }

                if ( groupMember != null )
                {
                    var newGroupMemberHistorical = new GroupMemberHistoricalImport()
                    {
                        ArchivedDateTime = groupMemberHistoricalCsv.ArchivedDateTime,
                        CurrentRowIndicator = groupMemberHistoricalCsv.CurrentRowIndicator,
                        EffectiveDateTime = groupMemberHistoricalCsv.EffectiveDateTime,
                        ExpireDateTime = groupMemberHistoricalCsv.ExpireDateTime,
                        GroupId = groupMember.GroupId,
                        GroupMember = groupMember,
                        GroupMemberId = groupMember.Id,
                        GroupMemberForeignKey = groupMember.ForeignKey,
                        GroupMemberHistoricalForeignKey = string.Format( "{0}^{1}_{2}_{3}", ImportInstanceFKPrefix, groupMemberHistoricalCsv.GroupId, groupMemberHistoricalCsv.PersonId, long.Parse( groupMemberHistoricalCsv.EffectiveDateTime.ToString( "yyMMddHHmm" ) ) ),
                        GroupTypeId = groupMember.GroupTypeId,
                        PersonId = groupMember.PersonId,
                        InactiveDateTime = groupMemberHistoricalCsv.InactiveDateTime,
                        IsArchived = groupMemberHistoricalCsv.IsArchived,
                        IsLeader = groupMemberHistoricalCsv.IsLeader,
                        Role = groupMemberHistoricalCsv.Role.Left( 100 ),
                    };
                    if ( groupMemberHistoricalCsv.GroupMemberStatus.HasValue )
                    {
                        newGroupMemberHistorical.GroupMemberStatus = groupMemberHistoricalCsv.GroupMemberStatus.Value;
                    }
                    groupMemberHistoricalImports.Add( newGroupMemberHistorical );
                }
            }

            var groupMemberHistorical = groupMemberHistoricalImports.Where( v => !groupMemHistLookup.ContainsKey( v.GroupMemberHistoricalForeignKey ) ).ToList();

            // Add GroupType Roles if needed

            BulkInsertMemberHistoricalGroupTypeRoles( rockContext, groupMemberHistorical, groupTypeRoleLookup );

            var groupMemberHistoricalToInsert = new List<GroupMemberHistorical>();

            var groupMemHistImportByGroupType = groupMemberHistorical.GroupBy( a => a.GroupTypeId.Value )
                                                .Select( a => new
                                                {
                                                    GroupTypeId = a.Key,
                                                    GroupMemberHistoricals = a.Select( x => x ).ToList()
                                                } );

            // populate GroupMemberHistorical records
            foreach ( var groupMemHistImportObj in groupMemHistImportByGroupType )
            {
                var groupTypeRoleNameLookup = groupTypeRoleLookup.GetValueOrNull( groupMemHistImportObj.GroupTypeId ).ToDictionary( k => k.Name, v => v );

                foreach ( var groupMemHistImport in groupMemHistImportObj.GroupMemberHistoricals )
                {
                    var groupRole = groupTypeRoleNameLookup.GetValueOrNull( groupMemHistImport.Role );
                    if ( groupRole == null )
                    {
                        groupMHErrors += $"{DateTime.Now}, GroupMember, Group Role {groupMemHistImport.Role} not found in Group Type. Group Member for Rock GroupId {groupMemHistImport.GroupId}, Rock PersonId {groupMemHistImport.PersonId} was set to default group type role.\"Member\".\r\n";
                        continue;
                    }
                    if ( groupMemHistImport.GroupMember.GroupRoleId == 0 )
                    {
                        groupMemHistImport.GroupMember.GroupRoleId = groupRole.Id;
                    }
                    var newGroupMemberHistorical = new GroupMemberHistorical
                    {
                        ArchivedDateTime = groupMemHistImport.ArchivedDateTime,
                        CurrentRowIndicator = groupMemHistImport.CurrentRowIndicator,
                        EffectiveDateTime = groupMemHistImport.EffectiveDateTime,
                        ExpireDateTime = groupMemHistImport.ExpireDateTime,
                        GroupId = groupMemHistImport.GroupMember.GroupId,
                        GroupMember = groupMemHistImport.GroupMember,
                        GroupMemberId = groupMemHistImport.GroupMemberId.Value,
                        GroupMemberStatus = groupMemHistImport.GroupMemberStatus,
                        GroupRoleId = groupRole.Id,
                        GroupRoleName = groupRole.Name,
                        InactiveDateTime = groupMemHistImport.InactiveDateTime,
                        IsArchived = groupMemHistImport.IsArchived.GetValueOrDefault(),
                        IsLeader = groupMemHistImport.IsLeader.GetValueOrDefault(),
                        ForeignKey = groupMemHistImport.GroupMemberHistoricalForeignKey
                    };
                    groupMemberHistoricalToInsert.Add( newGroupMemberHistorical );
                }
            }

            rockContext.BulkInsert( groupMembersToInsert );

            var groupMemberIdLookup = new GroupMemberService( new RockContext() ).Queryable().Where( gm => gm.GroupTypeId != familyGroupTypeId && gm.GroupTypeId != securityRoleGroupTypeId && gm.GroupTypeId != knownRelationshipGroupTypeId && gm.GroupTypeId != peerNetworkGroupTypeId ).Select( gm => new { gm.Id, gm.Guid } ).ToList().ToDictionary( k => k.Guid, v => v.Id );
            foreach ( var groupMemHist in groupMemberHistoricalToInsert.Where( h => h.GroupMemberId == 0 ) )
            {
                var groupMemberId = groupMemberIdLookup.GetValueOrNull( groupMemHist.GroupMember.Guid );
                if ( groupMemberId.HasValue )
                {
                    groupMemHist.GroupMemberId = groupMemberId.Value;
                }
            }

            rockContext.BulkInsert( groupMemberHistoricalToInsert );

            if ( groupMHErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, groupMHErrors, showMessage: false, hasMultipleErrors: true );
            }

            return groupMemberHistoricalCsvs.Count;
        }

        public void BulkInsertMemberHistoricalGroupTypeRoles( RockContext rockContext, List<GroupMemberHistoricalImport> groupMemberHistoricalImports, Dictionary<int?, List<GroupTypeRole>> groupTypeRoleLookup )
        {
            var importedDateTime = RockDateTime.Now;
            var groupMemHistErrors = string.Empty;
            var importedGroupTypeRoleNames = groupMemberHistoricalImports.GroupBy( a => a.GroupTypeId.Value ).Select( a => new
            {
                GroupTypeId = a.Key,
                RoleNames = a.Select( x => x.Role ).Distinct().ToList()
            } );

            // Create any missing roles on the GroupType
            var groupTypeRolesToInsert = new List<GroupTypeRole>();

            foreach ( var importedGroupTypeRoleName in importedGroupTypeRoleNames )
            {
                List<GroupTypeRole> existingGroupTypeRoles = groupTypeRoleLookup.GetValueOrNull( importedGroupTypeRoleName.GroupTypeId );
                foreach ( var roleName in importedGroupTypeRoleName.RoleNames )
                {
                    if ( !existingGroupTypeRoles.Any( a => a.Name.Left( 100 ).Equals( roleName, StringComparison.OrdinalIgnoreCase ) ) )
                    {
                        var newGroupTypeRole = new GroupTypeRole
                        {
                            GroupTypeId = importedGroupTypeRoleName.GroupTypeId,
                            Name = roleName.Left( 100 ),
                            CreatedDateTime = importedDateTime,
                            ModifiedDateTime = importedDateTime
                        };

                        groupTypeRolesToInsert.Add( newGroupTypeRole );
                    }
                }
            }

            if ( groupTypeRolesToInsert.Any() )
            {
                rockContext.BulkInsert( groupTypeRolesToInsert );

                // reload lookup to include new roles

                groupTypeRoleLookup = new GroupTypeRoleService( rockContext ).Queryable()
                                                                             .GroupBy( gt => gt.GroupTypeId )
                                                                             .ToDictionary( k => k.Key, v => v.Select( x => x ).ToList() );
            }
        }
    }
}
