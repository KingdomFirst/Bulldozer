// <copyright>
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
        /// Processes the group list.
        /// </summary>
        private int ImportGroups()
        {
            ReportProgress( 0, "Preparing Group data for import..." );
            LoadGroupDict();

            if ( this.GroupTypeDict == null )
            {
                LoadGroupTypeDict();
            }
            if ( this.LocationsDict == null )
            {
                LoadLocationDict();
            }
            var groupImportList = new List<GroupImport>();
            var invalidGroups = new List<string>();
            var invalidGroupTypes = new List<string>();

            foreach ( var groupCsv in this.GroupCsvList )
            {
                var groupCsvName = groupCsv.Name;
                if ( groupCsv.Name.IsNullOrWhiteSpace() )
                {
                    groupCsvName = "Unnamed Group";
                }
                var groupType = GroupTypeDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{groupCsv.GroupTypeId}" );
                if ( groupType == null )
                {
                    invalidGroups.Add( groupCsv.Id );
                    invalidGroupTypes.Add( groupCsv.GroupTypeId );
                    continue;
                }
                var groupImport = new GroupImport()
                {
                    GroupForeignId = groupCsv.Id.AsIntegerOrNull(),
                    GroupForeignKey = $"{this.ImportInstanceFKPrefix}^{groupCsv.Id}",
                    GroupTypeId = GroupTypeDict[$"{this.ImportInstanceFKPrefix}^{groupCsv.GroupTypeId}"].Id,
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

                if ( groupCsv.Name.IsNullOrWhiteSpace() )
                {
                    groupImport.Name = "Unnamed Group";
                }

                if ( groupCsv.CampusId.IsNotNullOrWhiteSpace() && groupCsv.CampusId.ToIntSafe( -1 ) != 0 )
                {
                    var csvCampusId = groupCsv.CampusId.AsIntegerOrNull();
                    int? campusId = null;
                    if ( csvCampusId.HasValue && csvCampusId.Value > 0 )
                    {
                        campusId = CampusDict.FirstOrDefault( d => d.Value.ForeignId == csvCampusId.Value ).Value?.Id;
                    }
                    if ( !campusId.HasValue && ( !csvCampusId.HasValue || csvCampusId.Value > 0 ) )
                    {
                        campusId = CampusDict[string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupCsv.CampusId )]?.Id;
                    }
                    groupImport.CampusId = campusId;
                }

                if ( groupCsv.LocationId.IsNotNullOrWhiteSpace() )
                {
                    var location = LocationsDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{groupCsv.GroupTypeId}" );
                    if ( location != null )
                    {
                        groupImport.Location = location;
                    }
                }

                groupImportList.Add( groupImport );
            }
            if ( invalidGroupTypes.Count > 0 && invalidGroups.Count > 0 )
            {
                LogException( "GroupImport", $"The following invalid GroupType(s) in the Group csv resulted in {invalidGroups.Count} group(s) being skipped:\r\n{string.Join( ", ", invalidGroupTypes )}\r\nSkipped GroupId(s):\r\n{string.Join( ", ", invalidGroups )}.", showMessage: false );
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Group Records...", groupImportList.Count ) );

            // Slice data into chunks and process
            var workingGroupImportList = groupImportList.ToList();
            var groupsRemainingToProcess = workingGroupImportList.Count;
            var workingGroupsWithParents = workingGroupImportList.Where( g => g.ParentGroupForeignKey.IsNotNullOrWhiteSpace() ).ToList();
            var completedGroups = 0;
            var insertedGroups = new List<Group>();

            while ( groupsRemainingToProcess > 0 )
            {
                if ( completedGroups > 0 && completedGroups % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroups} Groups processed." );
                }

                if ( completedGroups % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingGroupImportList.Take( Math.Min( this.DefaultChunkSize, workingGroupImportList.Count ) ).ToList();
                    var imported = BulkGroupImport( csvChunk, insertedGroups );
                    completedGroups += imported;
                    groupsRemainingToProcess -= csvChunk.Count;
                    workingGroupImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            // Process any new Schedules and GroupLoctions needed
            BulkInsertGroupSchedules( insertedGroups );
            BulkInsertGroupLocations( insertedGroups );

            var rockContext = new RockContext();
            this.ReportProgress( 0, string.Format( "Begin updating {0} Parent Group Records...", workingGroupsWithParents.Count ) );
            int groupTypeIdFamily = GroupTypeCache.GetFamilyGroupType().Id;
            int groupTypeIdRelationship = CachedTypes.KnownRelationshipGroupType.Id;
            var groupLookup = new GroupService( rockContext ).Queryable().Where( a => a.GroupTypeId != groupTypeIdFamily && a.GroupTypeId != groupTypeIdRelationship && !string.IsNullOrEmpty( a.ForeignKey ) && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) ).Select( a => new
            {
                Group = a,
                a.ForeignKey
            } ).ToDictionary( k => k.ForeignKey, v => v.Group );
            var groupsWithParentsRemainingToProcess = workingGroupsWithParents.Count;
            var completedGroupsWithParents = 0;

            while ( groupsWithParentsRemainingToProcess > 0 )
            {
                if ( completedGroupsWithParents > 0 && completedGroupsWithParents % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedGroupsWithParents} Groups updated." );
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

            return completedGroups;
        }

        /// <summary>
        /// Bulk import of GroupImports.
        /// </summary>
        /// <param name="groupImports">The group imports.</param>
        /// <returns></returns>
        public int BulkGroupImport( List<GroupImport> groupImports, List<Group> insertedGroups )
        {
            var rockContext = new RockContext();
            var importedDateTime = RockDateTime.Now;
            var groupLookup = GroupDict.ToDictionary( p => p.Key, p => p.Value );

            foreach ( var groupImport in groupImports )
            {
                Group group = null;

                if ( groupLookup.ContainsKey( groupImport.GroupForeignKey ) )
                {
                    group = groupLookup[groupImport.GroupForeignKey];
                }

                if ( group == null )
                {
                    var newGroup = new Group();
                    InitializeGroupFromGroupImport( newGroup, groupImport, importedDateTime );
                    newGroup.CreatedDateTime = importedDateTime;
                    newGroup.ModifiedDateTime = importedDateTime;

                    if ( groupImport.Location != null )
                    {
                        newGroup.GroupLocations.Add( new GroupLocation
                        {
                            Group = newGroup,
                            CreatedDateTime = importedDateTime,
                            ModifiedDateTime = importedDateTime,
                            LocationId = groupImport.Location.Id,
                            ForeignKey = groupImport.Location.ForeignKey
                        } );
                    }

                    // set weekly schedule for newly created groups
                    DayOfWeek meetingDay;
                    if ( !string.IsNullOrWhiteSpace( groupImport.MeetingDay ) && Enum.TryParse( groupImport.MeetingDay, out meetingDay ) )
                    {
                        TimeSpan.TryParse( groupImport.MeetingTime, out TimeSpan meetingTime );
                        newGroup.Schedule = new Schedule()
                        {
                            Name = newGroup.Name.Left( 50 ),
                            IsActive = newGroup.IsActive,
                            WeeklyDayOfWeek = meetingDay,
                            WeeklyTimeOfDay = meetingTime,
                            ForeignId = groupImport.GroupForeignId,
                            ForeignKey = groupImport.GroupForeignKey,
                            CreatedDateTime = importedDateTime,
                            ModifiedDateTime = importedDateTime,
                            Description = newGroup.Name.Length > 50 ? newGroup.Name : null
                        };
                    };

                    groupLookup.Add( groupImport.GroupForeignKey, newGroup );
                }
            }

            var groupsToInsert = groupLookup.Where( a => a.Value.Id == 0 ).Select( a => a.Value ).ToList();

            rockContext.BulkInsert( groupsToInsert );
            insertedGroups.AddRange( groupsToInsert );

            LoadGroupDict();

            return groupImports.Count;
        }

        public void BulkUpdateParentGroup( RockContext rockContext, List<GroupImport> groupImports, Dictionary<string, Group> groupLookup )
        {
            var groupsUpdated = false;
            int groupTypeIdFamily = GroupTypeCache.GetFamilyGroupType().Id;

            // Get lookups for Group so we can populate ParentGroups
            var qryGroupTypeGroupLookup = new GroupService( rockContext ).Queryable().Where( g => !string.IsNullOrEmpty( g.ForeignKey ) && g.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) ).Select( a => new
            {
                Group = a,
                GroupForeignKey = a.ForeignKey,
                GroupTypeId = a.GroupTypeId
            } );

            var groupTypeGroupLookup = qryGroupTypeGroupLookup.GroupBy( a => a.GroupTypeId ).ToDictionary( k => k.Key, v => v.ToDictionary( k1 => k1.GroupForeignKey, v1 => v1.Group ) );

            var parentGroupErrors = string.Empty;
            foreach ( var groupImport in groupImports )
            {
                Group group = null;

                group = groupLookup.GetValueOrNull( groupImport.GroupForeignKey );

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
            foreach ( var groupTypeId in groupTypeGroupLookup.Keys )
            {
                GroupTypeCache.UpdateCachedEntity( groupTypeId, EntityState.Detached );
            }

            if ( parentGroupErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, parentGroupErrors, showMessage: false, hasMultipleErrors: true );
            }
        }

        public void BulkInsertGroupSchedules( List<Group> insertedGroups )
        {
            var rockContext = new RockContext();

            var groupSchedulesToInsert = new List<Schedule>();
            foreach ( var groupWithSchedule in insertedGroups.Where( v => v.Schedule != null && v.Schedule.Id == 0 ).ToList() )
            {
                var groupId = GroupDict.GetValueOrNull( groupWithSchedule.ForeignKey )?.Id;
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

        public void BulkInsertGroupLocations( List<Group> insertedGroups )
        {
            var rockContext = new RockContext();

            var groupLocationsToInsert = new List<GroupLocation>();
            foreach ( var groupWithLocation in insertedGroups.Where( g => g.GroupLocations.Count > 0 && g.GroupLocations.Any( gl => gl.Id == 0 ) ).ToList() )
            {
                var groupId = GroupDict.GetValueOrNull( groupWithLocation.ForeignKey )?.Id;
                if ( groupId.HasValue )
                {
                    groupLocationsToInsert.AddRange( groupWithLocation.GroupLocations.Where( gl => gl.Id == 0 ).ToList() );
                }
            }

            rockContext.BulkInsert( groupLocationsToInsert );
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
            if ( this.GroupLocationTypeDVDict == null )
            {
                this.GroupLocationTypeDVDict = LoadDefinedValues( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE.AsGuid() );
            }

            var groupAddressImports = new List<GroupAddressImport>();
            var groupAddressErrors = string.Empty;

            foreach ( var groupAddressCsv in GroupAddressCsvList )
            {
                if ( string.IsNullOrEmpty( groupAddressCsv.Street1 ) )
                {
                    groupAddressErrors += $"{DateTime.Now}, GroupAddress, Blank Street Address for GroupId {groupAddressCsv.GroupId}, Address Type {groupAddressCsv.AddressType}. Group Address was skipped.\r\n";
                    continue;
                }
                var group = this.GroupDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupAddressCsv.GroupId ) );
                if ( group == null )
                {
                    groupAddressErrors += $"{DateTime.Now}, GroupAddress, GroupId {groupAddressCsv.GroupId} not found. Group Address was skipped.\r\n";
                    continue;
                }

                var groupLocationTypeValueId = GetGroupLocationTypeDVId( groupAddressCsv.AddressType );

                if ( groupLocationTypeValueId.HasValue )
                {
                    var newGroupAddress = new GroupAddressImport()
                    {
                        GroupId = group.Id,
                        GroupLocationTypeValueId = groupLocationTypeValueId.Value,
                        IsMailingLocation = groupAddressCsv.IsMailing,
                        IsMappedLocation = groupAddressCsv.AddressType == AddressType.Home,
                        Street1 = groupAddressCsv.Street1.Left( 100 ),
                        Street2 = groupAddressCsv.Street2.Left( 100 ),
                        City = groupAddressCsv.City.Left( 50 ),
                        State = groupAddressCsv.State.Left( 50 ),
                        Country = groupAddressCsv.Country.Left( 50 ),
                        PostalCode = groupAddressCsv.PostalCode.Left( 50 ),
                        Latitude = groupAddressCsv.Latitude.AsDoubleOrNull(),
                        Longitude = groupAddressCsv.Longitude.AsDoubleOrNull(),
                        AddressForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, groupAddressCsv.AddressId.IsNotNullOrWhiteSpace() ? groupAddressCsv.AddressId : string.Format( "{0}_{1}", groupAddressCsv.GroupId, groupAddressCsv.AddressType.ToString() ) )
                    };

                    groupAddressImports.Add( newGroupAddress );
                }
                else
                {
                    groupAddressErrors += $"{DateTime.Now}, GroupAddress, Unexpected Address Type ({groupAddressCsv.AddressType}) encountered for Group \"{groupAddressCsv.GroupId}\". Group Address was skipped.\r\n";
                }
            }

            var rockContext = new RockContext();
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
                LogException( null, groupAddressErrors, showMessage: false, hasMultipleErrors: true );
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
        private int ImportGroupAttributeValues()
        {
            this.ReportProgress( 0, "Preparing Group Attribute Value data for import..." );
            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }


            var rockContext = new RockContext();
            var groupAVImports = new List<AttributeValueImport>();
            var groupAVErrors = string.Empty;

            var definedTypeDict = DefinedTypeCache.All().ToDictionary( k => k.Id, v => v );
            var attributeDefinedValuesDict = new AttributeService( rockContext ).Queryable()
                                                                                .Where( a => a.FieldTypeId == DefinedValueFieldTypeId && a.EntityTypeId == GroupEntityTypeId )
                                                                                .ToDictionary( k => k.Key, v => definedTypeDict.GetValueOrNull( v.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsIntegerOrNull().Value ).DefinedValues.ToDictionary( d => d.Value, d => d.Guid.ToString() ) );

            foreach ( var attributeValueCsv in GroupAttributeValueCsvList )
            {
                var group = this.GroupDict.GetValueOrNull( string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.GroupId ) );
                if ( group == null )
                {
                    groupAVErrors += $"{DateTime.Now}, GroupAttributeValue, GroupId {attributeValueCsv.GroupId} not found. Group AttributeValue for {attributeValueCsv.AttributeKey} attribute was skipped.\r\n";
                    continue;
                }

                var attribute = this.GroupAttributeDict.GetValueOrNull( attributeValueCsv.AttributeKey );
                if ( attribute == null )
                {
                    groupAVErrors += $"{DateTime.Now}, GroupAttributeValue, AttributeKey {attributeValueCsv.AttributeKey} not found. AttributeValue for GroupId {attributeValueCsv.GroupId} was skipped.\r\n";
                    continue;
                }

                var newAttributeValue = new AttributeValueImport()
                {
                    AttributeId = attribute.Id,
                    Value = attributeValueCsv.AttributeValue,
                    AttributeValueForeignId = attributeValueCsv.AttributeValueId.AsIntegerOrNull(),
                    EntityId = group.Id,
                    AttributeValueForeignKey = string.Format( "{0}^{1}", ImportInstanceFKPrefix, attributeValueCsv.AttributeValueId.IsNotNullOrWhiteSpace() ? attributeValueCsv.AttributeValueId : string.Format( "{0}_{1}", attributeValueCsv.GroupId, attributeValueCsv.AttributeKey ) )
                };

                if ( attribute.FieldTypeId == DefinedValueFieldTypeId )
                {
                    newAttributeValue.Value = attributeDefinedValuesDict.GetValueOrNull( attribute.Key ).GetValueOrNull( attributeValueCsv.AttributeValue );
                }
                groupAVImports.Add( newAttributeValue );
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Group Attribute Value Records...", groupAVImports.Count ) );
            if ( groupAVErrors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, groupAVErrors, showMessage: false, hasMultipleErrors: true );
            }
            return ImportAttributeValues( groupAVImports );
        }

        /// <summary>
        /// Processes the Group AttributeValue list.
        /// </summary>
        private int ImportGroupMembers()
        {
            this.ReportProgress( 0, "Preparing Group Member data for import..." );
            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Group Member Records...", this.GroupMemberCsvList.Count ) );

            var rockContext = new RockContext();
            var groupMemberLookup = this.GroupDict.SelectMany( g => g.Value.Members )
                                                        .Where( gm => !string.IsNullOrEmpty( gm.ForeignKey ) && gm.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                                        .Select( a => new
                                                        {
                                                            GroupMember = a,
                                                            a.ForeignKey
                                                        } )
                                                        .ToDictionary( k => k.ForeignKey, v => v.GroupMember );

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

            // Reload Group Dictionary to include all newly imported groupmembers
            LoadGroupDict();

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

                var newGroupMember = new GroupMemberImport()
                {
                    PersonId = person.Id,
                    GroupId = group.Id,
                    GroupTypeId = group.GroupTypeId,
                    RoleName = groupMemberCsv.Role,
                    GroupMemberStatus = groupMemberCsv.GroupMemberStatus,
                    GroupMemberForeignKey = groupMemberCsv.GroupMemberId.IsNotNullOrWhiteSpace() ? string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, groupMemberCsv.GroupMemberId ) : string.Format( "{0}^{1}_{2}", this.ImportInstanceFKPrefix, groupMemberCsv.GroupId, groupMemberCsv.PersonId )
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
                RoleNames = a.Select( x => x.RoleName ).Distinct().ToList()
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
                            Name = roleName.Left( 100 ),
                            CreatedDateTime = importedDateTime,
                            ModifiedDateTime = importedDateTime
                        };

                        groupTypeRolesToInsert.Add( newGroupTypeRole );
                    }
                }
            }

            var updatedGroupTypes = groupTypeRolesToInsert.Select( a => a.GroupTypeId.Value ).Distinct().ToList();
            updatedGroupTypes.ForEach( id => GroupTypeCache.UpdateCachedEntity( id, EntityState.Detached ) );

            if ( groupTypeRolesToInsert.Any() )
            {
                rockContext.BulkInsert( groupTypeRolesToInsert );
            }
        }
    }
}
