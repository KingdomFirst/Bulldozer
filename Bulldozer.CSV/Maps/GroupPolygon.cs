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
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Entity.Spatial;
using System.Linq;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the group import methods
    /// </summary>
    partial class CSVComponent
    {

        /// <summary>
        /// Loads the polygon group data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadGroupPolygon( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var numImportedGroups = ImportedGroups.Count();
            var newGroupLocations = new Dictionary<GroupLocation, string>();
            var currentGroup = new Group();
            var coordinateString = string.Empty;
            var startCoordinate = string.Empty;
            var endCoordinate = string.Empty;
            var geographicAreaTypeId = DefinedValueCache.Get( "44990C3F-C45B-EDA3-4B65-A238A581A26F" ).Id;

            var completed = 0;

            ReportProgress( 0, $"Starting polygon group import ({numImportedGroups:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowGroupKey = row[GroupId];
                var rowGroupId = rowGroupKey.AsType<int?>();
                var rowLat = row[Latitude];
                var rowLong = row[Longitude];
                var groupForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowGroupKey );
                string parentGroupForeignKey = null;
                if ( row[GroupParentGroupId].IsNotNullOrWhiteSpace() )
                {
                    parentGroupForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, row[GroupParentGroupId] );
                }

                //
                // Determine if we are still working with the same group or not.
                //
                if ( !string.IsNullOrWhiteSpace( rowGroupKey ) && groupForeignKey != currentGroup.ForeignKey )
                {
                    if ( !string.IsNullOrWhiteSpace( coordinateString ) )
                    {
                        if ( startCoordinate != endCoordinate )
                        {
                            coordinateString = $"{coordinateString}|{startCoordinate}";
                        }

                        var coords = coordinateString.Split( '|' );
                        if ( coords.Length > 3 )
                        {
                            var polygon = CreatePolygonLocation( coordinateString, row[GroupCreatedDate], currentGroup.ForeignKey, rowGroupId );

                            if ( polygon != null )
                            {
                                var geographicArea = new GroupLocation
                                {
                                    LocationId = polygon.Id,
                                    IsMailingLocation = true,
                                    IsMappedLocation = true,
                                    GroupLocationTypeValueId = geographicAreaTypeId,
                                    GroupId = currentGroup.Id
                                };
                                newGroupLocations.Add( geographicArea, currentGroup.ForeignKey );
                            }
                        }
                    }

                    currentGroup = LoadGroupBasic( lookupContext, groupForeignKey, row[GroupName], row[GroupCreatedDate], row[GroupType], parentGroupForeignKey, row[GroupActive] );

                    // reset coordinateString
                    coordinateString = string.Empty;

                    if ( !string.IsNullOrWhiteSpace( rowLat ) && !string.IsNullOrWhiteSpace( rowLong ) && rowLat.AsType<double>() != 0 && rowLong.AsType<double>() != 0 )
                    {
                        coordinateString = $"{rowLat},{rowLong}";
                        startCoordinate = $"{rowLat},{rowLong}";
                    }

                    //
                    // Set the group campus
                    //
                    var campusName = row[GroupCampus];
                    Campus groupCampus = null;
                    if ( !string.IsNullOrWhiteSpace( campusName ) )
                    {
                        if ( UseExistingCampusIds )
                        {
                            groupCampus = this.CampusesDict.Values.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.OrdinalIgnoreCase )
                                        || ( c.ShortCode != null && c.ShortCode.Equals( campusName, StringComparison.OrdinalIgnoreCase ) ) );
                        }
                        else
                        {
                            groupCampus = this.CampusImportDict.Values.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.OrdinalIgnoreCase )
                                        || ( c.ShortCode != null && c.ShortCode.Equals( campusName, StringComparison.OrdinalIgnoreCase ) ) );
                        }
                        if ( groupCampus == null && !UseExistingCampusIds )
                        {
                            groupCampus = new Campus
                            {
                                IsSystem = false,
                                Name = campusName,
                                ShortCode = campusName.RemoveWhitespace(),
                                IsActive = true,
                                ForeignKey = $"{this.ImportInstanceFKPrefix}^{campusName}"
                            };
                            lookupContext.Campuses.Add( groupCampus );
                            lookupContext.SaveChanges( DisableAuditing );
                            this.CampusImportDict.Add( groupCampus.ForeignKey, groupCampus );
                        }

                        currentGroup.CampusId = groupCampus?.Id;
                    }

                    //
                    // Set the group's sorting order.
                    //
                    var groupOrder = 9999;
                    int.TryParse( row[GroupOrder], out groupOrder );
                    currentGroup.Order = groupOrder;

                    //
                    // Changes to groups need to be saved right away since one group
                    // will reference another group.
                    //
                    lookupContext.SaveChanges();

                    completed++;

                    if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} groups imported." );
                    }

                    if ( completed % DefaultChunkSize < 1 )
                    {
                        ReportPartialProgress();
                    }
                }
                else if ( groupForeignKey == currentGroup.ForeignKey && ( !string.IsNullOrWhiteSpace( rowLat ) && !string.IsNullOrWhiteSpace( rowLong ) && rowLat.AsType<double>() != 0 && rowLong.AsType<double>() != 0 ) )
                {
                    coordinateString = $"{coordinateString}|{rowLat},{rowLong}";
                    endCoordinate = $"{rowLat},{rowLong}";
                }
            }

            if ( !string.IsNullOrWhiteSpace( coordinateString ) )
            {
                if ( startCoordinate != endCoordinate )
                {
                    coordinateString = coordinateString + $"|{startCoordinate}";
                }

                var coords = coordinateString.Split( '|' );
                if ( coords.Length > 3 )
                {
                    var polygon = CreatePolygonLocation( coordinateString, currentGroup.CreatedDateTime.ToString(), currentGroup.ForeignKey, currentGroup.ForeignId );

                    if ( polygon != null )
                    {
                        var geographicArea = new GroupLocation
                        {
                            LocationId = polygon.Id,
                            IsMailingLocation = true,
                            IsMappedLocation = true,
                            GroupLocationTypeValueId = geographicAreaTypeId,
                            GroupId = currentGroup.Id
                        };
                        newGroupLocations.Add( geographicArea, currentGroup.ForeignKey );
                    }
                }
            }

            //
            // Save rows to the database
            //
            ReportProgress( 0, $"Saving {newGroupLocations.Count} polygons." );
            if ( newGroupLocations.Any() )
            {
                SaveGroupLocations( newGroupLocations );
            }

            lookupContext.SaveChanges();
            DetachAllInContext( lookupContext );
            lookupContext.Dispose();

            LoadGroupDict();

            ReportProgress( 0, $"Finished polygon group import: {completed:N0} groups added or updated." );

            return completed;
        }

        /// <summary>
        /// Load in the basic group information passed in by the caller. Group is not saved
        /// unless the caller explecitely save the group.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        /// <param name="groupForeignKey">The group key.</param>
        /// <param name="name">The name.</param>
        /// <param name="createdDate">The created date.</param>
        /// <param name="type">The type.</param>
        /// <param name="parentGroupKey">The parent group key.</param>
        /// <param name="active">The active.</param>
        /// <returns></returns>
        private Group LoadGroupBasic( RockContext lookupContext, string groupForeignKey, string name, string createdDate, string type, string parentGroupForeignKey, string active, string description = "" )
        {
            var groupTypeId = LoadGroupTypeId( lookupContext, type, this.ImportInstanceFKPrefix, true ).Value;
            var groupId = groupForeignKey.AsType<int?>();
            Group group, parent;

            //
            // See if we have already imported it previously. Otherwise
            // create it as a new group.
            //
            group = ImportedGroups.FirstOrDefault( g => g.ForeignKey == groupForeignKey );

            // Check if this was an existing group that needs foreign id added
            if ( group == null )
            {
                var parentGroupId = ImportedGroups.FirstOrDefault( g => g.ForeignKey == parentGroupForeignKey )?.Id;
                group = new GroupService( lookupContext ).Queryable().Where( g => g.ForeignKey == null && g.GroupTypeId == groupTypeId && g.Name.Equals( name, StringComparison.OrdinalIgnoreCase ) && g.ParentGroupId == parentGroupId ).FirstOrDefault();
            }

            if ( group == null )
            {
                group = new Group
                {
                    ForeignKey = groupForeignKey,
                    ForeignId = groupId,
                    Name = name,
                    CreatedByPersonAliasId = ImportPersonAliasId,
                    GroupTypeId = groupTypeId,
                    Description = description
                };

                lookupContext.Groups.Add( group );
                ImportedGroups.Add( group );
            }
            else
            {
                if ( string.IsNullOrWhiteSpace( group.ForeignKey ) )
                {
                    group.ForeignKey = groupForeignKey;
                    group.ForeignId = groupId;

                    if ( !ImportedGroups.Any( g => g.ForeignKey.Equals( groupForeignKey, StringComparison.OrdinalIgnoreCase ) ) )
                    {
                        ImportedGroups.Add( group );
                    }
                }

                lookupContext.Groups.Attach( group );
                lookupContext.Entry( group ).State = EntityState.Modified;
            }

            //
            // Find and set the parent group. If not found it becomes a root level group.
            //
            parent = ImportedGroups.FirstOrDefault( g => g.ForeignKey == parentGroupForeignKey );
            if ( parent != null )
            {
                group.ParentGroupId = parent.Id;
            }

            //
            // Setup the date created/modified values from the data, if we have them.
            //
            group.CreatedDateTime = ParseDateOrDefault( createdDate, ImportDateTime );
            group.ModifiedDateTime = ImportDateTime;

            //
            // Set the active state of this group.
            //
            if ( active.ToUpper() == "NO" )
            {
                group.IsActive = false;
            }
            else
            {
                group.IsActive = true;
            }

            return group;
        }

        /// <summary>
        /// Saves all group locations.
        /// </summary>
        /// <param name="newGroupLocations">The new group locations.</param>
        private void SaveGroupLocations( Dictionary<GroupLocation, string> newGroupLocations )
        {
            var rockContext = new RockContext();

            //
            // Now save any new locations
            //
            if ( newGroupLocations.Any() )
            {
                //
                // Match up the new, real, group Id for each location.
                //
                foreach ( var locationPair in newGroupLocations )
                {
                    var groupId = ImportedGroups.Where( g => g.ForeignKey == locationPair.Value ).Select( g => ( int? ) g.Id ).FirstOrDefault();
                    if ( groupId != null )
                    {
                        locationPair.Key.GroupId = ( int ) groupId;
                    }
                }

                //
                // Save locations to the database
                //
                rockContext.WrapTransaction( () =>
                {
                    rockContext.Configuration.AutoDetectChangesEnabled = false;
                    rockContext.GroupLocations.AddRange( newGroupLocations.Keys );
                    rockContext.ChangeTracker.DetectChanges();
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }

        /// <summary>
        /// Creates a new polygon location.
        /// </summary>
        /// <param name="coordinateString">String that contains the shapes. Should be formatted as: lat1,long1|lat2,long2|...</param>
        /// <param name="rowGroupCreatedDate">string to use as the CreatedDate.</param>
        /// <param name="groupForeignKey">String to use as the ForeignKey.</param>
        /// <param name="rowGroupId">Int to use as the ForeignId.</param>
        /// <returns></returns>
        private static Location CreatePolygonLocation( string coordinateString, string rowGroupCreatedDate, string groupForeignKey, int? rowGroupId )
        {
            var rockContext = new RockContext();
            var newPolygonList = new List<Location>();

            var polygon = new Location
            {
                GeoFence = DbGeography.PolygonFromText( Rock.Web.UI.Controls.GeoPicker.ConvertPolyToWellKnownText( coordinateString ), 4326 ),
                CreatedDateTime = ParseDateOrDefault( rowGroupCreatedDate, ImportDateTime ),
                ModifiedDateTime = ImportDateTime,
                CreatedByPersonAliasId = ImportPersonAliasId,
                ModifiedByPersonAliasId = ImportPersonAliasId,
                ForeignKey = groupForeignKey,
                ForeignId = rowGroupId
            };

            newPolygonList.Add( polygon );

            rockContext.WrapTransaction( () =>
            {
                rockContext.Locations.AddRange( newPolygonList );
                rockContext.SaveChanges( DisableAuditing );
            } );

            return polygon;
        }

    }
}