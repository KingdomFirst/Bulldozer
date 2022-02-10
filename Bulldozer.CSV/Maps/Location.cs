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
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the location import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the named location data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadNamedLocation( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var locationTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.LOCATION_TYPE ), lookupContext ).DefinedValues;
            int numImportedNamedLocations = ImportedLocations.Count( c => c.Name != null );
            var newNamedLocationList = new List<Location>();

            int completed = 0;
            ReportProgress( 0, string.Format( "Starting named location import ({0:N0} already exist).", numImportedNamedLocations ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                string rowNamedLocationName = row[NamedLocationName];
                string rowNamedLocationKey = row[NamedLocationId];

                // Check that this location isn't already in our data
                bool locationExists = false;
                if ( ImportedLocations.Count() > 0 )
                {
                    locationExists = ImportedLocations.Any( l => l.ForeignKey.Equals( rowNamedLocationKey ) );
                }

                // Check if this was an existing location that needs foreign id added
                if ( !locationExists )
                {
                    var location = new LocationService( lookupContext ).Queryable().FirstOrDefault( l => ( l.ForeignKey == null || l.ForeignKey.Trim() == "" ) && l.Name.Equals( rowNamedLocationName, StringComparison.OrdinalIgnoreCase ) );
                    if ( location != null )
                    {
                        location.ForeignKey = rowNamedLocationKey;
                        location.ForeignId = rowNamedLocationKey.AsIntegerOrNull();
                        location.ForeignGuid = rowNamedLocationKey.AsGuidOrNull();

                        lookupContext.SaveChanges();
                        locationExists = true;
                        ImportedLocations.Add( location );
                        completed++;
                    }
                }

                if ( !string.IsNullOrWhiteSpace( rowNamedLocationKey ) && !locationExists )
                {
                    string rowNamedLocationCreatedDate = row[NamedLocationCreatedDate];
                    string rowNamedLocationType = row[NamedLocationType];
                    string rowNamedLocationParent = row[NamedLocationParent];
                    string rowNamedLocationSoftRoomThreshold = row[NamedLocationSoftRoomThreshold];
                    string rowNamedLocationFirmRoomThreshold = row[NamedLocationFirmRoomThreshold];
                    int? rowSoftThreshold = rowNamedLocationSoftRoomThreshold.AsType<int?>();
                    int? rowFirmThreshold = rowNamedLocationFirmRoomThreshold.AsType<int?>();
                    int? rowNamedLocationId = rowNamedLocationKey.AsType<int?>();

                    Location newLocation = new Location();
                    newLocation.Name = rowNamedLocationName;
                    newLocation.CreatedDateTime = ParseDateOrDefault( rowNamedLocationCreatedDate, ImportDateTime );
                    newLocation.ModifiedDateTime = ImportDateTime;
                    newLocation.CreatedByPersonAliasId = ImportPersonAliasId;
                    newLocation.ModifiedByPersonAliasId = ImportPersonAliasId;
                    newLocation.ForeignKey = rowNamedLocationKey;
                    newLocation.ForeignId = rowNamedLocationId;

                    if ( rowSoftThreshold != null && rowSoftThreshold > 0 )
                    {
                        newLocation.SoftRoomThreshold = rowSoftThreshold;
                    }

                    if ( rowFirmThreshold != null && rowFirmThreshold > 0 )
                    {
                        newLocation.FirmRoomThreshold = rowFirmThreshold;
                    }

                    if ( !string.IsNullOrWhiteSpace( rowNamedLocationType ) )
                    {
                        var locationTypeId = locationTypes.Where( v => v.Value.Equals( rowNamedLocationType ) || v.Id.Equals( rowNamedLocationType ) || v.Guid.ToString().ToLower().Equals( rowNamedLocationType.ToLower() ) )
                            .Select( v => ( int? ) v.Id ).FirstOrDefault();
                        newLocation.LocationTypeValueId = locationTypeId;
                    }

                    if ( !string.IsNullOrWhiteSpace( rowNamedLocationParent ) )
                    {
                        int? parentLocationId = ImportedLocations.FirstOrDefault( l => l.ForeignKey.Equals( rowNamedLocationParent ) || ( l.Name != null && l.Name.Equals( rowNamedLocationParent ) ) ).Id;
                        newLocation.ParentLocationId = parentLocationId;
                    }

                    newNamedLocationList.Add( newLocation );

                    //
                    // Save Every Loop
                    //
                    SaveNamedLocation( newNamedLocationList );

                    newNamedLocationList.Clear();

                    //
                    // Keep the user informed as to what is going on and save in batches.
                    //
                    completed++;

                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} groups imported.", completed ) );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        ReportPartialProgress();
                    }
                }
            }

            //
            // Check to see if any rows didn't get saved to the database
            //
            if ( newNamedLocationList.Any() )
            {
                SaveNamedLocation( newNamedLocationList );
            }

            lookupContext.SaveChanges();
            DetachAllInContext( lookupContext );
            lookupContext.Dispose();

            ReportProgress( 0, string.Format( "Finished named location import: {0:N0} named locations added or updated.", completed ) );

            return completed;
        }

        /// <summary>
        /// Saves all named location changes.
        /// </summary>
        private void SaveNamedLocation( List<Location> newNamedLocationList )
        {
            var rockContext = new RockContext();

            //
            // First save any unsaved locations
            //
            if ( newNamedLocationList.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.Locations.AddRange( newNamedLocationList );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }

            //
            // Add these new locations to the global list
            //
            ImportedLocations.AddRange( newNamedLocationList );
        }
    }
}