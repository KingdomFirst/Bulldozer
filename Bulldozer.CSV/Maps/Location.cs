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
            int numImportedNamedLocations = LocationsDict.Count( c => c.Value.Name != null );
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
                if ( LocationsDict.Count() > 0 )
                {
                    locationExists = this.LocationsDict.ContainsKey( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowNamedLocationKey ) );
                }

                // Check if this was an existing location that needs foreign id added
                if ( !locationExists )
                {
                    var location = new LocationService( lookupContext ).Queryable().FirstOrDefault( l => ( l.ForeignKey == null || l.ForeignKey.Trim() == "" ) && l.Name.Equals( rowNamedLocationName, StringComparison.OrdinalIgnoreCase ) );
                    if ( location != null )
                    {
                        location.ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowNamedLocationKey );
                        location.ForeignId = rowNamedLocationKey.AsIntegerOrNull();
                        location.ForeignGuid = rowNamedLocationKey.AsGuidOrNull();

                        lookupContext.SaveChanges();
                        locationExists = true;
                        this.LocationsDict.Add( location.ForeignKey, location );
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
                    newLocation.ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowNamedLocationKey );
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
                        int? parentLocationId = this.LocationsDict.FirstOrDefault( l => l.Value.ForeignKey.Equals( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowNamedLocationParent ) ) || ( l.Value.Name != null && l.Value.Name.Equals( rowNamedLocationParent ) ) ).Value.Id;
                        newLocation.ParentLocationId = parentLocationId;
                    }

                    newNamedLocationList.Add( newLocation );
                    LocationsDict.Add( newLocation.ForeignKey, newLocation );

                    //
                    // Save Every Loop
                    //
                    SaveNamedLocation( newNamedLocationList );

                    newNamedLocationList.Clear();

                    //
                    // Keep the user informed as to what is going on and save in batches.
                    //
                    completed++;

                    if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} groups imported.", completed ) );
                    }

                    if ( completed % DefaultChunkSize < 1 )
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
        }

        public int ImportLocations()
        {
            this.ReportProgress( 0, "Preparing Location data for import..." );
            if ( this.LocationsDict == null )
            {
                LoadLocationDict();
            }

            var rockContext = new RockContext();
            var locationTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.LOCATION_TYPE ), rockContext ).DefinedValues;

            var locationService = new LocationService( rockContext );

            var importedDateTime = RockDateTime.Now;

            var locationsToInsert = new List<Location>();

            foreach ( var locationCsv in this.LocationCsvList )
            {
                var foreignKey = $"{this.ImportInstanceFKPrefix}^{locationCsv.Id}";
                if ( this.LocationsDict.ContainsKey( foreignKey ) )
                {
                    continue;
                }

                var newLocation = new Location
                {
                    Street1 = locationCsv.Street1.Left( 100 ),
                    Street2 = locationCsv.Street2.Left( 100 ),
                    City = locationCsv.City.Left( 50 ),
                    County = locationCsv.County.Left( 50 ),
                    State = locationCsv.State.Left( 50 ),
                    Country = locationCsv.Country.Left( 50 ),
                    PostalCode = locationCsv.PostalCode.Left( 50 ),
                    CreatedDateTime = importedDateTime,
                    ModifiedDateTime = importedDateTime,
                    ForeignKey = foreignKey,
                    ForeignId = locationCsv.Id.AsIntegerOrNull(),
                    IsActive = locationCsv.IsActive,
                    SoftRoomThreshold = locationCsv.SoftRoomThreshold,
                    FirmRoomThreshold = locationCsv.FirmRoomThreshold
                };

                if ( locationCsv.Name.IsNotNullOrWhiteSpace() )
                {
                    newLocation.Name = locationCsv.Name;
                }

                if ( locationCsv.LocationType.IsNotNullOrWhiteSpace() )
                {
                    var locationTypeId = locationTypes.Where( v => v.Value.Equals( locationCsv.LocationType ) || v.Id.Equals( locationCsv.LocationType ) || v.Guid.ToString().ToLower().Equals( locationCsv.LocationType.ToLower() ) )
                        .Select( v => ( int? ) v.Id ).FirstOrDefault();
                    newLocation.LocationTypeValueId = locationTypeId;
                }

                locationsToInsert.Add( newLocation );
            }

            this.ReportProgress( 0, "Begin processing Location records." );
            rockContext.BulkInsert( locationsToInsert );

            // Get the Location records for the locations that we imported so that we can populate the ParentLocations
            var locationLookup = locationService.Queryable()
                                                .Where( l => l.ForeignKey != null && l.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                .ToDictionary( k => k.ForeignKey, v => v );
            var locationsUpdated = false;
            foreach ( var locationCsv in this.LocationCsvList.Where( l => !string.IsNullOrWhiteSpace( l.ParentLocationId ) ) )
            {
                var foreignKey = $"{this.ImportInstanceFKPrefix}^{locationCsv.Id}";
                var location = locationLookup.GetValueOrNull( foreignKey );
                if ( location != null )
                {
                    var parentForeignKey = $"{this.ImportInstanceFKPrefix}^{locationCsv.ParentLocationId}";
                    var parentLocation = locationLookup.GetValueOrNull( parentForeignKey );
                    if ( parentLocation != null && location.ParentLocationId != parentLocation.Id )
                    {
                        location.ParentLocationId = parentLocation.Id;
                        locationsUpdated = true;
                    }
                }
            }

            if ( locationsUpdated )
            {
                rockContext.SaveChanges();
            }

            LoadLocationDict();

            return locationsToInsert.Count;
        }
    }
}