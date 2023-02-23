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
using System.Globalization;
using System.Linq;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the family import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the family data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadFamily( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var locationService = new LocationService( lookupContext );

            var newGroupLocations = new Dictionary<GroupLocation, string>();
            var currentFamilyGroup = new Group();
            var newFamilyList = new List<Group>();
            var updatedFamilyList = new List<Group>();

            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy" };

            var currentFamilyKey = string.Empty;
            var completed = 0;

            ReportProgress( 0, $"Starting family import ({ImportedFamilies.Count():N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowFamilyKey = row[FamilyId];
                var rowFamilyId = rowFamilyKey.AsType<int?>();
                var rowFamilyName = row[FamilyName];

                if ( rowFamilyKey != null && rowFamilyKey != currentFamilyGroup.ForeignKey )
                {
                    currentFamilyGroup = ImportedFamilies.FirstOrDefault( g => g.ForeignKey == rowFamilyKey );
                    if ( currentFamilyGroup == null )
                    {
                        currentFamilyGroup = new Group
                        {
                            ForeignKey = rowFamilyKey,
                            ForeignId = rowFamilyId,
                            CreatedByPersonAliasId = ImportPersonAliasId,
                            GroupTypeId = FamilyGroupTypeId
                        };
                        newFamilyList.Add( currentFamilyGroup );
                    }
                    else if ( !lookupContext.ChangeTracker.Entries<Group>().Any( g => g.Entity.ForeignKey == rowFamilyKey || ( g.Entity.ForeignKey == null && g.Entity.ForeignId == rowFamilyId ) ) )
                    {
                        // track changes if not currently tracking
                        lookupContext.Groups.Attach( currentFamilyGroup );
                    }

                    currentFamilyGroup.Name = row[FamilyName];

                    // Set the family campus
                    var campusName = row[Campus];
                    if ( !string.IsNullOrWhiteSpace( campusName ) )
                    {
                        var familyCampus = CampusList.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.OrdinalIgnoreCase )
                            || c.ShortCode.Equals( campusName, StringComparison.OrdinalIgnoreCase ) );
                        if ( familyCampus == null )
                        {
                            familyCampus = new Campus
                            {
                                IsSystem = false,
                                Name = campusName,
                                ShortCode = campusName.RemoveWhitespace(),
                                IsActive = true
                            };
                            lookupContext.Campuses.Add( familyCampus );
                            lookupContext.SaveChanges( DisableAuditing );
                            CampusList.Add( familyCampus );
                        }

                        currentFamilyGroup.CampusId = familyCampus.Id;
                        lookupContext.SaveChanges( DisableAuditing );
                    }

                    // Add the family addresses since they exist in this file
                    var famAddress = row[Address];
                    var famAddress2 = row[Address2];
                    var famCity = row[City];
                    var famState = row[State];
                    var famZip = row[Zip];
                    var famCountry = row[Country];

                    try
                    {
                        Location primaryAddress = GetOrAddLocation( lookupContext, famAddress, famAddress2, famCity, famState, famZip, famCountry );

                        if ( primaryAddress != null && currentFamilyGroup.GroupLocations.Count == 0 )
                        {
                            var primaryLocation = new GroupLocation
                            {
                                LocationId = primaryAddress.Id,
                                IsMailingLocation = true,
                                IsMappedLocation = true,
                                GroupLocationTypeValueId = HomeLocationTypeId
                            };
                            newGroupLocations.Add( primaryLocation, rowFamilyKey );
                        }
                    }
                    catch ( Exception ex )
                    {
                        LogException( "Invalid Primary Address", string.Format( "Error Importing Primary Address for FamilyId: {0}. {1}", rowFamilyKey, ex.Message ) );
                    }

                    var famSecondAddress = row[SecondaryAddress];
                    var famSecondAddress2 = row[SecondaryAddress2];
                    var famSecondCity = row[SecondaryCity];
                    var famSecondState = row[SecondaryState];
                    var famSecondZip = row[SecondaryZip];
                    var famSecondCountry = row[SecondaryCountry];

                    try
                    {
                        Location secondaryAddress = GetOrAddLocation( lookupContext, famSecondAddress, famSecondAddress2, famSecondCity, famSecondState, famSecondZip, famSecondCountry );

                        if ( secondaryAddress != null && currentFamilyGroup.GroupLocations.Count < 2 )
                        {
                            var secondaryLocation = new GroupLocation
                            {
                                LocationId = secondaryAddress.Id,
                                IsMailingLocation = true,
                                IsMappedLocation = false,
                                GroupLocationTypeValueId = PreviousLocationTypeId
                            };
                            newGroupLocations.Add( secondaryLocation, rowFamilyKey );
                        }
                    }
                    catch ( Exception ex )
                    {
                        LogException( "Invalid Secondary Address", string.Format( "Error Importing Secondary Address for FamilyId: {0}. {1}", rowFamilyKey, ex.Message ) );
                    }

                    DateTime createdDateValue;
                    if ( DateTime.TryParseExact( row[CreatedDate], dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out createdDateValue ) )
                    {
                        currentFamilyGroup.CreatedDateTime = createdDateValue;
                        currentFamilyGroup.ModifiedDateTime = ImportDateTime;
                    }
                    else
                    {
                        currentFamilyGroup.CreatedDateTime = ImportDateTime;
                        currentFamilyGroup.ModifiedDateTime = ImportDateTime;
                    }

                    completed++;
                    if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} families imported." );
                    }

                    if ( completed % DefaultChunkSize < 1 )
                    {
                        SaveFamilies( newFamilyList, newGroupLocations );
                        ReportPartialProgress();

                        // Reset lookup context
                        lookupContext.SaveChanges();
                        lookupContext.Dispose();
                        lookupContext = new RockContext();
                        locationService = new LocationService( lookupContext );
                        newFamilyList.Clear();
                        newGroupLocations.Clear();
                    }
                }
            }

            // Check to see if any rows didn't get saved to the database
            if ( newGroupLocations.Any() )
            {
                SaveFamilies( newFamilyList, newGroupLocations );
            }

            lookupContext.SaveChanges();
            DetachAllInContext( lookupContext );
            lookupContext.Dispose();

            ReportProgress( 0, $"Finished family import: {completed:N0} families added or updated." );
            return completed;
        }

        /// <summary>
        /// Saves all family changes.
        /// </summary>
        /// <param name="newFamilyList">The new family list.</param>
        /// <param name="newGroupLocations">The new group locations.</param>
        private void SaveFamilies( List<Group> newFamilyList, Dictionary<GroupLocation, string> newGroupLocations )
        {
            var rockContext = new RockContext();

            // First save any unsaved families
            if ( newFamilyList.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.Groups.AddRange( newFamilyList );
                    rockContext.SaveChanges( DisableAuditing );
                } );

                // Add these new families to the global list
                ImportedFamilies.AddRange( newFamilyList );
            }

            // Now save locations
            if ( newGroupLocations.Any() )
            {
                // Set updated family id on locations
                foreach ( var locationPair in newGroupLocations )
                {
                    var familyGroupId = ImportedFamilies.Where( g => g.ForeignKey == locationPair.Value ).Select( g => ( int? ) g.Id ).FirstOrDefault();
                    if ( familyGroupId != null )
                    {
                        locationPair.Key.GroupId = ( int ) familyGroupId;
                    }
                }

                // Save locations
                rockContext.WrapTransaction( () =>
                {
                    rockContext.GroupLocations.AddRange( newGroupLocations.Keys );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }
    }
}