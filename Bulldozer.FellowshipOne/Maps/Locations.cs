// <copyright>
// Copyright 2019 by Kingdom First Solutions
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
using OrcaMDF.Core.MetaData;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.F1
{
    public partial class F1Component
    {
        /// <summary>
        /// Maps the family address.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private void MapFamilyAddress( IQueryable<Row> tableData, long totalRows = 0 )
        {
            var lookupContext = new RockContext(); 
            var locationService = new LocationService( lookupContext );

            var familyGroupMemberList = new GroupMemberService( lookupContext ).Queryable( true ).AsNoTracking()
                .Where( gm => gm.Group.GroupType.Guid.Equals( new Guid( Rock.SystemGuid.GroupType.GROUPTYPE_FAMILY ) ) ).ToList();

            var customLocationTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE ), lookupContext ).DefinedValues;

            var newGroupLocations = new List<GroupLocation>();

            if ( totalRows == 0 )
            {
                totalRows = tableData.Count();
            }

            var completed = 0;
            var percentage = ( totalRows - 1 ) / 100 + 1;
            ReportProgress( 0, $"Verifying address import ({totalRows:N0} found)." );

            foreach ( var row in tableData.Where( r => r != null ) )
            {
                var individualId = row["Individual_ID"] as int?;
                var householdId = row["Household_ID"] as int?;
                var personKeys = GetPersonKeys( individualId, householdId, includeVisitors: false );
                if ( personKeys != null )
                {
                    var familyGroup = familyGroupMemberList.Where( gm => gm.PersonId == personKeys.PersonId )
                        .Select( gm => gm.Group ).FirstOrDefault();

                    if ( familyGroup != null )
                    {
                        var groupLocation = new GroupLocation();

                        var street1 = row["Address_1"] as string;
                        var street2 = row["Address_2"] as string;
                        var city = row["City"] as string;
                        var state = row["State"] as string;
                        var country = row["country"] as string; // NOT A TYPO: F1 has property in lower-case
                        var zip = row["Postal_Code"] as string ?? string.Empty;

                        // restrict zip to 5 places to prevent duplicates

                        Location familyAddress = GetOrAddLocation( lookupContext, street1, street2, city, state, zip.Left( 5 ), country );

                        if ( familyAddress != null && !familyGroup.GroupLocations.Any( gl => gl.LocationId == familyAddress.Id ) )
                        {
                            familyAddress.CreatedByPersonAliasId = ImportPersonAliasId;
                            familyAddress.IsActive = true;

                            groupLocation.GroupId = familyGroup.Id;
                            groupLocation.LocationId = familyAddress.Id;
                            groupLocation.IsMailingLocation = false;
                            groupLocation.IsMappedLocation = false;

                            var addressType = row["Address_Type"].ToString();
                            if ( addressType.Equals( "Primary", StringComparison.OrdinalIgnoreCase ) )
                            {
                                groupLocation.GroupLocationTypeValueId = HomeLocationTypeId;
                                groupLocation.IsMailingLocation = true;
                                groupLocation.IsMappedLocation = true;
                            }
                            else if ( addressType.Equals( "Business", StringComparison.OrdinalIgnoreCase ) || addressType.StartsWith( "Org", StringComparison.OrdinalIgnoreCase ) )
                            {
                                groupLocation.GroupLocationTypeValueId = WorkLocationTypeId;
                            }
                            else if ( addressType.Equals( "Previous", StringComparison.OrdinalIgnoreCase ) )
                            {
                                groupLocation.GroupLocationTypeValueId = PreviousLocationTypeId;
                            }
                            else if ( !string.IsNullOrWhiteSpace( addressType ) )
                            {
                                // look for existing group location types, otherwise add a new type
                                var customTypeId = customLocationTypes.Where( dv => dv.Value.Equals( addressType, StringComparison.OrdinalIgnoreCase ) )
                                    .Select( dv => ( int? ) dv.Id ).FirstOrDefault();

                                if ( !customTypeId.HasValue )
                                {
                                    var newLocationType = AddDefinedValue( lookupContext, Rock.SystemGuid.DefinedType.GROUP_LOCATION_TYPE, addressType );
                                    if ( newLocationType != null )
                                    {
                                        customLocationTypes.Add( newLocationType );
                                        customTypeId = newLocationType.Id;

                                        // add to the family group type
                                        var groupTypeLocationType = new GroupTypeLocationType
                                        {
                                            GroupTypeId = GroupTypeCache.GetFamilyGroupType().Id,
                                            LocationTypeValueId = newLocationType.Id
                                        };
                                        lookupContext.GroupTypeLocationTypes.Add( groupTypeLocationType );
                                        lookupContext.SaveChanges( DisableAuditing );
                                    }
                                }

                                groupLocation.GroupLocationTypeValueId = customTypeId;
                            }

                            familyGroup.GroupLocations.Add( groupLocation );
                            newGroupLocations.Add( groupLocation );
                            completed++;

                            if ( completed % percentage < 1 )
                            {
                                var percentComplete = completed / percentage;
                                ReportProgress( percentComplete, $"{completed:N0} addresses imported ({percentComplete}% complete)." );
                            }

                            if ( completed % ReportingNumber < 1 )
                            {
                                SaveFamilyAddress( newGroupLocations );

                                // Reset context
                                newGroupLocations.Clear();
                                lookupContext = new RockContext();
                                locationService = new LocationService( lookupContext );

                                ReportPartialProgress();
                            }
                        }
                        else if ( !string.IsNullOrWhiteSpace( street1 ) || !string.IsNullOrWhiteSpace( city ) || !string.IsNullOrWhiteSpace( state ) )
                        {
                            var missingAddrParts = new List<string>();
                            if ( string.IsNullOrWhiteSpace( street1 ) )
                            {
                                missingAddrParts.Add( "Address" );
                            }
                            if ( string.IsNullOrWhiteSpace( city ) )
                            {
                                missingAddrParts.Add( "City" );
                            }
                            if ( string.IsNullOrWhiteSpace( state ) )
                            {
                                missingAddrParts.Add( "State/Province" );
                            }
                            LogException( "Invalid Primary Address", string.Format( "FamilyId: {0} - Missing {1}. Address not imported.", householdId, string.Join( ", ", missingAddrParts ) ) );
                        }
                    }
                }
            }

            if ( newGroupLocations.Any() )
            {
                SaveFamilyAddress( newGroupLocations );
            }

            ReportProgress( 100, $"Finished address import: {completed:N0} addresses imported." );
        }

        /// <summary>
        /// Saves the family address.
        /// </summary>
        /// <param name="newGroupLocations">The new group locations.</param>
        private static void SaveFamilyAddress( List<GroupLocation> newGroupLocations )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.BulkInsert( newGroupLocations );
            }
        }
    }
}