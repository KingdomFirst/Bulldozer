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
using System;
using System.Collections.Generic;
using System.Linq;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Prayer related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region Prayer Methods

        /// <summary>
        /// Loads the Prayer Requests data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadPrayerRequest( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedPrayerRequests = new PrayerRequestService( lookupContext ).Queryable().Count( p => p.ForeignKey != null && p.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) );

            var prayerRequestList = new List<PrayerRequest>();

            var completedItems = 0;
            var addedItems = 0;
            ReportProgress( 0, string.Format( "Verifying prayer request import ({0:N0} already imported).", importedPrayerRequests ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var prayerRequestCategory = row[PrayerRequestCategory] as string;
                var prayerRequestText = row[PrayerRequestText] as string;
                var prayerRequestDate = row[PrayerRequestDate] as string;
                var prayerRequestId = row[PrayerRequestId] as string;
                var prayerRequestFirstName = row[PrayerRequestFirstName] as string;
                var prayerRequestLastName = row[PrayerRequestLastName] as string;
                var prayerRequestEmail = row[PrayerRequestEmail] as string;
                var prayerRequestExpireDate = row[PrayerRequestExpireDate] as string;
                var prayerRequestAllowComments = ParseBoolOrDefault( row[PrayerRequestAllowComments], true );
                var prayerRequestIsPublic = ParseBoolOrDefault( row[PrayerRequestIsPublic], true );
                var prayerRequestIsApproved = ParseBoolOrDefault( row[PrayerRequestIsApproved], true );
                var prayerRequestApprovedDate = row[PrayerRequestApprovedDate] as string;
                var prayerRequestApprovedById = row[PrayerRequestApprovedById] as string;
                var prayerRequestCreatedById = row[PrayerRequestCreatedById] as string;
                var prayerRequestRequestedById = row[PrayerRequestRequestedById] as string;
                var prayerRequestAnswerText = row[PrayerRequestAnswerText] as string;
                var prayerRequestCampusName = row[PrayerRequestCampus] as string;

                if ( !string.IsNullOrWhiteSpace( prayerRequestText ) )
                {
                    var email = string.Empty;
                    if ( prayerRequestEmail.IsEmail() )
                    {
                        email = prayerRequestEmail;
                    }

                    int? approvedByAliasId = null;
                    var approvedByPersonKeys = GetPersonKeys( prayerRequestApprovedById );
                    if ( approvedByPersonKeys != null )
                    {
                        approvedByAliasId = approvedByPersonKeys.PersonAliasId;
                    }

                    int? createdByAliasId = null;
                    var createdByPersonKeys = GetPersonKeys( prayerRequestCreatedById );
                    if ( createdByPersonKeys != null )
                    {
                        createdByAliasId = createdByPersonKeys.PersonAliasId;
                    }

                    int? requestedByAliasId = null;
                    var requestedByPersonKeys = GetPersonKeys( prayerRequestRequestedById );
                    if ( requestedByPersonKeys != null )
                    {
                        requestedByAliasId = requestedByPersonKeys.PersonAliasId;
                    }

                    var prayerRequest = AddPrayerRequest( lookupContext, prayerRequestCategory, prayerRequestText, prayerRequestDate, prayerRequestId,
                        prayerRequestFirstName, prayerRequestLastName, email, prayerRequestExpireDate, prayerRequestAllowComments, prayerRequestIsPublic,
                        prayerRequestIsApproved, prayerRequestApprovedDate, approvedByAliasId, createdByAliasId, requestedByAliasId, prayerRequestAnswerText,
                        false );

                    if ( prayerRequest.Id == 0 )
                    {
                        Campus campus = null;
                        if ( prayerRequestCampusName.IsNotNullOrWhiteSpace() )
                        {
                            if ( UseExistingCampusIds )
                            {
                                campus = this.CampusesDict.Values.FirstOrDefault( c => c.Name.Equals( prayerRequestCampusName, StringComparison.OrdinalIgnoreCase )
                                            || ( c.ShortCode != null && c.ShortCode.Equals( prayerRequestCampusName, StringComparison.OrdinalIgnoreCase ) ) );
                            }
                            else
                            {
                                campus = this.CampusImportDict.Values.FirstOrDefault( c => c.Name.Equals( prayerRequestCampusName, StringComparison.OrdinalIgnoreCase )
                                            || ( c.ShortCode != null && c.ShortCode.Equals( prayerRequestCampusName, StringComparison.OrdinalIgnoreCase ) ) );
                            }

                            if ( campus == null && !UseExistingCampusIds )
                            {
                                campus = new Campus
                                {
                                    IsSystem = false,
                                    Name = prayerRequestCampusName,
                                    ShortCode = prayerRequestCampusName.RemoveWhitespace(),
                                    IsActive = true,
                                    ForeignKey = $"{this.ImportInstanceFKPrefix}^{prayerRequestCampusName}"
                                };
                                lookupContext.Campuses.Add( campus );
                                lookupContext.SaveChanges( DisableAuditing );
                                this.CampusImportDict.Add( campus.ForeignKey, campus );
                            }
                        }

                        prayerRequest.CampusId = campus?.Id;

                        prayerRequestList.Add( prayerRequest );
                        addedItems++;
                    }

                    completedItems++;
                    if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} prayer requests processed.", completedItems ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        SavePrayerRequests( prayerRequestList );
                        ReportPartialProgress();
                        prayerRequestList.Clear();
                    }
                }
            }

            if ( prayerRequestList.Any() )
            {
                SavePrayerRequests( prayerRequestList );
            }

            ReportProgress( 100, string.Format( "Finished prayer request import: {0:N0} prayer requests processed, {1:N0} imported.", completedItems, addedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the prayer requests.
        /// </summary>
        /// <param name="prayerRequestList">The prayer request list.</param>
        private static void SavePrayerRequests( List<PrayerRequest> prayerRequestList )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.PrayerRequests.AddRange( prayerRequestList );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }
    }

    #endregion
}
