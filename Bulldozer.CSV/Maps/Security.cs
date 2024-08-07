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
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Linq;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Security related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region UserLogin Methods

        /// <summary>
        /// Loads the UserLogin data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadUserLogin( CSVInstance csvData )
        {
            if ( this.UserLoginDict == null )
            {
                LoadUserLoginDict();
            }
            var lookupContext = new RockContext();

            var newUserLoginList = new List<UserLogin>();
            var existingUserNames = new UserLoginService( lookupContext ).Queryable().Select( l => l.UserName );

            int completed = 0;
            int importedCount = 0;
            int alreadyImportedCount = this.UserLoginDict.Count;
            ReportProgress( 0, string.Format( "Starting user login import ({0:N0} previously imported).", alreadyImportedCount ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                string rowUserLoginId = row[UserLoginId];
                string rowUserLoginPersonId = row[UserLoginPersonId];
                string rowUserLoginUserName = row[UserLoginUserName];
                string rowUserLoginPassword = row[UserLoginPassword];
                string rowUserLoginDateCreated = row[UserLoginDateCreated];
                string rowUserLoginAuthenticationType = row[UserLoginAuthenticationType];
                string rowUserLoginIsConfirmed = row[UserLoginIsConfirmed];

                int? rowLoginId = rowUserLoginId.AsType<int?>();
                int authenticationTypeId = EntityTypeCache.Get( rowUserLoginAuthenticationType ).Id;

                //
                // Find this person in the database.
                //
                var personKeys = GetPersonKeys( rowUserLoginPersonId );
                if ( personKeys == null || personKeys.PersonId == 0 )
                {
                    ReportProgress( 0, string.Format( "Person key {0} not found", rowUserLoginPersonId ) );
                }

                //
                // Verify the authentication type exists.
                //
                if ( authenticationTypeId < 1 )
                {
                    ReportProgress( 0, string.Format( "Authentication type {0} not found", rowUserLoginAuthenticationType ) );
                }

                //
                // Check that this user login record doesn't already exist.
                //
                bool exists = this.UserLoginDict.ContainsKey( $"{this.ImportInstanceFKPrefix}^{rowUserLoginId}" );

                if ( exists == false )
                {
                    exists = existingUserNames.Any( a => a.ToLower() == rowUserLoginUserName.ToLower() );
                }

                if ( !exists && personKeys != null && personKeys.PersonId != 0 && authenticationTypeId > 0 )
                {
                    //
                    // Create and populate the new user login record.
                    //
                    UserLogin login = new UserLogin();
                    login.CreatedDateTime = ParseDateOrDefault( rowUserLoginDateCreated, DateTime.Now );
                    login.CreatedByPersonAliasId = ImportPersonAliasId;
                    login.EntityTypeId = authenticationTypeId;
                    login.IsConfirmed = ParseBoolOrDefault( rowUserLoginIsConfirmed, true );
                    login.UserName = rowUserLoginUserName;
                    login.Password = rowUserLoginPassword;
                    login.PersonId = personKeys.PersonId;
                    login.ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowUserLoginId );
                    login.ForeignId = rowLoginId;

                    //
                    // Force not confirmed if no password provided for database logins.
                    //
                    if ( rowUserLoginAuthenticationType == "Rock.Security.Authentication.Database" && string.IsNullOrWhiteSpace( rowUserLoginPassword ) )
                    {
                        login.IsConfirmed = false;
                    }

                    //
                    // Add the record for delayed saving.
                    //
                    newUserLoginList.Add( login );
                    importedCount++;
                }

                //
                // Notify user of our status.
                //
                completed++;
                if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} user login records processed, {1:N0} imported.", completed, importedCount ) );
                }

                if ( completed % DefaultChunkSize < 1 )
                {
                    SaveUserLogin( newUserLoginList );
                    lookupContext.SaveChanges();
                    ReportPartialProgress();

                    // Clear out variables
                    newUserLoginList.Clear();
                    existingUserNames = new UserLoginService( lookupContext ).Queryable().Select( l => l.UserName );
                }
            }

            //
            // Save any final changes to new records
            //
            if ( newUserLoginList.Any() )
            {
                SaveUserLogin( newUserLoginList );
            }

            //
            // Save any other changes to existing items.
            //
            lookupContext.SaveChanges();
            lookupContext.Dispose();
            LoadUserLoginDict();

            ReportProgress( 0, string.Format( "Finished user login import: {0:N0} records added.", importedCount ) );

            return completed;
        }

        /// <summary>
        /// Saves all user login changes.
        /// </summary>
        private void SaveUserLogin( List<UserLogin> userLoginList )
        {
            var rockContext = new RockContext();

            //
            // Save any records
            //
            if ( userLoginList.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.UserLogins.AddRange( userLoginList );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }

        #endregion UserLogin Methods
    }
}
