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
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Financial import methods
    /// </summary>
    public partial class CSVComponent
    {
        /// <summary>
        /// Maps the account data.
        /// </summary>
        /// <param name="csvData">The table data.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        private int MapAccount( CSVInstance csvData )
        {
            var lookupContext = new RockContext();

            if ( ImportedAccounts == null )
            {
                LoadImportedAccounts( lookupContext );
            }

            // Look for custom attributes in the Account file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > FinancialFundCampusName )
                .ToDictionary( f => f.index, f => f.node.Name );

            var completed = 0;
            ReportProgress( 0, $"Verifying account import ({ImportedAccounts.Count:N0} already exist)." );
            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var fundIdKey = row[FinancialFundID];
                var fundId = fundIdKey.AsType<int?>();
                var fundName = row[FinancialFundName] as string;
                var fundDescription = row[FinancialFundDescription] as string;
                var fundGLAccount = row[FinancialFundGLAccount] as string;
                var isFundActive = ParseBoolOrDefault( row[FinancialFundIsActive], null );
                var fundStartDateKey = row[FinancialFundStartDate] as string;
                var fundStartDate = fundStartDateKey.AsType<DateTime?>();
                var fundEndDateKey = row[FinancialFundEndDate] as string;
                var fundEndDate = fundEndDateKey.AsType<DateTime?>();
                var fundOrderKey = row[FinancialFundOrder];
                var fundOrder = fundOrderKey.AsType<int?>();
                var fundParentIdKey = row[FinancialFundParentID];
                var fundParentId = fundParentIdKey.AsType<int?>();
                var fundPublicName = row[FinancialFundPublicName] as string;
                var isTaxDeductible = ParseBoolOrDefault( row[FinancialFundIsTaxDeductible], null );
                var campusName = row[FinancialFundCampusName];

                if ( fundName.IsNotNullOrWhiteSpace() )
                {
                    var accountFk = $"{this.ImportInstanceFKPrefix}^{fundIdKey}";

                    var account = this.ImportedAccounts.GetValueOrNull( accountFk );
                    if ( account == null )
                    {
                        account = this.ImportedAccounts.Values.FirstOrDefault( f => f.Name.Equals( fundName.Truncate( 50 ) ) );
                    }

                    //
                    // add account if doesn't exist
                    // AddAccount() auto saves the account
                    //
                    if ( account == null )
                    {
                        int? parentAccountId = null;
                        if ( !string.IsNullOrWhiteSpace( fundParentIdKey ) )
                        {
                            var parentAccount = ImportedAccounts.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{fundParentIdKey}" );
                            if ( parentAccount != null )
                            {
                                parentAccountId = parentAccount.Id;
                            }
                        }

                        Campus campus = null;
                        if ( campusName.IsNotNullOrWhiteSpace() )
                        {
                            if ( UseExistingCampusIds )
                            {
                                campus = this.CampusesDict.Values.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.OrdinalIgnoreCase )
                                            || ( c.ShortCode != null && c.ShortCode.Equals( campusName, StringComparison.OrdinalIgnoreCase ) ) );
                            }
                            else
                            {
                                campus = this.CampusImportDict.Values.FirstOrDefault( c => c.Name.Equals( campusName, StringComparison.OrdinalIgnoreCase )
                                            || ( c.ShortCode != null && c.ShortCode.Equals( campusName, StringComparison.OrdinalIgnoreCase ) ) );
                            }
                        }
                        if ( campus == null )
                        {
                            if ( UseExistingCampusIds )
                            {
                                campus = this.CampusesDict.Values.FirstOrDefault( c => fundName.Contains( c.Name ) || ( c.ShortCode != null && fundName.Contains( c.ShortCode ) ) );
                            }
                            else
                            {
                                campus = this.CampusImportDict.Values.FirstOrDefault( c => fundName.Contains( c.Name ) || ( c.ShortCode != null && fundName.Contains( c.ShortCode ) ) );
                            }
                        }
                        if ( campus == null && campusName.IsNotNullOrWhiteSpace() && !UseExistingCampusIds )
                        {
                            campus = new Campus
                            {
                                IsSystem = false,
                                Name = campusName,
                                ShortCode = campusName.RemoveWhitespace(),
                                IsActive = true,
                                ForeignKey = $"{this.ImportInstanceFKPrefix}^{campusName}"
                            };
                            lookupContext.Campuses.Add( campus );
                            lookupContext.SaveChanges( DisableAuditing );
                            this.CampusImportDict.Add( campus.ForeignKey, campus );
                        }

                        account = AddAccount( lookupContext, fundName, fundGLAccount, campus?.Id, parentAccountId, isFundActive, fundStartDate, fundEndDate, fundOrder, fundId, fundDescription, fundPublicName, isTaxDeductible, foreignKey: accountFk );
                        ImportedAccounts.Add( account.ForeignKey, account );
                    }
                    else
                    {
                        if ( string.IsNullOrWhiteSpace( account.ForeignKey ) )
                        {
                            FinancialAccount updateAccount;
                            var rockContext = new RockContext();
                            var accountService = new FinancialAccountService( rockContext );
                            updateAccount = accountService.Get( account.Id );
                            updateAccount.ForeignId = fundId;
                            updateAccount.ForeignKey = accountFk;

                            rockContext.SaveChanges();
                            LoadImportedAccounts( rockContext );
                        }
                    }

                    completed++;
                    if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} accounts imported." );
                    }

                    if ( completed % DefaultChunkSize < 1 )
                    {
                        lookupContext.SaveChanges();
                        ReportPartialProgress();
                    }
                }
            }

            lookupContext.SaveChanges();
            LoadImportedAccounts( lookupContext );

            ReportProgress( 100, $"Finished account import: {completed:N0} accounts imported." );
            return completed;
        }

        /// <summary>
        /// Maps the account data.
        /// </summary>
        /// <param name="tableData">The table data.</param>
        /// <param name="totalRows">The total rows.</param>
        private int MapBankAccount( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedBankAccounts = new FinancialPersonBankAccountService( lookupContext ).Queryable().AsNoTracking().ToList();
            var newBankAccounts = new List<FinancialPersonBankAccount>();

            var completedItems = 0;
            ReportProgress( 0, $"Verifying bank account import ({importedBankAccounts.Count:N0} already exist)." );
            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowFamilyKey = row[FamilyId];
                var rowPersonKey = row[PersonId];
                var rowFamilyId = rowFamilyKey.AsType<int?>();
                var rowPersonId = rowPersonKey.AsType<int?>();
                var personKeys = GetPersonKeys( rowPersonKey );
                if ( personKeys != null && personKeys.PersonAliasId > 0 )
                {
                    var routingNumber = row[RoutingNumber];
                    var accountNumber = row[AccountNumber];
                    if ( routingNumber.AsIntegerOrNull().HasValue && !string.IsNullOrWhiteSpace( accountNumber ) )
                    {
                        accountNumber = accountNumber.Replace( " ", string.Empty );
                        var encodedNumber = FinancialPersonBankAccount.EncodeAccountNumber( routingNumber, accountNumber );
                        if ( !importedBankAccounts.Any( a => a.PersonAliasId == personKeys.PersonAliasId && a.AccountNumberSecured == encodedNumber ) )
                        {
                            var bankAccount = new FinancialPersonBankAccount
                            {
                                CreatedByPersonAliasId = ImportPersonAliasId,
                                AccountNumberSecured = encodedNumber,
                                AccountNumberMasked = accountNumber.ToString().Masked(),
                                PersonAliasId = ( int ) personKeys.PersonAliasId
                            };

                            newBankAccounts.Add( bankAccount );
                            completedItems++;
                            if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                            {
                                ReportProgress( 0, $"{completedItems:N0} bank accounts imported." );
                            }

                            if ( completedItems % DefaultChunkSize < 1 )
                            {
                                SaveBankAccounts( newBankAccounts );
                                newBankAccounts.Clear();
                                ReportPartialProgress();
                            }
                        }
                    }
                }
            }

            if ( newBankAccounts.Any() )
            {
                SaveBankAccounts( newBankAccounts );
            }

            lookupContext.SaveChanges();

            ReportProgress( 100, $"Finished account import: {completedItems:N0} accounts imported." );
            return completedItems;
        }

        /// <summary>
        /// Saves the bank accounts.
        /// </summary>
        /// <param name="newBankAccounts">The new bank accounts.</param>
        private static void SaveBankAccounts( List<FinancialPersonBankAccount> newBankAccounts )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialPersonBankAccounts.AddRange( newBankAccounts );
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Maps the batch data.
        /// </summary>
        /// <param name="csvData">The table data.</param>
        /// <exception cref="System.NotImplementedException"></exception>
        private int MapBatch( CSVInstance csvData )
        {
            var lookupContext = new RockContext();

            if ( this.ImportedBatches == null )
            {
                LoadImportedBatches( lookupContext );
            }

            var newBatches = new List<FinancialBatch>();
            var earliestBatchDate = ImportDateTime;

            // Look for custom attributes in the Batch file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > BatchAmount )
                .ToDictionary( f => f.index, f => f.node.Name );

            var completed = 0;
            ReportProgress( 0, $"Verifying batch import ({this.ImportedBatches.Count:N0} already exist)." );
            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var batchIdKey = row[BatchId];
                if ( !string.IsNullOrWhiteSpace( batchIdKey ) && !this.ImportedBatches.ContainsKey( string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, batchIdKey ) ) )
                {
                    var batch = new FinancialBatch
                    {
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, batchIdKey ),
                        ForeignId = batchIdKey.AsIntegerOrNull(),
                        Note = string.Empty,
                        Status = BatchStatus.Closed,
                        AccountingSystemCode = string.Empty
                    };

                    var name = row[BatchName] as string;
                    if ( !string.IsNullOrWhiteSpace( name ) )
                    {
                        name = name.Trim();
                        batch.Name = name.Left( 50 );

                        Campus campus = null;
                        if ( UseExistingCampusIds )
                        {
                            campus = this.CampusesDict.Values.FirstOrDefault( c => name.StartsWith( c.Name ) || ( c.ShortCode != null && name.StartsWith( c.ShortCode ) ) );
                        }
                        else
                        {
                            campus = this.CampusImportDict.Values.FirstOrDefault( c => name.StartsWith( c.Name ) || ( c.ShortCode != null && name.StartsWith( c.ShortCode ) ) );
                        }
                        batch.CampusId = campus?.Id;
                    }

                    var batchDateKey = row[BatchDate];
                    var batchDate = batchDateKey.AsType<DateTime?>();
                    if ( batchDate != null )
                    {
                        batch.BatchStartDateTime = batchDate;
                        batch.BatchEndDateTime = batchDate;
                        if ( earliestBatchDate > batchDate )
                        {
                            earliestBatchDate = ( DateTime ) batchDate;
                        }
                    }

                    var amountKey = row[BatchAmount];
                    var amount = amountKey.AsType<decimal?>();
                    if ( amount != null )
                    {
                        batch.ControlAmount = amount.HasValue ? amount.Value : new decimal();
                    }

                    //
                    // Process Attributes for Batch
                    //
                    if ( customAttributes.Any() )
                    {
                        // create transaction attributes
                        foreach ( var newAttributePair in customAttributes )
                        {
                            var pairs = newAttributePair.Value.Split( '^' );
                            var categoryName = string.Empty;
                            var attributeName = string.Empty;
                            var attributeTypeString = string.Empty;
                            var attributeForeignKey = string.Empty;
                            var definedValueForeignKey = string.Empty;
                            var fieldTypeId = TextFieldTypeId;

                            if ( pairs.Length == 1 )
                            {
                                attributeName = pairs[0];
                            }
                            else if ( pairs.Length == 2 )
                            {
                                attributeName = pairs[0];
                                attributeTypeString = pairs[1];
                            }
                            else if ( pairs.Length >= 3 )
                            {
                                categoryName = pairs[1];
                                attributeName = pairs[2];
                                if ( pairs.Length >= 4 )
                                {
                                    attributeTypeString = pairs[3];
                                }
                                if ( pairs.Length >= 5 )
                                {
                                    attributeForeignKey = pairs[4];
                                }
                                if ( pairs.Length >= 6 )
                                {
                                    definedValueForeignKey = pairs[5];
                                }
                            }

                            var definedValueForeignId = definedValueForeignKey.AsType<int?>();

                            //
                            // Translate the provided attribute type into one we know about.
                            //
                            fieldTypeId = GetAttributeFieldType( attributeTypeString );

                            if ( string.IsNullOrEmpty( attributeName ) )
                            {
                                LogException( "Financial Batch", $"Financial Batch Attribute Name cannot be blank '{newAttributePair.Value}'." );
                            }
                            else
                            {
                                //
                                // First try to find the existing attribute, if not found then add a new one.
                                //
                                if ( FindEntityAttribute( lookupContext, categoryName, attributeName, batch.TypeId, attributeForeignKey ) == null )
                                {
                                    var fk = string.Empty;
                                    if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                                    {
                                        fk = $"Bulldozer_FinancialBatch_{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}".Left( 100 );
                                    }
                                    else
                                    {
                                        fk = attributeForeignKey;
                                    }

                                    AddEntityAttribute( lookupContext, batch.TypeId, string.Empty, string.Empty, fk, categoryName, attributeName, string.Empty, fieldTypeId, true, definedValueForeignId, definedValueForeignKey, attributeTypeString: attributeTypeString );
                                }
                            }
                        }

                        //
                        // Add any Batch attribute values
                        //
                        foreach ( var attributePair in customAttributes )
                        {
                            var newValue = row[attributePair.Key];

                            if ( !string.IsNullOrWhiteSpace( newValue ) )
                            {
                                var pairs = attributePair.Value.Split( '^' );
                                var categoryName = string.Empty;
                                var attributeName = string.Empty;
                                var attributeTypeString = string.Empty;
                                var attributeForeignKey = string.Empty;
                                var definedValueForeignKey = string.Empty;

                                if ( pairs.Length == 1 )
                                {
                                    attributeName = pairs[0];
                                }
                                else if ( pairs.Length == 2 )
                                {
                                    attributeName = pairs[0];
                                    attributeTypeString = pairs[1];
                                }
                                else if ( pairs.Length >= 3 )
                                {
                                    categoryName = pairs[1];
                                    attributeName = pairs[2];
                                    if ( pairs.Length >= 4 )
                                    {
                                        attributeTypeString = pairs[3];
                                    }
                                    if ( pairs.Length >= 5 )
                                    {
                                        attributeForeignKey = pairs[4];
                                    }
                                    if ( pairs.Length >= 6 )
                                    {
                                        definedValueForeignKey = pairs[5];
                                    }
                                }

                                if ( !string.IsNullOrEmpty( attributeName ) )
                                {
                                    var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, batch.TypeId, attributeForeignKey );
                                    AddEntityAttributeValue( lookupContext, attribute, batch, newValue, null, true );
                                }
                            }
                        }
                    }

                    newBatches.Add( batch );
                    completed++;
                    if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} batches imported." );
                    }

                    if ( completed % DefaultChunkSize < 1 )
                    {
                        SaveFinancialBatches( newBatches );
                        newBatches.ForEach( b => this.ImportedBatches.Add( b.ForeignKey, ( int? ) b.Id ) );
                        newBatches.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            // add a default batch to use with contributions
            if ( !this.ImportedBatches.ContainsKey( "0" ) )
            {
                var defaultBatch = new FinancialBatch
                {
                    CreatedDateTime = ImportDateTime,
                    CreatedByPersonAliasId = ImportPersonAliasId,
                    BatchStartDateTime = earliestBatchDate,
                    Status = BatchStatus.Closed,
                    Name = $"Default Batch {ImportDateTime}",
                    ControlAmount = 0.0m,
                    ForeignKey = "0",
                    ForeignId = 0
                };

                newBatches.Add( defaultBatch );
            }

            if ( newBatches.Any() )
            {
                SaveFinancialBatches( newBatches );
                newBatches.ForEach( b => this.ImportedBatches.Add( b.ForeignKey, ( int? ) b.Id ) );
            }

            LoadImportedBatches();
            ReportProgress( 100, $"Finished batch import: {completed:N0} batches imported." );
            return completed;
        }

        /// <summary>
        /// Saves the financial batches.
        /// </summary>
        /// <param name="newBatches">The new batches.</param>
        private static void SaveFinancialBatches( List<FinancialBatch> newBatches )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialBatches.AddRange( newBatches );
                rockContext.SaveChanges( DisableAuditing );

                foreach ( var batch in newBatches )
                {
                    // Set attributes on this transaction
                    if ( batch.Attributes != null )
                    {
                        foreach ( var attributeCache in batch.Attributes.Select( a => a.Value ) )
                        {
                            var existingValue = rockContext.AttributeValues.FirstOrDefault( v => v.Attribute.Key == attributeCache.Key && v.EntityId == batch.Id );
                            var newAttributeValue = batch.AttributeValues[attributeCache.Key];

                            // set the new value and add it to the database
                            if ( existingValue == null )
                            {
                                existingValue = new AttributeValue
                                {
                                    AttributeId = newAttributeValue.AttributeId,
                                    EntityId = batch.Id,
                                    Value = newAttributeValue.Value
                                };

                                rockContext.AttributeValues.Add( existingValue );
                            }
                            else
                            {
                                existingValue.Value = newAttributeValue.Value;
                                rockContext.Entry( existingValue ).State = EntityState.Modified;
                            }
                        }
                    }
                }
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Maps the pledge.
        /// </summary>
        /// <param name="csvData">todo: describe csvData parameter on MapPledge</param>
        /// <returns></returns>
        /// <exception cref="System.NotImplementedException"></exception>
        private int MapPledge( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var accountList = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking().ToList();
            var importedPledges = new FinancialPledgeService( lookupContext )
                .Queryable().AsNoTracking()
                .Where( p => p.ForeignId != null )
                .ToDictionary( t => ( int ) t.ForeignId, t => ( int? ) t.Id );

            var pledgeFrequencies = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_FREQUENCY ), lookupContext ).DefinedValues;
            var oneTimePledgeFrequencyId = pledgeFrequencies.FirstOrDefault( f => f.Guid == new Guid( Rock.SystemGuid.DefinedValue.TRANSACTION_FREQUENCY_ONE_TIME ) ).Id;

            var newPledges = new List<FinancialPledge>();

            var completed = 0;
            ReportProgress( 0, $"Verifying pledge import ({importedPledges.Count:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var amountKey = row[TotalPledge];
                var amount = amountKey.AsType<decimal?>();
                var startDateKey = row[StartDate];
                if ( string.IsNullOrWhiteSpace( startDateKey ) )
                {
                    startDateKey = "01/01/0001";
                }
                var startDate = startDateKey.AsType<DateTime?>();
                var endDateKey = row[EndDate];
                if ( string.IsNullOrWhiteSpace( endDateKey ) )
                {
                    endDateKey = "12/31/9999";
                }
                var endDate = endDateKey.AsType<DateTime?>();
                var createdDateKey = row[PledgeCreatedDate];
                if ( string.IsNullOrWhiteSpace( createdDateKey ) )
                {
                    createdDateKey = ImportDateTime.ToString();
                }
                var createdDate = createdDateKey.AsType<DateTime?>();
                var modifiedDateKey = row[PledgeModifiedDate];
                if ( string.IsNullOrWhiteSpace( modifiedDateKey ) )
                {
                    modifiedDateKey = ImportDateTime.ToString();
                }
                var modifiedDate = modifiedDateKey.AsType<DateTime?>();

                var pledgeIdKey = row[PledgeId];
                var pledgeId = pledgeIdKey.AsType<int?>();
                if ( amount != null && !importedPledges.ContainsKey( ( int ) pledgeId ) )
                {
                    var individualIdKey = row[IndividualID];

                    var personKeys = GetPersonKeys( individualIdKey );
                    if ( personKeys != null && personKeys.PersonAliasId > 0 )
                    {
                        var pledge = new FinancialPledge
                        {
                            PersonAliasId = personKeys.PersonAliasId,
                            CreatedByPersonAliasId = ImportPersonAliasId,
                            StartDate = ( DateTime ) startDate,
                            EndDate = ( DateTime ) endDate,
                            TotalAmount = ( decimal ) amount,
                            CreatedDateTime = createdDate,
                            ModifiedDateTime = modifiedDate,
                            ModifiedByPersonAliasId = ImportPersonAliasId,
                            ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, pledgeIdKey ),
                            ForeignId = pledgeId
                        };

                        var frequency = row[PledgeFrequencyName].ToString().ToLower();
                        if ( !string.IsNullOrWhiteSpace( frequency ) )
                        {
                            frequency = frequency.ToLower();
                            if ( frequency.Equals( "one time" ) || frequency.Equals( "one-time" ) || frequency.Equals( "as can" ) )
                            {
                                pledge.PledgeFrequencyValueId = oneTimePledgeFrequencyId;
                            }
                            else
                            {
                                pledge.PledgeFrequencyValueId = pledgeFrequencies
                                    .Where( f => f.Value.ToLower().StartsWith( frequency ) || f.Description.ToLower().StartsWith( frequency ) )
                                    .Select( f => f.Id ).FirstOrDefault();
                            }
                        }

                        var fundName = row[FundName] as string;
                        var subFund = row[SubFundName] as string;
                        var fundGLAccount = row[FundGLAccount] as string;
                        var subFundGLAccount = row[SubFundGLAccount] as string;
                        var isFundActive = ParseBoolOrDefault( row[FundIsActive], null );
                        var isSubFundActive = ParseBoolOrDefault( row[SubFundIsActive], null );

                        if ( !string.IsNullOrWhiteSpace( fundName ) )
                        {
                            var parentAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName.Truncate( 50 ) ) );
                            if ( parentAccount == null )
                            {
                                var accountFk = $"{this.ImportInstanceFKPrefix}^{fundName.RemoveWhitespace().Truncate( 50 )}";
                                parentAccount = AddAccount( lookupContext, fundName, string.Empty, null, null, isFundActive, foreignKey: accountFk );
                                accountList.Add( parentAccount );
                            }

                            if ( !string.IsNullOrWhiteSpace( subFund ) )
                            {
                                // assign a campus if the subfund is a campus fund

                                Campus campus = null;
                                if ( UseExistingCampusIds )
                                {
                                    campus = this.CampusesDict.Values.FirstOrDefault( c => subFund.Contains( c.Name ) || ( c.ShortCode != null && subFund.Contains( c.ShortCode ) ) );
                                }
                                else
                                {
                                    campus = this.CampusImportDict.Values.FirstOrDefault( c => subFund.Contains( c.Name ) || ( c.ShortCode != null && subFund.Contains( c.ShortCode ) ) );
                                }

                                // add info to easily find/assign this fund in the view

                                var subAccountFk = $"{this.ImportInstanceFKPrefix}^{subFund.RemoveWhitespace().Truncate( 50 )}";
                                var childAccount = accountList.FirstOrDefault( c => c.Name.Equals( subFund.Truncate( 50 ) ) && c.ParentAccountId == parentAccount.Id );
                                if ( childAccount == null )
                                {
                                    childAccount = accountList.FirstOrDefault( c => !string.IsNullOrWhiteSpace( c.ForeignKey ) && c.ForeignKey.Equals( subAccountFk ) && c.ParentAccountId == parentAccount.Id );
                                }
                                if ( childAccount == null )
                                {
                                    // create a child account with a campusId if it was set
                                    childAccount = AddAccount( lookupContext, subFund, string.Empty, campus?.Id, parentAccount.Id, isSubFundActive, foreignKey: subAccountFk );
                                    accountList.Add( childAccount );
                                }

                                pledge.AccountId = childAccount.Id;
                            }
                            else
                            {
                                pledge.AccountId = parentAccount.Id;
                            }
                        }

                        newPledges.Add( pledge );
                        completed++;
                        if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                        {
                            ReportProgress( 0, $"{completed:N0} pledges imported." );
                        }

                        if ( completed % DefaultChunkSize < 1 )
                        {
                            SavePledges( newPledges );
                            ReportPartialProgress();
                            newPledges.Clear();
                        }
                    }
                }
            }

            if ( newPledges.Any() )
            {
                SavePledges( newPledges );
            }

            ReportProgress( 100, $"Finished pledge import: {completed:N0} pledges imported." );
            return completed;
        }

        /// <summary>
        /// Saves the pledges.
        /// </summary>
        /// <param name="newPledges">The new pledges.</param>
        private static void SavePledges( List<FinancialPledge> newPledges )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialPledges.AddRange( newPledges );
                rockContext.SaveChanges( DisableAuditing );
            }
        }

        /// <summary>
        /// Load the various scheduled transactions from the CSV data and imports
        /// them into the database.
        /// </summary>
        /// <param name="csvData"></param>
        /// <returns></returns>
        private int LoadScheduledTransaction( CSVInstance csvData )
        {
            // Required variables
            var lookupContext = new RockContext();
            var transactionService = new FinancialScheduledTransactionService( lookupContext );
            var paymentDetailService = new FinancialPaymentDetailService( lookupContext );
            var accountService = new FinancialAccountService( lookupContext );
            var accountList = accountService.Queryable().AsNoTracking().ToList();
            var gatewayService = new FinancialGatewayService( lookupContext );
            var frequencyValues = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.FINANCIAL_FREQUENCY.AsGuid() ).DefinedValues;

            var currentTransaction = new FinancialScheduledTransaction();
            var newTransactionList = new List<FinancialScheduledTransaction>();
            var updatedTransactionList = new List<FinancialScheduledTransaction>();
            var invalidTransactionKeys = new List<string>();

            var currentTransactionKey = string.Empty;

            var currencyTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE ) );
            var currencyTypeACH = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_ACH ) ) ).Id;
            var currencyTypeCreditCard = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CREDIT_CARD ) ) ).Id;

            var creditCardTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE ) ).DefinedValues;

            var completed = 0;
            var imported = new FinancialScheduledTransactionDetailService( lookupContext ).Queryable().AsNoTracking().Count( t => t.ForeignKey != null && t.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) );
            ReportProgress( 0, $"Starting scheduled transaction import ({imported:N0} detail records already exist)." );
            imported = 0;

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowTransactionKey = row[ScheduledTransactionId];
                var rowTransactionId = rowTransactionKey.AsType<int?>();
                var rowPersonKey = row[ScheduledTransactionPersonId];
                var transactionForeignKey = $"{this.ImportInstanceFKPrefix}^{rowTransactionKey}";

                //
                // Skip if the scheduled transaction has already been marked invalid
                //
                if ( invalidTransactionKeys.Any( k => k == rowTransactionKey ) )
                {
                    continue;
                }

                //
                // Determine if we are still working with the same scheduled transaction or not.
                //
                if ( rowTransactionKey != null && transactionForeignKey != currentTransaction.ForeignKey )
                {
                    currentTransaction = newTransactionList.FirstOrDefault( t => t.ForeignKey == transactionForeignKey );
                    if ( currentTransaction == null )
                    {
                        currentTransaction = transactionService.Queryable().FirstOrDefault( t => t.ForeignKey == transactionForeignKey );
                    }
                    if ( currentTransaction == null )
                    {
                        currentTransaction = new FinancialScheduledTransaction
                        {
                            ForeignKey = transactionForeignKey,
                            ForeignId = rowTransactionId
                        };
                    }

                    //
                    // Find and set the authorized person, i.e. the person whom the transaction is for.
                    //
                    var personKeys = GetPersonKeys( rowPersonKey );
                    if ( personKeys != null && personKeys.PersonAliasId > 0 )
                    {
                        currentTransaction.AuthorizedPersonAliasId = personKeys.PersonAliasId;
                        currentTransaction.CreatedDateTime = ParseDateOrDefault( row[ScheduledTransactionCreatedDate], ImportDateTime );
                        currentTransaction.ModifiedDateTime = ImportDateTime;
                        currentTransaction.CreatedByPersonAliasId = personKeys.PersonAliasId;
                        currentTransaction.ModifiedByPersonAliasId = personKeys.PersonAliasId;
                    }
                    else
                    {
                        LogException( "Invalid Scheduled Transaction", $"Cannot find person alias with person key '{rowPersonKey}' for scheduled transaction key {rowTransactionKey}." );
                        invalidTransactionKeys.Add( rowTransactionKey );
                        currentTransaction = new FinancialScheduledTransaction();
                        continue;
                    }

                    var startDate = ParseDateOrDefault( row[ScheduledTransactionStartDate], null );
                    if ( startDate == null )
                    {
                        throw new ArgumentOutOfRangeException( $"Cannot parse start date '{row[ScheduledTransactionStartDate]}' for scheduled transaction key {rowTransactionKey}" );
                    }
                    currentTransaction.StartDate = ( DateTime ) startDate;
                    currentTransaction.EndDate = ParseDateOrDefault( row[ScheduledTransactionEndDate], null );
                    currentTransaction.NextPaymentDate = ParseDateOrDefault( row[ScheduledTransactionNextPaymentDate], null );
                    currentTransaction.IsActive = ( bool ) ParseBoolOrDefault( row[ScheduledTransactionActive], true );

                    //
                    // Find the frequency by ID number or by name. If not found error out.
                    //
                    var rowFrequency = row[ScheduledTransactionFrequency];
                    var rowFrequencyId = rowFrequency.AsType<int?>();
                    var frequency = rowFrequencyId != null ? frequencyValues.FirstOrDefault( f => f.Id == rowFrequencyId )
                        : frequencyValues.FirstOrDefault( f => f.Value.Equals( rowFrequency, StringComparison.OrdinalIgnoreCase ) );

                    if ( frequency == null )
                    {
                        throw new ArgumentOutOfRangeException( $"Cannot find a scheduled frequency that matches '{rowFrequency}' for scheduled transaction key {rowTransactionKey}" );
                    }

                    currentTransaction.TransactionFrequencyValueId = frequency.Id;
                    currentTransaction.NumberOfPayments = row[ScheduledTransactionNumberOfPayments].AsType<int?>();
                    currentTransaction.TransactionCode = row[ScheduledTransactionTransactionCode];
                    currentTransaction.GatewayScheduleId = row[ScheduledTransactionGatewaySchedule];

                    //
                    // Find the gateway by ID number or by name. Error out if not found.
                    //
                    FinancialGateway gateway = null;
                    var rowGateway = row[ScheduledTransactionGateway];
                    var rowGatewayId = rowGateway.AsType<int?>();
                    gateway = rowGatewayId.HasValue ? gatewayService.Queryable().FirstOrDefault( g => g.Id == rowGatewayId ) : gatewayService.Queryable().FirstOrDefault( g => g.Name.Equals( rowGateway, StringComparison.OrdinalIgnoreCase ) );

                    if ( gateway == null )
                    {
                        gateway = AddFinancialGateway( lookupContext, rowGateway );
                    }

                    currentTransaction.FinancialGatewayId = gateway.Id;

                    var currencyType = row[ScheduledTransactionCurrencyType];
                    var creditCardType = row[ScheduledTransactionCreditCardType];
                    if ( !string.IsNullOrWhiteSpace( currencyType ) )
                    {
                        int? currencyTypeId = null, creditCardTypeId = null;

                        // Determine Currency Type
                        if ( currencyType.Equals( "ach", StringComparison.OrdinalIgnoreCase ) )
                        {
                            currencyTypeId = currencyTypeACH;
                        }
                        else if ( currencyType.Equals( "credit card", StringComparison.OrdinalIgnoreCase ) )
                        {
                            currencyTypeId = currencyTypeCreditCard;

                            // Determine CC Type
                            if ( !string.IsNullOrWhiteSpace( creditCardType ) )
                            {
                                creditCardTypeId = creditCardTypes.Where( c => c.Value.StartsWith( creditCardType, StringComparison.OrdinalIgnoreCase )
                                        || ( !string.IsNullOrWhiteSpace( c.Description ) && c.Description.StartsWith( creditCardType, StringComparison.OrdinalIgnoreCase ) ) )
                                    .Select( c => c.Id ).FirstOrDefault();
                            }
                        }

                        // Add Payment Details
                        var paymentDetail = paymentDetailService.Queryable().FirstOrDefault( t => t.ForeignKey == transactionForeignKey );
                        if ( paymentDetail == null )
                        {
                            paymentDetail = new FinancialPaymentDetail
                            {
                                CreatedDateTime = ParseDateOrDefault( row[ScheduledTransactionCreatedDate], ImportDateTime ),
                                CreatedByPersonAliasId = personKeys.PersonAliasId,
                                ModifiedDateTime = ImportDateTime,
                                ModifiedByPersonAliasId = personKeys.PersonAliasId,
                                CurrencyTypeValueId = currencyTypeId,
                                CreditCardTypeValueId = creditCardTypeId,
                                ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowTransactionKey ),
                                ForeignId = rowTransactionId
                            };
                        }
                        currentTransaction.FinancialPaymentDetail = paymentDetail;
                    }
                }

                //
                // We have the scheduled transaction setup, add in the individual account gift.
                //
                var rowAmount = row[ScheduledTransactionAmount].AsType<decimal?>();
                var rowAccount = row[ScheduledTransactionAccount];
                var rowAccountId = rowAccount.AsType<int?>();
                FinancialAccount account = null;
                var accountFk = $"{this.ImportInstanceFKPrefix}^";
                if ( rowAccountId.HasValue )
                {
                    account = accountList.FirstOrDefault( a => a.ForeignId == rowAccountId );
                }
                if ( account == null )
                {
                    if ( rowAccountId.HasValue )
                    {
                        accountFk += $"{rowAccountId.Value}";
                    }
                    else
                    {
                        accountFk += rowAccount.RemoveWhitespace().Truncate( 50 );
                    }
                    account = accountList.FirstOrDefault( a => a.ForeignKey == accountFk );
                    if ( account == null )
                    {
                        account = accountList.FirstOrDefault( a => a.Name.Equals( rowAccount, StringComparison.OrdinalIgnoreCase ) );
                    }
                }

                if ( account == null && !rowAccountId.HasValue )
                {
                    var accountContext = new RockContext();
                    account = AddAccount( accountContext, rowAccount, string.Empty, null, null, true, foreignId: rowAccountId, foreignKey: accountFk );
                    accountList.Add( account );
                }

                if ( account == null )
                {
                    throw new ArgumentOutOfRangeException( $"Cannot find an account that matches '{rowAccount}' for scheduled transaction key {rowTransactionKey}" );
                }

                if ( rowAmount == null )
                {
                    throw new ArgumentOutOfRangeException( $"Cannot parse amount value '{row[ScheduledTransactionAmount]}' for scheduled transaction key {rowTransactionKey}" );
                }

                var transactionDetail = currentTransaction.ScheduledTransactionDetails.FirstOrDefault( d => d.AccountId == account.Id );
                if ( transactionDetail == null )
                {
                    transactionDetail = new FinancialScheduledTransactionDetail
                    {
                        AccountId = account.Id,
                        ForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, rowTransactionKey ),
                        ForeignId = rowTransactionId,
                        CreatedDateTime = ParseDateOrDefault( row[ScheduledTransactionCreatedDate], ImportDateTime ),
                        ModifiedDateTime = ImportDateTime,
                        CreatedByPersonAliasId = currentTransaction.AuthorizedPersonAliasId,
                        ModifiedByPersonAliasId = currentTransaction.AuthorizedPersonAliasId
                    };
                }
                transactionDetail.Amount = ( decimal ) rowAmount;
                if ( transactionDetail.Id == 0 )
                {
                    currentTransaction.ScheduledTransactionDetails.Add( transactionDetail );
                }

                if ( currentTransaction.Id <= 0 )
                {
                    newTransactionList.Add( currentTransaction );
                }
                else
                {
                    updatedTransactionList.Add( currentTransaction );
                }

                //
                // Keep the user informed as to what is going on and save in batches.
                //
                completed++;
                if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed:N0} scheduled transaction details imported." );
                }

                if ( completed % DefaultChunkSize < 1 )
                {
                    SaveScheduledTransactions( newTransactionList, updatedTransactionList );
                    ReportPartialProgress();

                    // Reset lookup context
                    lookupContext.SaveChanges();
                    lookupContext = new RockContext();
                    transactionService = new FinancialScheduledTransactionService( lookupContext );
                    accountService = new FinancialAccountService( lookupContext );
                    gatewayService = new FinancialGatewayService( lookupContext );
                    newTransactionList.Clear();
                    updatedTransactionList.Clear();
                }
            }

            //
            // Check to see if any rows didn't get saved to the database
            //
            if ( newTransactionList.Any() || updatedTransactionList.Any() )
            {
                SaveScheduledTransactions( newTransactionList, updatedTransactionList );
            }

            lookupContext.SaveChanges();
            lookupContext.Dispose();

            ReportProgress( 0, $"Finished scheduled transaction import: {completed:N0} scheduled transaction details added or updated." );

            return completed;
        }

        /// <summary>
        /// Saves new scheduled transactions into the database.
        /// </summary>
        /// <param name="newTransactions">The new transactions.</param>
        /// <param name="updatedTransactions">The updated transactions.</param>
        private static void SaveScheduledTransactions( List<FinancialScheduledTransaction> newTransactions, List<FinancialScheduledTransaction> updatedTransactions )
        {
            var rockContext = new RockContext();
            var updatedList = updatedTransactions.Except( newTransactions ).Distinct().ToList();

            if ( newTransactions.Any() )
            {
                rockContext.WrapTransaction( () =>
                {
                    rockContext.FinancialScheduledTransactions.AddRange( newTransactions );
                    rockContext.SaveChanges( DisableAuditing );
                } );
            }
        }

        /// <summary>
        /// Adds the account.
        /// </summary>
        /// <param name="lookupContext">The lookup context.</param>
        /// <param name="fundName">Name of the fund.</param>
        /// <param name="accountGL">The account gl.</param>
        /// <param name="campusId">The campus identifier.</param>
        /// <param name="parentAccountId">The parent account identifier.</param>
        /// <param name="isActive">The is active.</param>
        /// <param name="startDate">The start date.</param>
        /// <param name="endDate">The end date.</param>
        /// <param name="order">The order.</param>
        /// <param name="foreignId">The foreign identifier.</param>
        /// <param name="fundDescription">The fund description.</param>
        /// <param name="fundPublicName">Name of the fund public.</param>
        /// <param name="isTaxDeductible">The is tax deductible.</param>
        /// <returns></returns>
        public FinancialAccount AddAccount( RockContext lookupContext, string fundName, string accountGL, int? campusId, int? parentAccountId, bool? isActive, DateTime? startDate = null, DateTime? endDate = null, int? order = null, int? foreignId = null, string fundDescription = "", string fundPublicName = "", bool? isTaxDeductible = null, string foreignKey = null )
        {
            lookupContext = lookupContext ?? new RockContext();

            var account = new FinancialAccount
            {
                Name = fundName.Truncate( 50 ),
                Description = fundDescription,
                GlCode = accountGL.Truncate( 50 ),
                IsTaxDeductible = isTaxDeductible ?? true,
                IsActive = isActive ?? true,
                IsPublic = false,
                CampusId = campusId,
                ParentAccountId = parentAccountId,
                CreatedByPersonAliasId = ImportPersonAliasId,
                StartDate = startDate,
                EndDate = endDate,
                ForeignId = foreignId,
                ForeignKey = foreignKey
            };

            if ( !string.IsNullOrWhiteSpace( fundPublicName ) )
            {
                account.PublicName = fundPublicName.Truncate( 50 );
            }
            else
            {
                account.PublicName = fundName.Truncate( 50 );
            }

            if ( order != null )
            {
                account.Order = order ?? -1;
            }

            lookupContext.FinancialAccounts.Add( account );
            lookupContext.SaveChanges( DisableAuditing );

            return account;
        }

        /// <summary>
        /// Maps the financial account data.
        /// </summary>
        /// <exception cref="System.NotImplementedException"></exception>
        private int ImportFinancialAccounts()
        {
            ReportProgress( 0, "Preparing FinancialAccount data for import..." );

            var rockContext = new RockContext();
            var accountsToInsert = new List<FinancialAccount>();

            if ( this.ImportedAccounts == null )
            {
                LoadImportedAccounts( rockContext );
            }
            if ( this.CampusesDict == null )
            {
                LoadCampusDict( rockContext );
            }

            var accounts = this.FinancialAccountCsvList.Where( a => !this.ImportedAccounts.ContainsKey( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.Id ) ) ).ToList();
            ReportProgress( 0, $"{this.FinancialAccountCsvList.Count - accounts.Count} Financial Accounts already exist. Begin processing {accounts.Count} FinancialAccount Records..." );

            foreach ( var account in accounts )
            {
                var accountName = account.Name.IsNotNullOrWhiteSpace() ? account.Name.Truncate( 50 ) : "Unnamed Financial Account";
                var newAccount = new FinancialAccount
                {
                    Name = accountName,
                    Description = account.Description,
                    IsTaxDeductible = account.IsTaxDeductible.GetValueOrDefault(),
                    StartDate = account.StartDate,
                    EndDate = account.EndDate,
                    IsActive = account.IsActive.GetValueOrDefault(),
                    ForeignKey = $"{ImportInstanceFKPrefix}^{account.Id}",
                    ForeignId = account.Id.AsIntegerOrNull()
                };

                if ( account.Name.Length > 50 )
                {
                    newAccount.Description = account.Name;
                }
                if ( account.Campus != null )
                {
                    newAccount.CampusId = GetCampus( account.Campus.CampusId, this.ImportInstanceFKPrefix, UseExistingCampusIds, account.Campus.CampusName, true );
                }
                if ( account.PublicName.IsNotNullOrWhiteSpace() )
                {
                    newAccount.PublicName = account.PublicName.Truncate( 50 );
                }
                else
                {
                    newAccount.PublicName = accountName;
                }

                if ( account.Order.HasValue )
                {
                    newAccount.Order = account.Order.Value;
                }

                accountsToInsert.Add( newAccount );
            }

            rockContext.BulkInsert( accountsToInsert );

            var accountsUpdated = false;
            var accountsWithParentAccount = accounts.Where( a => !string.IsNullOrWhiteSpace( a.ParentAccountId ) ).ToList();
            var accountLookup = new FinancialAccountService( rockContext ).Queryable().Where( a => a.ForeignKey != null && a.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) ).ToDictionary( k => k.ForeignKey, v => v );
            foreach ( var account in accountsWithParentAccount )
            {
                var accountFk = $"{ImportInstanceFKPrefix}^{account.Id}";
                var financialAccount = accountLookup.GetValueOrNull( accountFk );
                if ( financialAccount != null )
                {
                    var parentAccountFk = $"{ImportInstanceFKPrefix}^{account.ParentAccountId}";
                    var parentAccount = accountLookup.GetValueOrNull( parentAccountFk );
                    if ( parentAccount != null && financialAccount.ParentAccountId != parentAccount.Id )
                    {
                        financialAccount.ParentAccountId = parentAccount.Id;
                        accountsUpdated = true;
                    }
                    else
                    {
                        LogException( "Financial Account", $"Unable to find ParentAccountId ({account.ParentAccountId}) for FinancialAccount {account.Id}. No parent account was set for this FinancialAccount." );
                    }
                }
                else
                {
                    LogException( "Financial Account", $"Unable to find FinancialAccount {account.Id} to set up a parent account ({account.ParentAccountId}) for it. No parent account was set for this FinancialAccount." );
                }
            }

            if ( accountsUpdated )
            {
                rockContext.SaveChanges( true );
            }
            LoadImportedAccounts( rockContext );

            ReportProgress( 100, $"Finished account import: {accounts.Count} FinancialAccounts imported." );
            return accounts.Count;
        }

        /// <summary>
        /// Maps the financial batch data.
        /// </summary>
        /// <exception cref="System.NotImplementedException"></exception>
        private int ImportFinancialBatch()
        {
            ReportProgress( 0, "Preparing FinancialBatch data for import..." );

            var rockContext = new RockContext();
            var batchesToInsert = new List<FinancialBatch>();

            if ( this.ImportedAccounts == null )
            {
                LoadImportedAccounts( rockContext );
            }
            if ( this.ImportedBatches == null )
            {
                LoadImportedBatches( rockContext );
            }

            var batches = this.FinancialBatchCsvList.Where( a => !this.ImportedBatches.ContainsKey( string.Format( "{0}^{1}", ImportInstanceFKPrefix, a.Id ) ) ).ToList();
            ReportProgress( 0, $"{this.FinancialBatchCsvList.Count - batches.Count} Financial Batches already exist. Begin processing {batches.Count} FinancialBatch Records..." );

            if ( ImportedPeopleKeys == null )
            {
                LoadPersonKeys( rockContext );
            }
            var importedDateTime = RockDateTime.Now;

            foreach ( var batch in batches )
            {
                var batchname = !batch.Name.IsNotNullOrWhiteSpace() ? batch.Name.Truncate( 50 ) : "Unnamed Financial Batch";
                var newBatch = new FinancialBatch
                {
                    Name = batchname,
                    ControlAmount = batch.ControlAmount,
                    CreatedDateTime = batch.CreatedDateTime.ToSQLSafeDate(),
                    ModifiedDateTime = batch.ModifiedDateTime.ToSQLSafeDate(),
                    BatchStartDateTime = batch.StartDate.ToSQLSafeDate(),
                    BatchEndDateTime = batch.EndDate.ToSQLSafeDate(),
                    ForeignKey = $"{ImportInstanceFKPrefix}^{batch.Id}",
                    ForeignId = batch.Id.AsIntegerOrNull()
                };

                switch ( batch.Status )
                {
                    case ( CSVInstance.BatchStatus ) BatchStatus.Closed:
                        newBatch.Status = BatchStatus.Closed;
                        break;

                    case ( CSVInstance.BatchStatus ) BatchStatus.Open:
                        newBatch.Status = BatchStatus.Open;
                        break;

                    case ( CSVInstance.BatchStatus ) BatchStatus.Pending:
                        newBatch.Status = BatchStatus.Pending;
                        break;
                }
                if ( batch.Campus != null )
                {
                    newBatch.CampusId = GetCampus( batch.Campus.CampusId, this.ImportInstanceFKPrefix, UseExistingCampusIds, batch.Campus.CampusName );
                }
                if ( batch.CreatedByPersonId.IsNotNullOrWhiteSpace() )
                {
                    newBatch.CreatedByPersonAliasId = ImportedPeopleKeys.GetValueOrNull( $"{ImportInstanceFKPrefix}^{batch.CreatedByPersonId}" )?.PersonAliasId;
                }
                if ( batch.ModifieddByPersonId.IsNotNullOrWhiteSpace() )
                {
                    newBatch.ModifiedByPersonAliasId = ImportedPeopleKeys.GetValueOrNull( $"{ImportInstanceFKPrefix}^{batch.ModifieddByPersonId}" )?.PersonAliasId;
                }
                if ( !newBatch.CreatedByPersonAliasId.HasValue )
                {
                    newBatch.CreatedByPersonAliasId = ImportPersonAliasId;
                }

                batchesToInsert.Add( newBatch );
            }

            // Slice data into chunks and process
            var workingBatchImportList = batchesToInsert.ToList();
            var batchesRemainingToProcess = batchesToInsert.Count;
            var completed = 0;
            var insertedBatches = new List<FinancialBatch>();

            while ( batchesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} Financial Batches processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingBatchImportList.Take( Math.Min( this.DefaultChunkSize, workingBatchImportList.Count ) ).ToList();
                    rockContext.BulkInsert( batchesToInsert );
                    completed += insertedBatches.Count;
                    batchesRemainingToProcess -= csvChunk.Count;
                    workingBatchImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            LoadImportedBatches();
            return completed;
        }
    }
}