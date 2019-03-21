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
            var accountList = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking().ToList();

            // Look for custom attributes in the Account file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > FinancialFundIsTaxDeductible )
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
                var isFundActiveKey = row[FinancialFundIsActive];
                var isFundActive = isFundActiveKey.AsType<bool?>();
                var fundStartDateKey = row[FinancialFundStartDate] as string;
                var fundStartDate = fundStartDateKey.AsType<DateTime?>();
                var fundEndDateKey = row[FinancialFundEndDate] as string;
                var fundEndDate = fundEndDateKey.AsType<DateTime?>();
                var fundOrderKey = row[FinancialFundOrder];
                var fundOrder = fundOrderKey.AsType<int?>();
                var fundParentIdKey = row[FinancialFundParentID];
                var fundParentId = fundParentIdKey.AsType<int?>();
                var fundPublicName = row[FinancialFundPublicName] as string;
                var isTaxDeductibleKey = row[FinancialFundIsTaxDeductible];
                var isTaxDeductible = isTaxDeductibleKey.AsType<bool?>();

                if ( !string.IsNullOrWhiteSpace( fundName ) )
                {
                    var account = accountList.FirstOrDefault( a => a.ForeignId.Equals( fundId ) || a.Name.Equals( fundName.Truncate( 50 ) ) );

                    //
                    // add account if doesn't exist
                    // AddAccount() auto saves the account
                    //
                    if ( account == null )
                    {
                        int? parentAccountId = null;
                        if ( fundParentId != null )
                        {
                            var parentAccount = accountList.FirstOrDefault( p => p.ForeignId.Equals( fundParentId ) );
                            if ( parentAccount != null )
                            {
                                parentAccountId = parentAccount.Id;
                            }
                        }

                        int? campusFundId = null;
                        var campusFund = CampusList.FirstOrDefault( c => fundName.Contains( c.Name ) || fundName.Contains( c.ShortCode ) );
                        if ( campusFund != null )
                        {
                            campusFundId = campusFund.Id;
                        }

                        account = AddAccount( lookupContext, fundName, fundGLAccount, campusFundId, parentAccountId, isFundActive, fundStartDate, fundEndDate, fundOrder, fundId, fundDescription, fundPublicName, isTaxDeductible );
                        accountList.Add( account );
                    }
                    else
                    {
                        if ( account.ForeignId == null )
                        {
                            FinancialAccount updateAccount;
                            var rockContext = new RockContext();
                            var accountService = new FinancialAccountService( rockContext );
                            updateAccount = accountService.Get( account.Id );
                            updateAccount.ForeignId = fundId;
                            updateAccount.ForeignKey = fundId.ToString();

                            rockContext.SaveChanges();
                            accountList = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking().ToList();
                        }
                    }

                    //
                    // Process Attributes for Account
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
                                LogException( "Financial Account", $"Financial Account Attribute Name cannot be blank '{newAttributePair.Value}'." );
                            }
                            else
                            {
                                //
                                // First try to find the existing attribute, if not found then add a new one.
                                //
                                if ( FindEntityAttribute( lookupContext, categoryName, attributeName, account.TypeId, attributeForeignKey ) == null )
                                {
                                    var fk = string.Empty;
                                    if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                                    {
                                        fk = $"Bulldozer_FinancialAccount_{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}".Left( 100 );
                                    }
                                    else
                                    {
                                        fk = attributeForeignKey;
                                    }

                                    AddEntityAttribute( lookupContext, account.TypeId, string.Empty, string.Empty, fk, categoryName, attributeName, string.Empty, fieldTypeId, true, definedValueForeignId, definedValueForeignKey, attributeTypeString: attributeTypeString );
                                }
                            }
                        }

                        //
                        // Add any Account attribute values
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
                                    var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, account.TypeId, attributeForeignKey );
                                    AddEntityAttributeValue( lookupContext, attribute, account, newValue, null, true );
                                }
                            }
                        }
                    }

                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} accounts imported." );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        lookupContext.SaveChanges();
                        ReportPartialProgress();
                    }
                }
            }

            lookupContext.SaveChanges();

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
                            if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                            {
                                ReportProgress( 0, $"{completedItems:N0} bank accounts imported." );
                            }

                            if ( completedItems % ReportingNumber < 1 )
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
            var newBatches = new List<FinancialBatch>();
            var earliestBatchDate = ImportDateTime;

            // Look for custom attributes in the Batch file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > BatchAmount )
                .ToDictionary( f => f.index, f => f.node.Name );

            var completed = 0;
            ReportProgress( 0, $"Verifying batch import ({ImportedBatches.Count:N0} already exist)." );
            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var batchIdKey = row[BatchID];
                if ( !string.IsNullOrWhiteSpace( batchIdKey ) && !ImportedBatches.ContainsKey( batchIdKey ) )
                {
                    var batch = new FinancialBatch
                    {
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ForeignKey = batchIdKey,
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
                        batch.CampusId = CampusList.Where( c => name.StartsWith( c.Name ) || name.StartsWith( c.ShortCode ) )
                            .Select( c => ( int? ) c.Id ).FirstOrDefault();
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
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} batches imported." );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        SaveFinancialBatches( newBatches );
                        newBatches.ForEach( b => ImportedBatches.Add( b.ForeignKey, ( int? ) b.Id ) );
                        newBatches.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            // add a default batch to use with contributions
            if ( !ImportedBatches.ContainsKey( "0" ) )
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
                newBatches.ForEach( b => ImportedBatches.Add( b.ForeignKey, ( int? ) b.Id ) );
            }

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
            }
        }

        /// <summary>
        /// Maps the contribution.
        /// </summary>
        /// <param name="csvData">The table data.</param>
        private int MapContribution( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var gatewayService = new FinancialGatewayService( lookupContext );
            var scheduledTransactionService = new FinancialScheduledTransactionService( lookupContext );

            var currencyTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE ) );
            var currencyTypeACH = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_ACH ) ) ).Id;
            var currencyTypeCash = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CASH ) ) ).Id;
            var currencyTypeCheck = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CHECK ) ) ).Id;
            var currencyTypeCreditCard = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CREDIT_CARD ) ) ).Id;
            var currencyTypeUnknown = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_UNKNOWN ) ) ).Id;
            var currencyTypeNonCash = currencyTypes.DefinedValues.Where( dv => dv.Value.Equals( "Non-Cash" ) ).Select( dv => ( int? ) dv.Id ).FirstOrDefault();
            if ( currencyTypeNonCash == null )
            {
                var newTenderNonCash = new DefinedValue
                {
                    Value = "Non-Cash",
                    Description = "Non-Cash",
                    DefinedTypeId = currencyTypes.Id
                };
                lookupContext.DefinedValues.Add( newTenderNonCash );
                lookupContext.SaveChanges();
                currencyTypeNonCash = newTenderNonCash.Id;
            }

            var creditCardTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE ) ).DefinedValues;

            var sourceTypeOnsite = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_ONSITE_COLLECTION ), lookupContext ).Id;
            var sourceTypeWebsite = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_WEBSITE ), lookupContext ).Id;
            var sourceTypeKiosk = DefinedValueCache.Get( new Guid( Rock.SystemGuid.DefinedValue.FINANCIAL_SOURCE_TYPE_KIOSK ), lookupContext ).Id;

            var refundReasons = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_TRANSACTION_REFUND_REASON ), lookupContext ).DefinedValues;

            var accountList = new FinancialAccountService( lookupContext ).Queryable().AsNoTracking().ToList();

            int? defaultBatchId = null;
            if ( ImportedBatches.ContainsKey( "0" ) )
            {
                defaultBatchId = ImportedBatches["0"];
            }

            // Look for custom attributes in the Contribution file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > ScheduledTransactionForeignKey )
                .ToDictionary( f => f.index, f => f.node.Name );

            // Get all imported contributions
            var importedContributions = new FinancialTransactionService( lookupContext )
                .Queryable().AsNoTracking()
                .Where( c => c.ForeignId != null )
                .Select( t => new { ForeignId = t.ForeignId, ForeignKey = t.ForeignKey } )
                .OrderBy( t => t ).ToList();

            // List for batching new contributions
            var newTransactions = new List<FinancialTransaction>();

            var completed = 0;
            ReportProgress( 0, $"Verifying contribution import ({importedContributions.Count:N0} already exist)." );
            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var individualIdKey = row[IndividualID];
                var contributionIdKey = row[ContributionID];
                var contributionId = contributionIdKey.AsType<int?>();
                var isAnonymous = ( bool ) ParseBoolOrDefault( row[IsAnonymous], false );


                if ( ( contributionId != null && !importedContributions.Any( c => c.ForeignId == contributionId ) ) ||
                    ( !string.IsNullOrWhiteSpace( contributionIdKey ) && !importedContributions.Any( c => c.ForeignKey == contributionIdKey ) ) )
                {
                    var transaction = new FinancialTransaction
                    {
                        CreatedByPersonAliasId = ImportPersonAliasId,
                        ModifiedByPersonAliasId = ImportPersonAliasId,
                        TransactionTypeValueId = TransactionTypeContributionId,
                        ForeignKey = contributionIdKey,
                        ForeignId = contributionId,
                        ShowAsAnonymous = isAnonymous
                    };

                    int? giverAliasId = null;
                    var personKeys = GetPersonKeys( individualIdKey );
                    if ( personKeys != null && personKeys.PersonAliasId > 0 )
                    {
                        giverAliasId = personKeys.PersonAliasId;
                        transaction.CreatedByPersonAliasId = giverAliasId;
                        transaction.AuthorizedPersonAliasId = giverAliasId;
                        transaction.ProcessedByPersonAliasId = giverAliasId;
                    }
                    else if ( AnonymousGiverAliasId != null && AnonymousGiverAliasId > 0 )
                    {
                        giverAliasId = AnonymousGiverAliasId;
                        transaction.AuthorizedPersonAliasId = giverAliasId;
                        transaction.ProcessedByPersonAliasId = giverAliasId;
                    }

                    var summary = row[Memo] as string;
                    if ( !string.IsNullOrWhiteSpace( summary ) )
                    {
                        transaction.Summary = summary;
                    }

                    //
                    // Find the gateway by ID number or by name. Error out if not found.
                    //
                    var rowGateway = row[Gateway];
                    if ( !string.IsNullOrWhiteSpace( rowGateway ) )
                    {
                        FinancialGateway gateway = null;
                        var rowGatewayId = rowGateway.AsType<int?>();
                        gateway = rowGatewayId.HasValue ? gatewayService.Queryable().FirstOrDefault( g => g.Id == rowGatewayId ) : gatewayService.Queryable().FirstOrDefault( g => g.Name.Equals( rowGateway, StringComparison.CurrentCultureIgnoreCase ) );

                        if ( gateway == null )
                        {
                            gateway = AddFinancialGateway( lookupContext, rowGateway );
                        }

                        transaction.FinancialGatewayId = gateway.Id;
                    }

                    var rowScheduledTransactionKey = row[ScheduledTransactionForeignKey];
                    if ( !string.IsNullOrWhiteSpace( rowScheduledTransactionKey ) )
                    {
                        var scheduledTransaction = scheduledTransactionService
                            .Queryable().AsNoTracking()
                            .FirstOrDefault( t => t.ForeignKey == rowScheduledTransactionKey );

                        if ( scheduledTransaction != null )
                        {
                            transaction.ScheduledTransactionId = scheduledTransaction.Id;
                        }
                    }

                    var batchKey = row[ContributionBatchID];
                    if ( !string.IsNullOrWhiteSpace( batchKey ) && ImportedBatches.Any( b => b.Key.Equals( batchKey ) ) )
                    {
                        transaction.BatchId = ImportedBatches.FirstOrDefault( b => b.Key.Equals( batchKey, StringComparison.CurrentCultureIgnoreCase ) ).Value;
                    }
                    else
                    {
                        // use the default batch for any non-matching transactions
                        transaction.BatchId = defaultBatchId;
                    }

                    var receivedDateKey = row[ReceivedDate];
                    var receivedDate = receivedDateKey.AsType<DateTime?>();
                    if ( receivedDate != null )
                    {
                        transaction.TransactionDateTime = receivedDate;
                        transaction.CreatedDateTime = receivedDate;
                        transaction.ModifiedDateTime = ImportDateTime;
                    }

                    var contributionType = row[ContributionTypeName];
                    var creditCardType = row[ContributionCreditCardType];

                    // set default source to onsite, exceptions listed below
                    transaction.SourceTypeValueId = sourceTypeOnsite;

                    int? paymentCurrencyTypeId = null, creditCardTypeId = null;

                    if ( string.IsNullOrWhiteSpace( contributionType ) )
                    {
                        paymentCurrencyTypeId = currencyTypeUnknown;
                    }
                    else if ( contributionType.Equals( "cash", StringComparison.CurrentCultureIgnoreCase ) )
                    {
                        paymentCurrencyTypeId = currencyTypeCash;
                    }
                    else if ( contributionType.Equals( "check", StringComparison.CurrentCultureIgnoreCase ) )
                    {
                        paymentCurrencyTypeId = currencyTypeCheck;
                    }
                    else if ( contributionType.Equals( "ach", StringComparison.CurrentCultureIgnoreCase ) )
                    {
                        paymentCurrencyTypeId = currencyTypeACH;
                        transaction.SourceTypeValueId = sourceTypeWebsite;
                    }
                    else if ( contributionType.Equals( "credit card", StringComparison.CurrentCultureIgnoreCase ) )
                    {
                        paymentCurrencyTypeId = currencyTypeCreditCard;
                        transaction.SourceTypeValueId = sourceTypeWebsite;

                        // Determine CC Type
                        if ( !string.IsNullOrWhiteSpace( creditCardType ) )
                        {
                            creditCardTypeId = creditCardTypes.Where( c => c.Value.StartsWith( creditCardType, StringComparison.CurrentCultureIgnoreCase )
                                    || c.Description.StartsWith( creditCardType, StringComparison.CurrentCultureIgnoreCase ) )
                                .Select( c => c.Id ).FirstOrDefault();
                        }
                    }
                    else if ( contributionType.Equals( "non-cash", StringComparison.CurrentCultureIgnoreCase ) || contributionType.Equals( "noncash", StringComparison.CurrentCultureIgnoreCase ) )
                    {
                        paymentCurrencyTypeId = currencyTypeNonCash;
                    }
                    else
                    {
                        var definedValue = FindDefinedValueByTypeAndName( lookupContext, currencyTypes.Guid, contributionType );
                        if ( definedValue == null )
                        {
                            definedValue = AddDefinedValue( new RockContext(), currencyTypes.Guid.ToString(), contributionType );
                        }

                        // todo parse, create lookup and fallback to unknown
                        if ( definedValue != null )
                        {
                            paymentCurrencyTypeId = definedValue.Id;
                        }
                        else
                        {
                            paymentCurrencyTypeId = currencyTypeUnknown;
                        }
                    }

                    var paymentDetail = new FinancialPaymentDetail
                    {
                        CreatedDateTime = receivedDate,
                        CreatedByPersonAliasId = giverAliasId,
                        ModifiedDateTime = ImportDateTime,
                        ModifiedByPersonAliasId = giverAliasId,
                        CurrencyTypeValueId = paymentCurrencyTypeId,
                        CreditCardTypeValueId = creditCardTypeId,
                        ForeignKey = contributionId.ToString(),
                        ForeignId = contributionId
                    };

                    transaction.FinancialPaymentDetail = paymentDetail;

                    var transactionCode = row[CheckNumber] as string;
                    // if transaction code provided, put it in the transaction code
                    if ( !string.IsNullOrEmpty( transactionCode ) )
                    {
                        transaction.TransactionCode = transactionCode;

                        // check for SecureGive kiosk transactions
                        if ( transactionCode.StartsWith( "SG" ) )
                        {
                            transaction.SourceTypeValueId = sourceTypeKiosk;
                        }
                    }

                    var fundName = row[FundName] as string;
                    fundName = fundName.Truncate( 50 );
                    var subFund = row[SubFundName] as string;
                    var fundGLAccount = row[FundGLAccount] as string;
                    var subFundGLAccount = row[SubFundGLAccount] as string;
                    var isFundActiveKey = row[FundIsActive];
                    var isFundActive = isFundActiveKey.AsType<bool?>();
                    var isSubFundActiveKey = row[SubFundIsActive];
                    var isSubFundActive = isSubFundActiveKey.AsType<bool?>();
                    var statedValueKey = row[StatedValue];
                    var statedValue = statedValueKey.AsType<decimal?>();
                    var amountKey = row[Amount];
                    var amount = amountKey.AsType<decimal?>();
                    if ( !string.IsNullOrWhiteSpace( fundName ) & amount != null )
                    {
                        int transactionAccountId;
                        var parentAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName ) || a.Name.EndsWith( fundName ) );
                        if ( parentAccount == null )
                        {
                            parentAccount = AddAccount( lookupContext, fundName, fundGLAccount, null, null, isFundActive, null, null, null, null, "", "", null );
                            accountList.Add( parentAccount );
                        }

                        if ( !string.IsNullOrWhiteSpace( subFund ) )
                        {
                            int? campusFundId = null;
                            // assign a campus if the subfund is a campus fund
                            var campusFund = CampusList.FirstOrDefault( c => subFund.Contains( c.Name ) || subFund.Contains( c.ShortCode ) );
                            if ( campusFund != null )
                            {
                                campusFundId = campusFund.Id;
                            }

                            // add info to easily find/assign this fund in the view
                            subFund = $"{fundName} {subFund}";

                            var childAccount = accountList.FirstOrDefault( c => c.Name.Equals( subFund.Truncate( 50 ) ) && c.ParentAccountId == parentAccount.Id );
                            if ( childAccount == null )
                            {
                                // create a child account with a campusId if it was set
                                childAccount = AddAccount( lookupContext, subFund, subFundGLAccount, campusFundId, parentAccount.Id, isSubFundActive, null, null, null, null, "", "", null );
                                accountList.Add( childAccount );
                            }

                            transactionAccountId = childAccount.Id;
                        }
                        else
                        {
                            transactionAccountId = parentAccount.Id;
                        }

                        if ( amount == 0 && statedValue != null && statedValue != 0 )
                        {
                            amount = statedValue;
                        }

                        var transactionDetail = new FinancialTransactionDetail
                        {
                            Amount = ( decimal ) amount,
                            CreatedDateTime = receivedDate,
                            AccountId = transactionAccountId
                        };
                        transaction.TransactionDetails.Add( transactionDetail );

                        if ( amount < 0 )
                        {
                            transaction.RefundDetails = new FinancialTransactionRefund();
                            transaction.RefundDetails.CreatedDateTime = receivedDate;
                            transaction.RefundDetails.RefundReasonValueId = refundReasons.Where( dv => summary != null && dv.Value.Contains( summary ) )
                                .Select( dv => ( int? ) dv.Id ).FirstOrDefault();
                            transaction.RefundDetails.RefundReasonSummary = summary;
                        }
                    }

                    //
                    // Process Attributes for Transaction
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
                                LogException( "Financial Transaction", $"Financial Transaction Attribute Name cannot be blank '{newAttributePair.Value}'." );
                            }
                            else
                            {
                                //
                                // First try to find the existing attribute, if not found then add a new one.
                                //
                                if ( FindEntityAttribute( lookupContext, categoryName, attributeName, transaction.TypeId, attributeForeignKey ) == null )
                                {
                                    var fk = string.Empty;
                                    if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                                    {
                                        fk = $"Bulldozer_FinancialTransaction_{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}".Left( 100 );
                                    }
                                    else
                                    {
                                        fk = attributeForeignKey;
                                    }

                                    AddEntityAttribute( lookupContext, transaction.TypeId, string.Empty, string.Empty, fk, categoryName, attributeName, string.Empty, fieldTypeId, true, definedValueForeignId, definedValueForeignKey, attributeTypeString: attributeTypeString );
                                }
                            }
                        }

                        //
                        // Add any Transaction attribute values
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
                                    var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, transaction.TypeId, attributeForeignKey );
                                    AddEntityAttributeValue( lookupContext, attribute, transaction, newValue, null, true );
                                }
                            }
                        }
                    }

                    newTransactions.Add( transaction );
                    completed++;
                    if ( completed % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completed:N0} contributions imported." );
                    }

                    if ( completed % ReportingNumber < 1 )
                    {
                        SaveContributions( newTransactions );
                        newTransactions.Clear();
                        ReportPartialProgress();
                    }
                }
            }

            if ( newTransactions.Any() )
            {
                SaveContributions( newTransactions );
            }

            ReportProgress( 100, $"Finished contribution import: {completed:N0} contributions imported." );
            return completed;
        }

        /// <summary>
        /// Saves the contributions.
        /// </summary>
        /// <param name="newTransactions">The new transactions.</param>
        private static void SaveContributions( List<FinancialTransaction> newTransactions )
        {
            using ( var rockContext = new RockContext() )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                rockContext.FinancialTransactions.AddRange( newTransactions );
                rockContext.SaveChanges( DisableAuditing );

                foreach ( var transaction in newTransactions )
                {
                    // Set attributes on this transaction
                    if ( transaction.Attributes != null )
                    {
                        foreach ( var attributeCache in transaction.Attributes.Select( a => a.Value ) )
                        {
                            var existingValue = rockContext.AttributeValues.FirstOrDefault( v => v.Attribute.Key == attributeCache.Key && v.EntityId == transaction.Id );
                            var newAttributeValue = transaction.AttributeValues[attributeCache.Key];

                            // set the new value and add it to the database
                            if ( existingValue == null )
                            {
                                existingValue = new AttributeValue
                                {
                                    AttributeId = newAttributeValue.AttributeId,
                                    EntityId = transaction.Id,
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
                            ForeignKey = pledgeIdKey,
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
                        var isFundActiveKey = row[FundIsActive];
                        var isFundActive = isFundActiveKey.AsType<bool?>();
                        var isSubFundActiveKey = row[SubFundIsActive];
                        var isSubFundActive = isSubFundActiveKey.AsType<bool?>();

                        if ( !string.IsNullOrWhiteSpace( fundName ) )
                        {
                            var parentAccount = accountList.FirstOrDefault( a => a.Name.Equals( fundName.Truncate( 50 ) ) );
                            if ( parentAccount == null )
                            {
                                parentAccount = AddAccount( lookupContext, fundName, string.Empty, null, null, isFundActive, null, null, null, null, "", "", null );
                                accountList.Add( parentAccount );
                            }

                            if ( !string.IsNullOrWhiteSpace( subFund ) )
                            {
                                int? campusFundId = null;
                                // assign a campus if the subfund is a campus fund
                                var campusFund = CampusList.FirstOrDefault( c => subFund.Contains( c.Name ) || subFund.Contains( c.ShortCode ) );
                                if ( campusFund != null )
                                {
                                    campusFundId = campusFund.Id;
                                }

                                // add info to easily find/assign this fund in the view
                                subFund = $"{fundName} {subFund}";

                                var childAccount = accountList.FirstOrDefault( c => c.Name.Equals( subFund.Truncate( 50 ) ) && c.ParentAccountId == parentAccount.Id );
                                if ( childAccount == null )
                                {
                                    // create a child account with a campusId if it was set
                                    childAccount = AddAccount( lookupContext, subFund, string.Empty, campusFundId, parentAccount.Id, isSubFundActive, null, null, null, null, "", "", null );
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
                        if ( completed % ( ReportingNumber * 10 ) < 1 )
                        {
                            ReportProgress( 0, $"{completed:N0} pledges imported." );
                        }

                        if ( completed % ReportingNumber < 1 )
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
            var accountService = new FinancialAccountService( lookupContext );
            var gatewayService = new FinancialGatewayService( lookupContext );
            var frequencyValues = DefinedTypeCache.Get( Rock.SystemGuid.DefinedType.FINANCIAL_FREQUENCY.AsGuid() ).DefinedValues;

            var currentTransaction = new FinancialScheduledTransaction();
            var newTransactionList = new List<FinancialScheduledTransaction>();
            var updatedTransactionList = new List<FinancialScheduledTransaction>();

            var currentTransactionKey = string.Empty;

            var currencyTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CURRENCY_TYPE ) );
            var currencyTypeACH = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_ACH ) ) ).Id;
            var currencyTypeCreditCard = currencyTypes.DefinedValues.FirstOrDefault( dv => dv.Guid.Equals( new Guid( Rock.SystemGuid.DefinedValue.CURRENCY_TYPE_CREDIT_CARD ) ) ).Id;

            var creditCardTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.FINANCIAL_CREDIT_CARD_TYPE ) ).DefinedValues;

            var completed = 0;
            var imported = new FinancialScheduledTransactionDetailService( lookupContext ).Queryable().AsNoTracking().Count( t => t.ForeignKey != null );
            ReportProgress( 0, $"Starting scheduled transaction import ({imported:N0} detail records already exist)." );
            imported = 0;

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowTransactionKey = row[ScheduledTransactionId];
                var rowTransactionId = rowTransactionKey.AsType<int?>();
                var rowPersonKey = row[ScheduledTransactionPersonId];

                //
                // Determine if we are still working with the same scheduled transaction or not.
                //
                if ( rowTransactionKey != null && rowTransactionKey != currentTransaction.ForeignKey )
                {
                    currentTransaction = newTransactionList.FirstOrDefault( t => t.ForeignKey == rowTransactionKey );
                    if ( currentTransaction == null )
                        currentTransaction = transactionService.Queryable().FirstOrDefault( t => t.ForeignKey == rowTransactionKey );
                    if ( currentTransaction == null )
                    {
                        currentTransaction = new FinancialScheduledTransaction
                        {
                            ForeignKey = rowTransactionKey,
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
                    }
                    else
                    {
                        throw new ArgumentOutOfRangeException( $"Cannot find person alias with person key '{rowPersonKey}' for scheduled transaction key {rowTransactionKey}" );
                    }

                    currentTransaction.CreatedDateTime = ParseDateOrDefault( row[ScheduledTransactionCreatedDate], ImportDateTime );
                    currentTransaction.ModifiedDateTime = ImportDateTime;
                    currentTransaction.CreatedByPersonAliasId = personKeys.PersonAliasId;
                    currentTransaction.ModifiedByPersonAliasId = personKeys.PersonAliasId;

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
                        : frequencyValues.FirstOrDefault( f => f.Value.Equals( rowFrequency, StringComparison.CurrentCultureIgnoreCase ) );

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
                    gateway = rowGatewayId.HasValue ? gatewayService.Queryable().FirstOrDefault( g => g.Id == rowGatewayId ) : gatewayService.Queryable().FirstOrDefault( g => g.Name.Equals( rowGateway, StringComparison.CurrentCultureIgnoreCase ) );

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
                        if ( currencyType.Equals( "ach", StringComparison.CurrentCultureIgnoreCase ) )
                        {
                            currencyTypeId = currencyTypeACH;
                        }
                        else if ( currencyType.Equals( "credit card", StringComparison.CurrentCultureIgnoreCase ) )
                        {
                            currencyTypeId = currencyTypeCreditCard;

                            // Determine CC Type
                            if ( !string.IsNullOrWhiteSpace( creditCardType ) )
                            {
                                creditCardTypeId = creditCardTypes.Where( c => c.Value.StartsWith( creditCardType, StringComparison.CurrentCultureIgnoreCase )
                                        || c.Description.StartsWith( creditCardType, StringComparison.CurrentCultureIgnoreCase ) )
                                    .Select( c => c.Id ).FirstOrDefault();
                            }
                        }

                        // Add Payment Details
                        var paymentDetail = new FinancialPaymentDetail
                        {
                            CreatedDateTime = ParseDateOrDefault( row[ScheduledTransactionCreatedDate], ImportDateTime ),
                            CreatedByPersonAliasId = personKeys.PersonAliasId,
                            ModifiedDateTime = ImportDateTime,
                            ModifiedByPersonAliasId = personKeys.PersonAliasId,
                            CurrencyTypeValueId = currencyTypeId,
                            CreditCardTypeValueId = creditCardTypeId,
                            ForeignKey = rowTransactionKey,
                            ForeignId = rowTransactionId
                        };

                        currentTransaction.FinancialPaymentDetail = paymentDetail;
                    }
                }

                //
                // We have the scheduled transaction setup, add in the individual account gift.
                //
                var rowAmount = row[ScheduledTransactionAmount].AsType<decimal?>();
                var rowAccount = row[ScheduledTransactionAccount];
                var rowAccountId = rowAccount.AsType<int?>();
                var account = rowAccountId.HasValue ? accountService.Queryable().FirstOrDefault( a => a.ForeignId == rowAccountId )
                    : accountService.Queryable().FirstOrDefault( a => a.Name.Equals( rowAccount, StringComparison.CurrentCultureIgnoreCase ) );

                if ( account == null && !rowAccountId.HasValue )
                {
                    var accountContext = new RockContext();
                    AddAccount( accountContext, rowAccount, string.Empty, null, null, true, null, null, null, null, "", "", null );
                    account = new FinancialAccountService( accountContext ).Queryable().FirstOrDefault( a => a.Name.Equals( rowAccount, StringComparison.CurrentCultureIgnoreCase ) );
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
                        ForeignKey = rowTransactionKey,
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
                if ( completed % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed:N0} scheduled transaction details imported." );
                }

                if ( completed % ReportingNumber < 1 )
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
        /// <param name="fundCampusId">The fund campus identifier.</param>
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
        private static FinancialAccount AddAccount( RockContext lookupContext, string fundName, string accountGL, int? fundCampusId, int? parentAccountId, bool? isActive, DateTime? startDate, DateTime? endDate, int? order, int? foreignId, string fundDescription, string fundPublicName, bool? isTaxDeductible )
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
                CampusId = fundCampusId,
                ParentAccountId = parentAccountId,
                CreatedByPersonAliasId = ImportPersonAliasId,
                StartDate = startDate,
                EndDate = endDate,
                ForeignId = foreignId,
                ForeignKey = foreignId.ToString()
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
    }
}