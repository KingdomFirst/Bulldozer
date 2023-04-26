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
using System.Globalization;
using System.Linq;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Security;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Content Channel related import methods
    /// </summary>
    partial class CSVComponent
    {
        #region ContentChannel Methods

        /// <summary>
        /// Loads the Content Channel data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadContentChannel( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var contentChannelService = new ContentChannelService( lookupContext );
            var contentChannelTypeService = new ContentChannelTypeService( lookupContext );
            var groupService = new GroupService( lookupContext );

            // Look for custom attributes in the Content Channel file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > ContentChannelParentId )
                .ToDictionary( f => f.index, f => f.node.Name );

            var completed = 0;
            var importedCount = 0;
            var alreadyImportedCount = contentChannelService.Queryable().AsNoTracking().Count( c => c.ForeignKey != null );
            ReportProgress( 0, $"Starting Content Channel import ({alreadyImportedCount:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowContentChannelName = row[ContentChannelName];
                var rowContentChannelTypeName = row[ContentChannelTypeName];
                var rowContentChannelDescription = row[ContentChannelDescription];
                var rowContentChannelId = row[ContentChannelId];
                var rowContentChannelRequiresApproval = row[ContentChannelRequiresApproval];
                var rowContentChannelParentId = row[ContentChannelParentId];

                var rowChannelId = rowContentChannelId.AsType<int?>();

                var requiresApproval = ( bool ) ParseBoolOrDefault( rowContentChannelRequiresApproval, false );

                var contentChannelTypeId = 0;
                if ( contentChannelTypeService.Queryable().AsNoTracking().FirstOrDefault( t => t.Name.ToLower() == rowContentChannelTypeName.ToLower() ) != null )
                {
                    contentChannelTypeId = contentChannelTypeService.Queryable().AsNoTracking().FirstOrDefault( t => t.Name.ToLower() == rowContentChannelTypeName.ToLower() ).Id;
                }

                //
                // Verify the Content Channel Type Exists
                //
                if ( contentChannelTypeId < 1 )
                {
                    var newConentChannelType = new ContentChannelType
                    {
                        Name = rowContentChannelTypeName,
                        DateRangeType = ContentChannelDateType.DateRange,
                        IncludeTime = true,
                        DisablePriority = true,
                        CreatedDateTime = ImportDateTime
                    };

                    lookupContext.ContentChannelTypes.Add( newConentChannelType );
                    lookupContext.SaveChanges( DisableAuditing );

                    contentChannelTypeId = lookupContext.ContentChannelTypes.FirstOrDefault( t => t.Name == rowContentChannelTypeName ).Id;
                }

                //
                // Check that this Content Channel doesn't already exist.
                //
                var exists = false;
                if ( alreadyImportedCount > 0 )
                {
                    exists = contentChannelService.Queryable().AsNoTracking().Any( c => c.ForeignKey == rowContentChannelId );
                }

                if ( !exists )
                {
                    //
                    // Create and populate the new Content Channel.
                    //
                    var contentChannel = new ContentChannel
                    {
                        Name = rowContentChannelName,
                        Description = rowContentChannelDescription,
                        ContentChannelTypeId = contentChannelTypeId,
                        ForeignKey = rowContentChannelId,
                        ForeignId = rowChannelId,
                        ContentControlType = ContentControlType.HtmlEditor,
                        RequiresApproval = requiresApproval
                    };

                    //
                    // Look for Parent Id and create appropriate objects.
                    //
                    if ( !string.IsNullOrWhiteSpace( rowContentChannelParentId ) )
                    {
                        var ParentChannels = new List<ContentChannel>();
                        var parentChannel = contentChannelService.Queryable().FirstOrDefault( p => p.ForeignKey == rowContentChannelParentId );
                        if ( parentChannel.ForeignKey == rowContentChannelParentId )
                        {
                            ParentChannels.Add( parentChannel );
                            contentChannel.ParentContentChannels = ParentChannels;
                        }
                    }

                    // Save changes for context
                    lookupContext.WrapTransaction( () =>
                    {
                        lookupContext.ContentChannels.Add( contentChannel );
                        lookupContext.SaveChanges( DisableAuditing );
                    } );

                    // Set security if needed
                    if ( contentChannel.RequiresApproval )
                    {
                        var rockAdmins = groupService.Get( Rock.SystemGuid.Group.GROUP_ADMINISTRATORS.AsGuid() );
                        contentChannel.AllowSecurityRole( Authorization.APPROVE, rockAdmins, lookupContext );

                        var communicationAdmins = groupService.Get( Rock.SystemGuid.Group.GROUP_COMMUNICATION_ADMINISTRATORS.AsGuid() );
                        contentChannel.AllowSecurityRole( Authorization.APPROVE, communicationAdmins, lookupContext );

                        // Save security changes
                        lookupContext.WrapTransaction( () =>
                        {
                            lookupContext.SaveChanges( DisableAuditing );
                        } );
                    }

                    //
                    // Process Attributes for Content Channels
                    //
                    if ( customAttributes.Any() )
                    {
                        // create content channel attributes
                        foreach ( var newAttributePair in customAttributes )
                        {
                            var pairs = newAttributePair.Value.Split( '^' );
                            var categoryName = string.Empty;
                            var attributeName = string.Empty;
                            var attributeTypeString = string.Empty;
                            var attributeIdString = string.Empty;
                            var definedTypeIdString = string.Empty;
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
                                    attributeIdString = pairs[4];
                                }
                                if ( pairs.Length >= 6 )
                                {
                                    definedTypeIdString = pairs[5];
                                }
                            }

                            var definedTypeForeignKey = $"{this.ImportInstanceFKPrefix}^{definedTypeIdString}";
                            var definedTypeForeignId = definedTypeIdString.AsType<int?>();

                            //
                            // Translate the provided attribute type into one we know about.
                            //
                            fieldTypeId = GetAttributeFieldType( attributeTypeString );

                            if ( string.IsNullOrEmpty( attributeName ) )
                            {
                                LogException( "Content Channel Type", $"Content Channel Type Channel Attribute Name cannot be blank '{newAttributePair.Value}'." );
                            }
                            else
                            {
                                var fk = string.Empty;
                                if ( string.IsNullOrWhiteSpace( attributeIdString ) )
                                {
                                    fk = $"{this.ImportInstanceFKPrefix}^ContentChannelType_{contentChannelTypeId}_{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}".Left( 100 );
                                }
                                else
                                {
                                    fk = $"{this.ImportInstanceFKPrefix}^{attributeIdString}";
                                }

                                AddEntityAttribute( lookupContext, contentChannel.TypeId, "ContentChannelTypeId", contentChannelTypeId.ToString(), fk, categoryName, attributeName, string.Empty, fieldTypeId, true, definedTypeForeignId, definedTypeForeignKey, attributeTypeString: attributeTypeString );
                            }
                        }

                        //
                        // Add any Content Channel attribute values
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
                                    string fk = string.Empty;
                                    if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                                    {
                                        fk = $"Bulldozer_ContentChannelType_{contentChannelTypeId}_{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}".Left( 100 );
                                    }
                                    else
                                    {
                                        fk = attributeForeignKey;
                                    }

                                    var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, contentChannel.TypeId, fk );
                                    AddEntityAttributeValue( lookupContext, attribute, contentChannel, newValue, null, true );
                                }
                            }
                        }
                    }

                    importedCount++;
                }

                //
                // Notify user of our status.
                //
                completed++;
                if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed:N0} Content Channel records processed, {importedCount:N0} imported." );
                }

                if ( completed % DefaultChunkSize < 1 )
                {
                    lookupContext.SaveChanges();
                    ReportPartialProgress();

                    // Clear out variables
                    contentChannelService = new ContentChannelService( lookupContext );
                }
            }

            //
            // Save any other changes to existing items.
            //
            lookupContext.SaveChanges();
            lookupContext.Dispose();

            ReportProgress( 0, $"Finished Content Channel import: {importedCount:N0} records added." );

            return completed;
        }

        #endregion ContentChannel Methods

        #region ContentChannelItem Methods

        /// <summary>
        /// Loads the ContentChannelItem data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadContentChannelItem( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var contentChannelItemService = new ContentChannelItemService( lookupContext );
            var contentChannelService = new ContentChannelService( lookupContext );
            var contentChannelTypeService = new ContentChannelTypeService( lookupContext );

            // Look for custom attributes in the Content Channel file
            var allFields = csvData.TableNodes.FirstOrDefault().Children.Select( ( node, index ) => new { node = node, index = index } ).ToList();
            var customAttributes = allFields
                .Where( f => f.index > ItemParentId )
                .ToDictionary( f => f.index, f => f.node.Name );

            // Set the supported date formats
            var dateFormats = new[] { "yyyy-MM-dd", "MM/dd/yyyy", "MM/dd/yy",
                                      "M/d/yyyy", "M/dd/yyyy",
                                      "M/d/yyyy h:mm:ss tt", "M/d/yyyy h:mm tt",
                                      "MM/dd/yyyy hh:mm:ss", "M/d/yyyy h:mm:ss",
                                      "M/d/yyyy hh:mm tt", "M/d/yyyy hh tt",
                                      "M/d/yyyy h:mm", "M/d/yyyy h:mm",
                                      "MM/dd/yyyy hh:mm", "M/dd/yyyy hh:mm",
                                      "yyyy-MM-dd HH:mm:ss" };

            var importedChannelIds = new List<int>();

            var completed = 0;
            var importedCount = 0;
            var alreadyImportedCount = contentChannelItemService.Queryable().AsNoTracking().Count( i => i.ForeignKey != null );
            ReportProgress( 0, $"Starting Content Channel Item import ({alreadyImportedCount:N0} already exist)." );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var rowContentChannelName = row[ContentChannelName];
                var rowContentChannelItemTitle = row[ItemTitle];
                var rowContentChannelItemContent = row[ItemContent];
                var rowContentChannelItemId = row[ItemId];
                var rowContentChannelItemParentId = row[ItemParentId];

                var rowChannelItemId = rowContentChannelItemId.AsType<int?>();

                ContentChannel contentChannel = null;
                if ( contentChannelService.Queryable().AsNoTracking().FirstOrDefault( t => t.Name.ToLower() == rowContentChannelName.ToLower() ) != null )
                {
                    contentChannel = contentChannelService.Queryable().AsNoTracking().FirstOrDefault( c => c.Name.ToLower() == rowContentChannelName.ToLower() );
                }

                //
                // Verify the Content Channel exists.
                //
                if ( contentChannel.Id < 1 )
                {
                    throw new System.Collections.Generic.KeyNotFoundException( $"Content Channel {rowContentChannelName} not found", null );
                }

                //
                // Get content channel type
                //
                var contentChannelTypeId = contentChannelService.Queryable().AsNoTracking().FirstOrDefault( c => c.Id == contentChannel.Id ).ContentChannelTypeId;

                //
                // Check that this Content Channel Item doesn't already exist.
                //
                var exists = false;
                if ( alreadyImportedCount > 0 )
                {
                    exists = contentChannelItemService.Queryable().AsNoTracking().Any( i => i.ForeignKey == rowContentChannelItemId );
                }

                if ( !exists )
                {
                    //
                    // Create and populate the new Content Channel.
                    //
                    var contentChannelItem = new ContentChannelItem
                    {
                        Title = rowContentChannelItemTitle,
                        Status = ContentChannelItemStatus.Approved,
                        Content = rowContentChannelItemContent,
                        ForeignKey = rowContentChannelItemId,
                        ForeignId = rowChannelItemId,
                        ContentChannelId = contentChannel.Id,
                        ContentChannelTypeId = contentChannelTypeId
                    };

                    DateTime startDateValue;
                    if ( DateTime.TryParseExact( row[ItemStart], dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out startDateValue ) )
                    {
                        contentChannelItem.StartDateTime = startDateValue;
                    }

                    DateTime expireDateValue;
                    if ( DateTime.TryParseExact( row[ItemExpire], dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None, out expireDateValue ) && expireDateValue != System.DateTime.MinValue )
                    {
                        contentChannelItem.ExpireDateTime = expireDateValue;
                    }

                    if ( contentChannel.RequiresApproval )
                    {
                        contentChannelItem.Status = ContentChannelItemStatus.Approved;
                        contentChannelItem.ApprovedDateTime = ImportDateTime;
                        contentChannelItem.ApprovedByPersonAliasId = ImportPersonAliasId;
                    }

                    // Save changes for context
                    lookupContext.WrapTransaction( () =>
                    {
                        lookupContext.ContentChannelItems.Add( contentChannelItem );
                        lookupContext.SaveChanges( DisableAuditing );
                    } );

                    //
                    // Look for Parent Id and create appropriate objects.
                    //
                    if ( !string.IsNullOrWhiteSpace( rowContentChannelItemParentId ) )
                    {
                        var parentFound = false;
                        parentFound = contentChannelItemService.Queryable().AsNoTracking().Any( i => i.ForeignKey == rowContentChannelItemParentId );

                        if ( parentFound )
                        {
                            var parentItem = contentChannelItemService.Queryable().FirstOrDefault( i => i.ForeignKey == rowContentChannelItemParentId );

                            var service = new ContentChannelItemAssociationService( lookupContext );
                            var order = service.Queryable().AsNoTracking()
                                .Where( a => a.ContentChannelItemId == parentItem.Id )
                                .Select( a => ( int? ) a.Order )
                                .DefaultIfEmpty()
                                .Max();

                            var assoc = new ContentChannelItemAssociation();
                            assoc.ContentChannelItemId = parentItem.Id;
                            assoc.ChildContentChannelItemId = contentChannelItem.Id;
                            assoc.Order = order.HasValue ? order.Value + 1 : 0;
                            service.Add( assoc );

                            lookupContext.SaveChanges( DisableAuditing );
                        }
                    }

                    //
                    // Process Attributes for Content Channel Items
                    //
                    if ( customAttributes.Any() )
                    {
                        // create content channel item attributes, but only if not already processed in this csv file
                        if ( !importedChannelIds.Contains( contentChannel.Id ) )
                        {
                            // add current content channel id to list so we don't process multiple times
                            importedChannelIds.Add( contentChannel.Id );

                            // create content channel item attributes
                            foreach ( var newAttributePair in customAttributes )
                            {
                                var pairs = newAttributePair.Value.Split( '^' );
                                var categoryName = string.Empty;
                                var attributeName = string.Empty;
                                var attributeTypeString = string.Empty;
                                var attributeIdString = string.Empty;
                                var definedTypeIdString = string.Empty;
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
                                        attributeIdString = pairs[4];
                                    }
                                    if ( pairs.Length >= 6 )
                                    {
                                        definedTypeIdString = pairs[5];
                                    }
                                }

                                var definedTypeForeignKey = $"{this.ImportInstanceFKPrefix}^{definedTypeIdString}";
                                var definedTypeForeignId = definedTypeIdString.AsType<int?>();

                                //
                                // Translate the provided attribute type into one we know about.
                                //
                                fieldTypeId = GetAttributeFieldType( attributeTypeString );

                                if ( string.IsNullOrEmpty( attributeName ) )
                                {
                                    LogException( $"Content Channel {contentChannelItem.ContentChannel.Name}", $"Content Channel {contentChannelItem.ContentChannel.Name} Item Attribute Name cannot be blank '{newAttributePair.Value}'." );
                                }
                                else
                                {
                                    //
                                    // First try to find the existing attribute, if not found then add a new one.
                                    //
                                    var fk = string.Empty;
                                    if ( string.IsNullOrWhiteSpace( attributeIdString ) )
                                    {
                                        fk = $"{this.ImportInstanceFKPrefix}^ContentChannelItem_{contentChannel.Name.RemoveWhitespace()}_{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}".Left( 100 );
                                    }
                                    else
                                    {
                                        fk = $"{this.ImportInstanceFKPrefix}^{attributeIdString}";
                                    }

                                    AddEntityAttribute( lookupContext, contentChannelItem.TypeId, "ContentChannelId", contentChannelItem.ContentChannelId.ToString(), fk, categoryName, attributeName, string.Empty, fieldTypeId, true, definedTypeForeignId, definedTypeForeignKey, attributeTypeString: attributeTypeString );
                                }
                            } // end add attributes
                        } // end test for first run

                        //
                        // Add any Content Channel Item attribute values
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
                                    string fk = string.Empty;
                                    if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                                    {
                                        fk = $"Bulldozer_ContentChannelItem_{contentChannel.Name.RemoveWhitespace()}_{categoryName.RemoveWhitespace()}_{attributeName.RemoveWhitespace()}".Left( 100 );
                                    }
                                    else
                                    {
                                        fk = attributeForeignKey;
                                    }

                                    var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, contentChannelItem.TypeId, fk );
                                    AddEntityAttributeValue( lookupContext, attribute, contentChannelItem, newValue, null, true );
                                }
                            }
                        } // end attribute value processing
                    } // end custom attribute processing

                    importedCount++;
                }

                //
                // Notify user of our status.
                //
                completed++;
                if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed:N0} Content Channel records processed, {importedCount:N0} imported." );
                }

                if ( completed % DefaultChunkSize < 1 )
                {
                    lookupContext.SaveChanges();
                    ReportPartialProgress();

                    // Clear out variables
                    contentChannelService = new ContentChannelService( lookupContext );
                }
            }

            //
            // Save any other changes to existing items.
            //
            lookupContext.SaveChanges();
            lookupContext.Dispose();

            ReportProgress( 0, $"Finished Content Channel Item import: {importedCount:N0} records added." );

            return completed;
        }

        #endregion ContentChannelItem Methods
    }
}