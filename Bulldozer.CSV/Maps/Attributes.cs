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
using Bulldozer.Model;
using Rock;
using Rock.Attribute;
using Rock.Data;
using Rock.Model;
using Rock.Security;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;
using Attribute = Rock.Model.Attribute;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the Attribute related import methods
    /// </summary>
    partial class CSVComponent
    {

        #region Attribute Methods

        /// <summary>
        /// Loads the Entity Attribute data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadEntityAttributes( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedAttributes = new AttributeService( lookupContext ).Queryable().Count( a => a.ForeignKey != null );
            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();
            var completedItems = 0;
            var addedItems = 0;

            ReportProgress( 0, "Begin Processing Entity Attributes" );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var entityTypeName = row[AttributeEntityTypeName];
                var entityTypeNameArray = entityTypeName.Split('.');
                var rockKey = row[AttributeRockKey];
                var attributeName = row[AttributeName];
                var categoryName = row[AttributeCategoryName];
                var attributeTypeString = row[AttributeType];
                var definedTypeIdString = row[AttributeDefinedTypeId];
                var entityTypeQualifierName = row[AttributeEntityTypeQualifierName];
                var entityTypeQualifierValue = row[AttributeEntityTypeQualifierValue];
                var definedTypeForeignKey = $"{this.ImportInstanceFKPrefix}^{definedTypeIdString}";

                if ( !string.IsNullOrWhiteSpace( entityTypeQualifierName ) && !string.IsNullOrWhiteSpace( entityTypeQualifierValue ) )
                {
                    entityTypeQualifierValue = GetEntityTypeQualifierValue( entityTypeQualifierName, entityTypeQualifierValue, lookupContext );
                }

                if ( string.IsNullOrEmpty( attributeName ) )
                {
                    LogException( "Attribute", $"Entity Attribute Name cannot be blank for {rockKey} ({entityTypeNameArray.LastOrDefault()})" );
                }
                else
                {
                    var entityTypeId = entityTypes.FirstOrDefault( et => et.Name.Equals( entityTypeName ) ).Id;
                    var definedTypeForeignId = definedTypeIdString.AsType<int?>();
                    var fieldTypeId = TextFieldTypeId;
                    fieldTypeId = GetAttributeFieldType( attributeTypeString );

                    var attributeForeignKey = $"{this.ImportInstanceFKPrefix}^{rockKey}_{entityTypeNameArray.LastOrDefault()}".Left( 100 );

                    var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, entityTypeId, attributeForeignKey, rockKey );
                    if ( attribute == null || attribute.ForeignKey.IsNullOrWhiteSpace() )
                    {
                        attribute = AddEntityAttribute( lookupContext, entityTypeId, entityTypeQualifierName, entityTypeQualifierValue, attributeForeignKey, categoryName, attributeName,
                            rockKey, fieldTypeId, true, definedTypeForeignId, definedTypeForeignKey, attributeTypeString: attributeTypeString );

                        addedItems++;
                    }

                    completedItems++;
                    if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} attributes processed.", completedItems ) );
                    }

                    if ( completedItems % DefaultChunkSize < 1 )
                    {
                        ReportPartialProgress();
                    }
                }
            }

            ReportProgress( 100, string.Format( "Finished attribute import: {0:N0} attributes imported.", addedItems ) );
            return completedItems;
        }

        #endregion Attribute Methods

        #region Entity Attribute Value Methods

        /// <summary>
        /// Loads the Entity Attribute data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadEntityAttributeValues()
        {
            this.ReportProgress( 0, "Preparing Entity Attribute Value data for import..." );

            var rockContext = new RockContext();
            var entityAVImports = new List<AttributeValueImport>();
            var errors = string.Empty;

            var definedTypeDict = DefinedTypeCache.All().ToDictionary( k => k.Id, v => v );
            var attributeValuesByEntityType = this.EntityAttributeValueCsvList
                                                .DistinctBy( av => new { av.AttributeKey, av.EntityId, av.EntityTypeName } )  // Protect against duplicates in import data
                                                .GroupBy( av => av.EntityTypeName )
                                                .Select( av => new { EntityTypeName = av.Key, AttributeValueCsvs = av.ToList() } )
                                                .ToList()
                                                .ToDictionary( k => k.EntityTypeName, v => v.AttributeValueCsvs );
            var attributeValueLookup = GetAttributeValueLookup( rockContext );
            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();
            var attributeLookup = new AttributeService( rockContext ).Queryable().ToList();

            foreach ( var entityType in attributeValuesByEntityType )
            {
                IService contextService = null;
                var entityTypeId = entityTypes.FirstOrDefault( et => et.Name.Equals( entityType.Key ) )?.Id;
                if ( entityTypeId.HasValue )
                {
                    this.ReportProgress( 0, $"Preparing Attribute Value data for {entityType.Key} EntityType..." );
                    var contextModelType = entityTypes.FirstOrDefault( et => et.Id.Equals( entityTypeId.Value ) ).GetEntityType();
                    var contextDbContext = Reflection.GetDbContextForEntityType( contextModelType );
                    if ( contextDbContext != null )
                    {
                        contextService = Reflection.GetServiceForEntityType( contextModelType, contextDbContext );
                    }

                    if ( contextService != null )
                    {
                        MethodInfo qryMethod = contextService.GetType().GetMethod( "Queryable", new Type[] { } );
                        var entityQry = qryMethod.Invoke( contextService, new object[] { } ) as IQueryable<IEntity>;
                        var entityIdDict = entityQry.Where( e => e.ForeignKey != null && e.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) ).ToDictionary( k => k.ForeignKey, v => v );
                        var attributeDict = attributeLookup.Where( a => a.EntityTypeId == entityTypeId ).ToDictionary( k => k.Key, v => v );
                        var attributeDefinedValuesDict = attributeLookup.Where( a => a.EntityTypeId == entityTypeId && a.FieldTypeId == DefinedValueFieldTypeId ).ToDictionary( k => k.Key, v => definedTypeDict.GetValueOrNull( v.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsIntegerOrNull().Value ).DefinedValues.ToDictionary( d => d.Value, d => d.Guid.ToString() ) );

                        foreach ( var attributeValueCsv in entityType.Value )
                        {
                            var entity = entityIdDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{attributeValueCsv.EntityId}" );
                            if ( entity == null )
                            {
                                errors += $"{DateTime.Now}, EntityAttributeValue, EntityId {attributeValueCsv.EntityId} not found for EntityTypeName {attributeValueCsv.EntityTypeName}. Entity AttributeValue for {attributeValueCsv.AttributeKey} attribute was skipped.\r\n";
                                continue;
                            }

                            var attribute = attributeDict.GetValueOrNull( attributeValueCsv.AttributeKey );
                            if ( attribute == null )
                            {
                                errors += $"{DateTime.Now}, EntityAttributeValue, AttributeKey {attributeValueCsv.AttributeKey} not found. AttributeValue for EntityId {attributeValueCsv.EntityId} of entity type {attributeValueCsv.EntityTypeName} was skipped.\r\n";
                                continue;
                            }

                            if ( attributeValueLookup.Any( l => l.Item1 == attribute.Id && l.Item2 == entity.Id ) )
                            {
                                errors += $"{DateTime.Now}, EntityAttributeValue, AttributeValue for AttributeKey {attributeValueCsv.AttributeKey} and EntityId {attributeValueCsv.EntityId} already exists. AttributeValueId {attributeValueCsv.AttributeValueId} was skipped.\r\n";
                                continue;
                            }

                            var newAttributeValue = new AttributeValueImport()
                            {
                                AttributeId = attribute.Id,
                                AttributeValueForeignId = attributeValueCsv.AttributeValueId.AsIntegerOrNull(),
                                EntityId = entity.Id,
                                AttributeValueForeignKey = string.Format( "{0}^{1}", this.ImportInstanceFKPrefix, attributeValueCsv.AttributeValueId.IsNotNullOrWhiteSpace() ? attributeValueCsv.AttributeValueId : string.Format( "{0}_{1}", attributeValueCsv.EntityId, attributeValueCsv.AttributeKey ) )
                            };
                            newAttributeValue.Value = GetAttributeValueStringByAttributeType( rockContext, attributeValueCsv.AttributeValue, attribute, attributeDefinedValuesDict );

                            entityAVImports.Add( newAttributeValue );
                        }
                    }
                }
            }

            this.ReportProgress( 0, string.Format( "Begin processing {0} Entity Attribute Value Records...", entityAVImports.Count ) );
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }
            return ImportAttributeValues( entityAVImports );
        }

        public string GetAttributeValueStringByAttributeType( RockContext rockContext, string value, Attribute attribute, Dictionary<string, Dictionary<string,string>> attributeDefinedValuesDict )
        {
            string newValue = null;
            if ( attribute.FieldTypeId == DateFieldTypeId )
            {
                var dateValue = ParseDateOrDefault( value, null );
                if ( dateValue != null && dateValue != DefaultDateTime && dateValue != DefaultSQLDateTime )
                {
                    newValue = ( ( DateTime ) dateValue ).ToString( "s" );
                }
            }
            else if ( attribute.FieldTypeId == BooleanFieldTypeId )
            {
                var boolValue = ParseBoolOrDefault( value, null );
                if ( boolValue != null )
                {
                    newValue = ( ( bool ) boolValue ).ToString();
                }
            }
            else if ( attribute.FieldTypeId == DefinedValueFieldTypeId )
            {
                var allowMultiple = false;
                if ( attribute.AttributeQualifiers != null )
                {
                    var allowMultipleQualifier = attribute.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "allowmultiple" );
                    if ( allowMultipleQualifier != null )
                    {
                        allowMultiple = allowMultipleQualifier.Value.AsBoolean( false );
                    }
                }
                if ( !allowMultiple )
                {
                    newValue = attributeDefinedValuesDict.GetValueOrNull( attribute.Key )?.GetValueOrNull( value );
                }
                if ( newValue.IsNullOrWhiteSpace() )
                {
                    Guid definedValueGuid;
                    var definedTypeId = attribute.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsInteger();
                    var attributeDVType = DefinedTypeCache.Get( definedTypeId );

                    if ( allowMultiple )
                    {
                        //
                        // Check for multiple and walk the loop
                        //
                        var valueList = new List<string>();
                        var values = value.Split( new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries ).ToList();

                        foreach ( var v in values )
                        {
                            //
                            // Add the defined value if it doesn't exist.
                            //
                            var attributeDefinedValue = FindDefinedValueByTypeAndName( new RockContext(), attributeDVType.Guid, v.Trim() );
                            if ( attributeDefinedValue == null )
                            {
                                attributeDefinedValue = AddDefinedValue( new RockContext(), attributeDVType.Guid.ToString(), v.Trim() );
                            }

                            definedValueGuid = attributeDefinedValue.Guid;

                            valueList.Add( definedValueGuid.ToString().ToUpper() );
                        }

                        //
                        // Convert list of Guids to single comma delimited string
                        //
                        newValue = valueList.AsDelimited( "," );
                    }
                    else
                    {
                        //
                        // Add the defined value if it doesn't exist.
                        //
                        var attributeDefinedValue = FindDefinedValueByTypeAndName( new RockContext(), attributeDVType.Guid, value );
                        if ( attributeDefinedValue == null )
                        {
                            attributeDefinedValue = AddDefinedValue( new RockContext(), attributeDVType.Guid.ToString(), value );
                        }

                        definedValueGuid = attributeDefinedValue.Guid;
                        newValue = definedValueGuid.ToString().ToUpper();
                    }
                }
            }
            else if ( attribute.FieldTypeId == ValueListFieldTypeId )
            {
                if ( attributeDefinedValuesDict != null )
                {
                    newValue = attributeDefinedValuesDict.GetValueOrNull( attribute.Key )?.GetValueOrNull( value );
                }

                if ( newValue.IsNullOrWhiteSpace() )
                {
                    int definedTypeId = attribute.AttributeQualifiers.FirstOrDefault( aq => aq.Key == "definedtype" ).Value.AsInteger();
                    var attributeValueTypes = DefinedTypeCache.Get( definedTypeId, rockContext );

                    var dvRockContext = new RockContext();
                    dvRockContext.Configuration.AutoDetectChangesEnabled = false;

                    //
                    // Check for multiple and walk the loop
                    //
                    var valueList = new List<string>();
                    var values = value.Split( new char[] { '^' }, StringSplitOptions.RemoveEmptyEntries ).ToList();
                    foreach ( var v in values )
                    {
                        if ( definedTypeId > 0 )
                        {
                            //
                            // Add the defined value if it doesn't exist.
                            //
                            var attributeExists = attributeValueTypes.DefinedValues.Any( a => a.Value.Equals( v ) );
                            if ( !attributeExists )
                            {
                                var newDefinedValue = new DefinedValue
                                {
                                    DefinedTypeId = attributeValueTypes.Id,
                                    Value = v,
                                    Order = 0,
                                    ForeignKey = $"{this.ImportInstanceFKPrefix}^AT_{attribute.Id}"
                                };

                                DefinedTypeCache.Remove( attributeValueTypes.Id );

                                dvRockContext.DefinedValues.Add( newDefinedValue );
                                dvRockContext.SaveChanges( DisableAuditing );

                                valueList.Add( newDefinedValue.Id.ToString() );
                            }
                            else
                            {
                                valueList.Add( attributeValueTypes.DefinedValues.FirstOrDefault( a => a.Value.Equals( v ) ).Id.ToString() );
                            }
                        }
                        else
                        {
                            valueList.Add( v );
                        }
                    }

                    //
                    // Convert list of Ids to single pipe delimited string
                    //
                    newValue = valueList.AsDelimited( "|", "|" );
                }
            }
            else if ( attribute.FieldTypeId == EncryptedTextFieldTypeId || attribute.FieldTypeId == SsnFieldTypeId )
            {
                newValue = Encryption.EncryptString( value );
            }
            else
            {
                newValue = value;
            }
            return newValue;
        }

        #endregion Entity Attribute Value Methods

    }
}
