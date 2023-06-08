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
                var attributeForeignKey = row[AttributeId];
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
                    LogException( "Attribute", string.Format( "Entity Attribute Name cannot be blank for {0} {1}", entityTypeName, attributeForeignKey ) );
                }
                else
                {
                    var entityTypeId = entityTypes.FirstOrDefault( et => et.Name.Equals( entityTypeName ) ).Id;
                    var definedTypeForeignId = definedTypeIdString.AsType<int?>();
                    var fieldTypeId = TextFieldTypeId;
                    fieldTypeId = GetAttributeFieldType( attributeTypeString );

                    var fk = string.Empty;
                    if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                    {
                        fk = string.Format( "Bulldozer_{0}_{1}", categoryName.RemoveWhitespace(), attributeName.RemoveWhitespace() ).Left( 100 );
                    }
                    else
                    {
                        fk = attributeForeignKey;
                    }

                    var attribute = FindEntityAttribute( lookupContext, categoryName, attributeName, entityTypeId, attributeForeignKey, rockKey );
                    if ( attribute == null )
                    {
                        attribute = AddEntityAttribute( lookupContext, entityTypeId, entityTypeQualifierName, entityTypeQualifierValue, fk, categoryName, attributeName,
                            rockKey, fieldTypeId, true, definedTypeForeignId, definedTypeForeignKey, attributeTypeString: attributeTypeString );

                        addedItems++;
                    }
                    else if ( string.IsNullOrWhiteSpace( attribute.ForeignKey ) )
                    {
                        attribute = AddEntityAttribute( lookupContext, entityTypeId, entityTypeQualifierName, entityTypeQualifierValue, fk, categoryName, attributeName,
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
        private int LoadEntityAttributeValues( CSVInstance csvData )
        {
            var lookupContext = new RockContext();
            var importedAttributeValues = new AttributeValueService( lookupContext ).Queryable().Where( a => a.ForeignKey != null ).ToList();
            var importedAttributeValueCount = importedAttributeValues.Count();
            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();
            var attributeService = new AttributeService( lookupContext );
            var attributeValues = new List<AttributeValue>();

            var completedItems = 0;
            var addedItems = 0;

            int? entityTypeId = null;
            var prevEntityTypeName = string.Empty;
            var prevAttributeForeignKey = string.Empty;
            var prevRockKey = string.Empty;
            Attribute attribute = null;
            Type contextModelType = null;
            System.Data.Entity.DbContext contextDbContext = null;
            IService contextService = null;
            IHasAttributes entity = null;
            var prevAttributeValueEntityId = string.Empty;

            ReportProgress( 0, string.Format( "Verifying attribute value import ({0:N0} already imported).", importedAttributeValueCount ) );

            string[] row;
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                var entityTypeName = row[AttributeEntityTypeName];
                var attributeIdString = row[AttributeId];
                var rockKey = row[AttributeRockKey];
                var attributeValueForeignKey = row[AttributeValueId];
                var attributeValueEntityId = row[AttributeValueEntityId];
                var attributeValue = row[AttributeValue];
                var attributeForeignKey = $"{this.ImportInstanceFKPrefix}^{attributeIdString}";

                if ( !string.IsNullOrWhiteSpace( entityTypeName ) &&
                     !string.IsNullOrWhiteSpace( attributeValueEntityId ) &&
                     !string.IsNullOrWhiteSpace( attributeValue ) &&
                     ( !string.IsNullOrEmpty( attributeIdString ) || !string.IsNullOrEmpty( rockKey ) ) )
                {
                    var findNewEntity = false;

                    if ( !entityTypeId.HasValue || !prevEntityTypeName.Equals( entityTypeName, StringComparison.OrdinalIgnoreCase ) )
                    {
                        entityTypeId = entityTypes.FirstOrDefault( et => et.Name.Equals( entityTypeName ) ).Id;
                        prevEntityTypeName = entityTypeName;
                        findNewEntity = true;

                        contextModelType = entityTypes.FirstOrDefault( et => et.Name.Equals( entityTypeName ) ).GetEntityType();
                        contextDbContext = Reflection.GetDbContextForEntityType( contextModelType );
                        if ( contextDbContext != null )
                        {
                            contextService = Reflection.GetServiceForEntityType( contextModelType, contextDbContext );
                        }
                    }

                    if ( entityTypeId.HasValue && contextService != null )
                    {
                        if ( !string.IsNullOrWhiteSpace( attributeIdString ) && !prevAttributeForeignKey.Equals( attributeForeignKey, StringComparison.OrdinalIgnoreCase ) )
                        {
                            attribute = attributeService.GetByEntityTypeId( entityTypeId ).FirstOrDefault( a => a.ForeignKey == attributeForeignKey );
                            prevAttributeForeignKey = attributeForeignKey;
                            prevRockKey = string.Empty;
                        }
                        else if ( string.IsNullOrWhiteSpace( attributeIdString ) )
                        {
                            // if no FK provided force attribute to null so the rockKey is tested
                            attribute = null;
                        }

                        if ( attribute == null && !string.IsNullOrWhiteSpace( rockKey ) )
                        {
                            attribute = attributeService.GetByEntityTypeId( entityTypeId ).FirstOrDefault( a => a.Key == rockKey );
                            if ( !prevRockKey.Equals( rockKey, StringComparison.OrdinalIgnoreCase ) )
                            {
                                prevRockKey = rockKey;
                                prevAttributeForeignKey = string.Empty;
                            }
                        }

                        if ( attribute != null )
                        {
                            // set the fk if it wasn't for some reason
                            if ( string.IsNullOrWhiteSpace( attribute.ForeignKey ) && !string.IsNullOrWhiteSpace( attributeIdString ) )
                            {
                                var updatedAttributeRockContext = new RockContext();
                                var updatedAttributeService = new AttributeService( updatedAttributeRockContext );
                                var updatedAttribute = updatedAttributeService.GetByEntityTypeId( entityTypeId ).FirstOrDefault( a => a.Id == attribute.Id );
                                updatedAttribute.ForeignKey = attributeForeignKey;
                                updatedAttribute.ForeignId = attributeIdString.AsIntegerOrNull();
                                updatedAttributeRockContext.SaveChanges( DisableAuditing );
                            }

                            if ( entity == null || ( findNewEntity || !prevAttributeValueEntityId.Equals( attributeValueEntityId, StringComparison.OrdinalIgnoreCase ) ) )
                            {
                                MethodInfo qryMethod = contextService.GetType().GetMethod( "Queryable", new Type[] { } );
                                var entityQry = qryMethod.Invoke( contextService, new object[] { } ) as IQueryable<IEntity>;
                                entity = entityQry.FirstOrDefault( e => e.ForeignKey.Equals( attributeValueEntityId, StringComparison.OrdinalIgnoreCase ) ) as IHasAttributes;
                                prevAttributeValueEntityId = attributeValueEntityId;
                            }

                            if ( entity != null )
                            {
                                var av = CreateEntityAttributeValue( lookupContext, attribute, entity, attributeValue, attributeValueForeignKey );

                                if ( av != null && !attributeValues.Where( e => e.EntityId == av.EntityId ).Where( a => a.AttributeId == av.AttributeId ).Any() )
                                {
                                    attributeValues.Add( av );
                                    addedItems++;
                                }
                            }
                        }
                    }
                }

                completedItems++;
                if ( completedItems % ( DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} attribute values processed.", completedItems ) );
                }

                if ( completedItems % DefaultChunkSize < 1 )
                {
                    SaveAttributeValues( lookupContext, attributeValues, importedAttributeValues );
                    attributeValues.Clear();
                    lookupContext.Dispose();
                    lookupContext = new RockContext();
                    attributeService = new AttributeService( lookupContext );
                    attribute = null;

                    ReportPartialProgress();
                }
            }

            if ( attributeValues.Any() )
            {
                SaveAttributeValues( lookupContext, attributeValues, importedAttributeValues );
            }

            ReportProgress( 100, string.Format( "Finished attribute value import: {0:N0} attribute values imported.", addedItems ) );
            return completedItems;
        }

        /// <summary>
        /// Saves the attribute values.
        /// </summary>
        /// <param name="updatedEntityList">The updated entity list.</param>
        private static void SaveAttributeValues( RockContext rockContext, List<AttributeValue> attributeValues, List<AttributeValue> importedAttributeValues )
        {
            using ( rockContext )
            {
                rockContext.Configuration.AutoDetectChangesEnabled = false;
                var importedAttributeValueIds = importedAttributeValues.Select( av => av.Id );
                var newAttributeValues = attributeValues.Where( v => !importedAttributeValueIds.Any( id => id == v.Id ) ).ToList();
                rockContext.AttributeValues.AddRange( newAttributeValues );
                rockContext.SaveChanges( DisableAuditing );
            }
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
