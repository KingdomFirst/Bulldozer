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
using System.Linq;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using Attribute = Rock.Model.Attribute;
using static Bulldozer.Utility.CachedTypes;
using static Bulldozer.Utility.Extensions;
using Rock.Attribute;
using System.Reflection;

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

            ReportProgress( 0, string.Format( "Verifying attribute import ({0:N0} already imported).", importedAttributes ) );

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
                var definedValueForeignKey = row[AttributeDefinedTypeId];
                var entityTypeQualifierName = row[AttributeEntityTypeQualifierName];
                var entityTypeQualifierValue = row[AttributeEntityTypeQualifierValue];

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
                    var definedValueForeignId = definedValueForeignKey.AsType<int?>();
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
                            rockKey, fieldTypeId, true, definedValueForeignId, definedValueForeignKey, attributeTypeString: attributeTypeString );

                        addedItems++;
                    }
                    else if ( string.IsNullOrWhiteSpace( attribute.ForeignKey ) )
                    {
                        attribute = AddEntityAttribute( lookupContext, entityTypeId, entityTypeQualifierName, entityTypeQualifierValue, fk, categoryName, attributeName,
                            rockKey, fieldTypeId, true, definedValueForeignId, definedValueForeignKey, attributeTypeString: attributeTypeString );

                        addedItems++;
                    }

                    completedItems++;
                    if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} attributes processed.", completedItems ) );
                    }

                    if ( completedItems % ReportingNumber < 1 )
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
                var attributeForeignKey = row[AttributeId];
                var rockKey = row[AttributeRockKey];
                var attributeValueForeignKey = row[AttributeValueId];
                var attributeValueEntityId = row[AttributeValueEntityId];
                var attributeValue = row[AttributeValue];

                if ( !string.IsNullOrWhiteSpace( entityTypeName ) &&
                     !string.IsNullOrWhiteSpace( attributeValueEntityId ) &&
                     !string.IsNullOrWhiteSpace( attributeValue ) &&
                     ( !string.IsNullOrEmpty( attributeForeignKey ) || !string.IsNullOrEmpty( rockKey ) ) )
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
                        if ( !string.IsNullOrWhiteSpace( attributeForeignKey ) && !prevAttributeForeignKey.Equals( attributeForeignKey, StringComparison.OrdinalIgnoreCase ) )
                        {
                            attribute = attributeService.GetByEntityTypeId( entityTypeId ).FirstOrDefault( a => a.ForeignKey == attributeForeignKey );
                            prevAttributeForeignKey = attributeForeignKey;
                            prevRockKey = string.Empty;
                        }
                        else if ( string.IsNullOrWhiteSpace( attributeForeignKey ) )
                        {
                            // if no FK provided force attribute to null so the rockKey is tested
                            attribute = null;
                        }

                        if ( attribute == null && !string.IsNullOrWhiteSpace( rockKey ) && !prevRockKey.Equals( rockKey, StringComparison.OrdinalIgnoreCase ) )
                        {
                            attribute = attributeService.GetByEntityTypeId( entityTypeId ).FirstOrDefault( a => a.Key == rockKey );
                            prevRockKey = rockKey;
                            prevAttributeForeignKey = string.Empty;
                        }

                        if ( attribute != null )
                        {
                            // set the fk if it wasn't for some reason
                            if ( string.IsNullOrWhiteSpace( attribute.ForeignKey ) && !string.IsNullOrWhiteSpace( attributeForeignKey ) )
                            {
                                var updatedAttributeRockContext = new RockContext();
                                var updatedAttributeService = new AttributeService( updatedAttributeRockContext );
                                var updatedAttribute = updatedAttributeService.GetByEntityTypeId( entityTypeId ).FirstOrDefault( a => a.Id == attribute.Id );
                                updatedAttribute.ForeignKey = attributeForeignKey;
                                updatedAttribute.ForeignId = attributeForeignKey.AsIntegerOrNull();
                                updatedAttributeRockContext.SaveChanges( DisableAuditing );
                            }

                            if ( entity == null || ( findNewEntity || !prevAttributeValueEntityId.Equals( attributeValueEntityId, StringComparison.OrdinalIgnoreCase ) ) )
                            {
                                MethodInfo qryMethod = contextService.GetType().GetMethod( "Queryable", new Type[] { } );
                                var entityQry = qryMethod.Invoke( contextService, new object[] { } ) as IQueryable<IEntity>;
                                var entityResult = entityQry.Where( e => e.ForeignKey.Equals( attributeValueEntityId, StringComparison.OrdinalIgnoreCase ) );
                                entity = entityResult.FirstOrDefault() as IHasAttributes;
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
                if ( completedItems % ( ReportingNumber * 10 ) < 1 )
                {
                    ReportProgress( 0, string.Format( "{0:N0} attribute values processed.", completedItems ) );
                }

                if ( completedItems % ReportingNumber < 1 )
                {
                    SaveAttributeValues( lookupContext, attributeValues, importedAttributeValues );
                    attributeValues.Clear();
                    lookupContext.Dispose();
                    lookupContext = new RockContext();
                    attributeService = new AttributeService( lookupContext );

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

        #endregion Entity Attribute Value Methods

    }
}
