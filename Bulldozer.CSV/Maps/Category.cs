// <copyright>
// Copyright 2024 by Kingdom First Solutions
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
        /// Maps the category import data.
        /// </summary>
        /// <exception cref="System.NotImplementedException"></exception>
        private int ImportCategories()
        {
            ReportProgress( 0, "Preparing Category data for import..." );

            var rockContext = new RockContext();
            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();

            var categoryLookup = new CategoryService( rockContext )
                                        .Queryable()
                                        .AsNoTracking()
                                        .ToList();

            var importedCategoryDict = categoryLookup
                                         .Where( c => c.ForeignKey != null && c.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                         .GroupBy( m => m.ForeignKey )
                                         .ToDictionary( k => k.Key, v => v.FirstOrDefault() );

            var existingCategoryForeignKeys = importedCategoryDict.Keys.ToList();

            var categoriesToProcess = this.CategoryCsvList.Where( c => !importedCategoryDict.ContainsKey( string.Format( "{0}^{1}", ImportInstanceFKPrefix, c.Id ) ) ).ToList();

            ReportProgress( 0, $"{this.CategoryCsvList.Count - categoriesToProcess.Count} Categories already exist. Begin processing {categoriesToProcess.Count} Category Records..." );

            // Process categories by entitytype

            var categoriesByEntityType = categoriesToProcess
                                            .GroupBy( c => c.EntityTypeName )
                                            .Select( g => new { EntityTypeName = g.Key, CategoryCsvList = g.ToList() } );
            var invalidEntityTypeNames = new List<string>();
            var invalidParentCategoryCsvs = new List<CategoryCsv>();

            var importedDateTime = RockDateTime.Now;
            foreach ( var entityTypeCategory in categoriesByEntityType )
            {
                var categoriesToInsert = new List<Category>();
                var entityType = entityTypes.FirstOrDefault( et => et.Name.Equals( entityTypeCategory.EntityTypeName ) );
                var entityTypeId = entityType?.Id;
                if ( !entityTypeId.HasValue )
                {
                    invalidEntityTypeNames.Add( entityTypeCategory.EntityTypeName );
                    continue;
                }

                var entityCategoryLookup = categoryLookup.Where( c => c.EntityTypeId == entityTypeId.Value ).ToList();
                foreach ( var categoryCsv in entityTypeCategory.CategoryCsvList )
                {
                    Category category = null;
                    var foreignKey = $"{this.ImportInstanceFKPrefix}^{categoryCsv.Id}";
                    var matchingCategories = GetCategoriesByFKOrName( rockContext, foreignKey, categoryCsv.Name, entityCategoryLookup );

                    // If only one match is returned, select it
                    // OR if multiple are returned but one matches an existing foreign key, select the first one to avoid duplicate foreign keys
                    if ( matchingCategories.Count == 1 || matchingCategories.Any( mc => existingCategoryForeignKeys.Any( k => mc.ForeignKey == k ) ) )
                    {
                        category = matchingCategories.First();
                    }
                    if ( matchingCategories.Count == 0 )
                    {
                        var newCategory = new Category();
                        InitializeCategoryFromCategoryCsv( newCategory, categoryCsv, importedDateTime );
                        newCategory.EntityTypeId = entityTypeId.Value;

                        categoriesToInsert.Add( newCategory );
                        entityCategoryLookup.Add( newCategory );
                        importedCategoryDict.Add( newCategory.ForeignKey, newCategory );
                    }
                }

                rockContext.BulkInsert( categoriesToInsert );

                var entityCategories = new CategoryService( rockContext )
                                        .Queryable()
                                        .Where( c => c.ForeignKey != null && c.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) && c.EntityTypeId == entityTypeId )
                                        .ToList();

                // Now deal with connecting Parent categories

                ReportProgress( 0, "Processing parent category connections..." );

                var categoryUpdated = false;
                var entityCategoriesWithParents = entityTypeCategory.CategoryCsvList.Where( c => c.ParentCategoryId.IsNotNullOrWhiteSpace() ).ToList();
                importedDateTime = RockDateTime.Now;
                foreach ( var categoryCsv in entityCategoriesWithParents )
                {
                    var foreignKey = $"{this.ImportInstanceFKPrefix}^{categoryCsv.Id}";
                    var parentForeignKey = $"{this.ImportInstanceFKPrefix}^{categoryCsv.ParentCategoryId}";
                    var category = entityCategories.FirstOrDefault( c => c.ForeignKey == foreignKey );
                    if ( category == null || category.EntityTypeId != entityTypeId )
                    {
                        continue;
                    }
                    var parentCategory = GetCategoriesByFKOrName( rockContext, parentForeignKey, categoryCsv.ParentCategoryId, entityCategories ).FirstOrDefault();
                    var parentCategoryId = parentCategory?.Id;

                    if ( parentCategoryId.HasValue && parentCategoryId.Value > 0 && category.ParentCategoryId != parentCategoryId )
                    {
                        category.ParentCategoryId = parentCategoryId;
                        categoryUpdated = true;
                    }
                    else if ( category.ParentCategoryId == parentCategoryId )
                    {
                        // The category's ParentCategoryId is already set correctly, so ignore this.
                    }
                    else
                    {
                        invalidParentCategoryCsvs.Add( categoryCsv );
                    }

                }

                if ( categoryUpdated )
                {
                    rockContext.SaveChanges( true );
                }

                if ( invalidParentCategoryCsvs.Count > 0 )
                {
                    LogException( $"CategoryImport", $"The following {entityType.FriendlyName} type Categories had invalid ParentCategoryId values. Their ParentCategoryId value was not set:\r\n{string.Join( ", ", invalidParentCategoryCsvs )}" );
                }
            }

            if ( invalidEntityTypeNames.Count > 0 )
            {
                LogException( $"CategoryImport", $"The following EntityTypeNames are invalid. Any categories using them were skipped:\r\n{string.Join( ", ", invalidEntityTypeNames )}" );
            }

            ReportProgress( 100, $"Finished Category import: {categoriesToProcess.Count} FinancialAccounts processed." );
            return categoriesToProcess.Count;
        }
    }
}