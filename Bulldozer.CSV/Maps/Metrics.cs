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
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.Extensions;
using static Bulldozer.Utility.CachedTypes;
using System.Reflection;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the family import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Loads the family data.
        /// </summary>
        /// <param name="csvData">The CSV data.</param>
        private int LoadMetrics( CSVInstance csvData )
        {
            if ( this.CampusImportDict == null && this.CampusesDict == null )
            {
                LoadCampusDict();
            }
            if ( this.GroupDict == null )
            {
                LoadGroupDict();
            }

            // Required variables
            var lookupContext = new RockContext();
            var metricService = new MetricService( lookupContext );
            var metricCategoryService = new MetricCategoryService( lookupContext );
            var categoryService = new CategoryService( lookupContext );
            var metricSourceTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.METRIC_SOURCE_TYPE ) ).DefinedValues;
            var metricManualSource = metricSourceTypes.FirstOrDefault( m => m.Guid == new Guid( Rock.SystemGuid.DefinedValue.METRIC_SOURCE_VALUE_TYPE_MANUAL ) );

            var scheduleService = new ScheduleService( lookupContext );
            var scheduleMetrics = scheduleService.Queryable().AsNoTracking()
                .Where( s => s.Category.Guid == new Guid( Rock.SystemGuid.Category.SCHEDULE_SERVICE_TIMES ) ).ToList();
            var scheduleCategoryId = categoryService.Queryable().AsNoTracking()
                .Where( c => c.Guid == new Guid( Rock.SystemGuid.Category.SCHEDULE_SERVICE_TIMES ) ).FirstOrDefault().Id;

            var metricEntityTypeId = EntityTypeCache.Get<MetricCategory>( false, lookupContext ).Id;

            var metricLookup = metricService.Queryable().AsNoTracking().Where( m => m.ForeignKey != null && m.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) ).ToDictionary( k => k.ForeignKey, v => v );
            var metricCategories = categoryService.Queryable().AsNoTracking()
                .Where( c => c.EntityType.Guid == new Guid( Rock.SystemGuid.EntityType.METRICCATEGORY ) ).ToList();

            var defaultMetricCategory = metricCategories.FirstOrDefault( c => c.Name == "Metrics" );

            if ( defaultMetricCategory == null )
            {
                defaultMetricCategory = new Category();
                defaultMetricCategory.Name = "Metrics";
                defaultMetricCategory.IsSystem = false;
                defaultMetricCategory.EntityTypeId = metricEntityTypeId;
                defaultMetricCategory.EntityTypeQualifierColumn = string.Empty;
                defaultMetricCategory.EntityTypeQualifierValue = string.Empty;
                defaultMetricCategory.IconCssClass = string.Empty;
                defaultMetricCategory.Description = string.Empty;

                lookupContext.Categories.Add( defaultMetricCategory );
                lookupContext.SaveChanges();

                metricCategories.Add( defaultMetricCategory );
            }

            var metricValues = new List<MetricValue>();

            Metric currentMetric = null;
            int completed = 0;

            ReportProgress( 0, string.Format( "Starting metrics import ({0:N0} already exist).", 0 ) );

            string[] row;
            var invalidGroupIds = new List<string>();

            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                string metricId = row[MetricId];
                string metricName = row[MetricName];
                string metricCategoryString = row[MetricCategory];
                string partitionCampus = row[PartitionCampus];
                DateTime? partitionServiceDate = row[PartitionService].AsDateTime();
                string partitionGroup = row[PartitionGroup];
                decimal? value = row[MetricValue].AsDecimalOrNull();
                string metricValueId = row[MetricValueId];
                DateTime? metricValueDate = row[MetricValueDate].AsDateTime();
                string metricValueNote = row[MetricValueNote];

                if ( !string.IsNullOrEmpty( metricName ) )
                {
                    var metricCategoryId = defaultMetricCategory.Id;

                    // create the category if it doesn't exist
                    Category newMetricCategory = null;
                    if ( !string.IsNullOrEmpty( metricCategoryString ) )
                    {
                        newMetricCategory = metricCategories.FirstOrDefault( c => c.Name == metricCategoryString );
                        if ( newMetricCategory == null )
                        {
                            newMetricCategory = new Category();
                            newMetricCategory.Name = metricCategoryString;
                            newMetricCategory.IsSystem = false;
                            newMetricCategory.EntityTypeId = metricEntityTypeId;
                            newMetricCategory.EntityTypeQualifierColumn = string.Empty;
                            newMetricCategory.EntityTypeQualifierValue = string.Empty;
                            newMetricCategory.IconCssClass = string.Empty;
                            newMetricCategory.Description = string.Empty;

                            lookupContext.Categories.Add( newMetricCategory );
                            lookupContext.SaveChanges();

                            metricCategories.Add( newMetricCategory );
                        }

                        metricCategoryId = newMetricCategory.Id;
                    }

                    // create metric if it doesn't exist
                    currentMetric = metricLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricId}" );
                    if ( currentMetric == null )
                    {
                        currentMetric = new Metric();
                        currentMetric.Title = metricName;
                        currentMetric.IsSystem = false;
                        currentMetric.IsCumulative = false;
                        currentMetric.SourceSql = string.Empty;
                        currentMetric.Subtitle = string.Empty;
                        currentMetric.Description = string.Empty;
                        currentMetric.IconCssClass = string.Empty;
                        currentMetric.SourceValueTypeId = metricManualSource.Id;
                        currentMetric.CreatedByPersonAliasId = ImportPersonAliasId;
                        currentMetric.CreatedDateTime = ImportDateTime;
                        currentMetric.ForeignKey = $"{this.ImportInstanceFKPrefix}^{metricId}";

                        currentMetric.MetricPartitions = new List<MetricPartition>(); 
                        if ( partitionCampus.IsNotNullOrWhiteSpace() )
                        {
                            currentMetric.MetricPartitions.Add( new MetricPartition { Label = "Campus", EntityTypeId = CampusEntityTypeId, Metric = currentMetric, Order = 0 } );
                        }
                        if ( partitionServiceDate.HasValue )
                        {
                            currentMetric.MetricPartitions.Add( new MetricPartition { Label = "Service", EntityTypeId = ScheduleEntityTypeId, Metric = currentMetric, Order = currentMetric.MetricPartitions.Count } );
                        }
                        if ( partitionGroup.IsNotNullOrWhiteSpace() )
                        {
                            currentMetric.MetricPartitions.Add( new MetricPartition { Label = "Group", EntityTypeId = GroupEntityTypeId, Metric = currentMetric, Order = currentMetric.MetricPartitions.Count } );
                        }

                        metricService.Add( currentMetric );
                        lookupContext.SaveChanges();

                        if ( currentMetric.MetricCategories == null || !currentMetric.MetricCategories.Any( a => a.CategoryId == metricCategoryId ) )
                        {
                            metricCategoryService.Add( new MetricCategory { CategoryId = metricCategoryId, MetricId = currentMetric.Id } );
                            lookupContext.SaveChanges();
                        }

                        metricLookup.Add( currentMetric.ForeignKey, currentMetric );
                    }

                    // create values for this metric
                    var metricValue = new MetricValue();
                    metricValue.MetricValueType = MetricValueType.Measure;
                    metricValue.CreatedByPersonAliasId = ImportPersonAliasId;
                    metricValue.CreatedDateTime = ImportDateTime;
                    metricValue.MetricValueDateTime = metricValueDate.Value.Date;
                    metricValue.MetricId = currentMetric.Id;
                    metricValue.Note = string.Empty;
                    metricValue.XValue = string.Empty;
                    metricValue.YValue = value;
                    metricValue.ForeignKey = $"{this.ImportInstanceFKPrefix}^{metricValueId}";
                    metricValue.Note = metricValueNote;

                    if ( partitionCampus.IsNotNullOrWhiteSpace() )
                    {
                        var campusIdInt = partitionCampus.AsIntegerOrNull();
                        Campus campus = null;
                        if ( UseExistingCampusIds )
                        {
                            if ( campusIdInt.HasValue )
                            {
                                campus = this.CampusesDict.GetValueOrNull( campusIdInt.Value );
                            }
                            else
                            {
                                campus = this.CampusesDict.Values.FirstOrDefault( c => c.Name.Equals( partitionCampus, StringComparison.OrdinalIgnoreCase )
                                        || c.ShortCode.Equals( partitionCampus, StringComparison.OrdinalIgnoreCase ) || c.Id.Equals( partitionCampus ) );
                            }
                        }
                        else
                        {
                            campus = this.CampusImportDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{partitionCampus}" );
                            if ( campus == null )
                            {
                                campus = this.CampusImportDict.Values.FirstOrDefault( c => c.Name.Equals( partitionCampus, StringComparison.OrdinalIgnoreCase )
                                        || c.ShortCode.Equals( partitionCampus, StringComparison.OrdinalIgnoreCase ) || c.Id.Equals( partitionCampus ) );
                            }
                        }

                        if ( campus == null && !UseExistingCampusIds )
                        {
                            var newCampus = new Campus
                            {
                                IsSystem = false,
                                Name = partitionCampus,
                                ShortCode = partitionCampus.RemoveWhitespace(),
                                IsActive = true,
                                ForeignKey = $"{this.ImportInstanceFKPrefix}^{partitionCampus}"
                            };
                            lookupContext.Campuses.Add( newCampus );
                            lookupContext.SaveChanges( DisableAuditing );

                            this.CampusImportDict.Add( newCampus.ForeignKey, newCampus );
                            campus = newCampus;
                        }

                        if ( campus != null )
                        {
                            var metricPartitionCampusId = currentMetric.MetricPartitions.FirstOrDefault( p => p.Label == "Campus" ).Id;
                            metricValue.MetricValuePartitions.Add( new MetricValuePartition { MetricPartitionId = metricPartitionCampusId, EntityId = campus.Id } );
                        }
                    }

                    if ( partitionGroup.IsNotNullOrWhiteSpace() )
                    {
                        var group = this.GroupDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{partitionGroup}" );

                        if ( group == null )
                        {
                            invalidGroupIds.Add( partitionGroup );
                            continue;
                        }
                        var metricPartitionGroupId = currentMetric.MetricPartitions.FirstOrDefault( p => p.Label == "Group" ).Id;
                        metricValue.MetricValuePartitions.Add( new MetricValuePartition { MetricPartitionId = metricPartitionGroupId, EntityId = group.Id } );
                    }

                    if ( partitionServiceDate.HasValue )
                    {
                        var metricPartitionScheduleId = currentMetric.MetricPartitions.FirstOrDefault( p => p.Label == "Service" ).Id;

                        var date = ( DateTime ) partitionServiceDate;
                        var scheduleName = date.DayOfWeek.ToString();

                        if ( date.TimeOfDay.TotalSeconds > 0 )
                        {
                            scheduleName = scheduleName + string.Format( " {0}", date.ToString( "hh:mm" ) ) + string.Format( "{0}", date.ToString( "tt" ).ToLower() );
                        }

                        if ( !scheduleMetrics.Any( s => s.Name == scheduleName ) )
                        {
                            Schedule newSchedule = new Schedule();
                            newSchedule.Name = scheduleName;
                            newSchedule.CategoryId = scheduleCategoryId;
                            newSchedule.CreatedByPersonAliasId = ImportPersonAliasId;
                            newSchedule.CreatedDateTime = ImportDateTime;
                            newSchedule.ForeignKey = string.Format( "Metric Schedule imported {0}", ImportDateTime );

                            scheduleMetrics.Add( newSchedule );
                            lookupContext.Schedules.Add( newSchedule );
                            lookupContext.SaveChanges();
                        }

                        var scheduleId = scheduleMetrics.FirstOrDefault( s => s.Name == scheduleName ).Id;

                        metricValue.MetricValuePartitions.Add( new MetricValuePartition { MetricPartitionId = metricPartitionScheduleId, EntityId = scheduleId } );
                    }

                    metricValues.Add( metricValue );

                    completed++;
                    if ( completed % ( DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, string.Format( "{0:N0} metrics imported.", completed ) );
                    }

                    if ( completed % DefaultChunkSize < 1 )
                    {
                        SaveMetrics( metricValues );
                        ReportPartialProgress();

                        metricValues.Clear();
                    }
                }
            }

            // Check to see if any rows didn't get saved to the database
            if ( metricValues.Any() )
            {
                SaveMetrics( metricValues );
            }
            if ( invalidGroupIds.Any() )
            {
                LogException( null, $"Some Metric Values did not have a Group partition set for them due to an invalid GroupId. The invalid PartitionGroup values were:\r\n{string.Join( ",", invalidGroupIds )}" );
            }

            ReportProgress( 0, string.Format( "Finished metrics import: {0:N0} metrics added or updated.", completed ) );
            return completed;
        }

        /// <summary>
        /// Saves all the metric values.
        /// </summary>
        private void SaveMetrics( List<MetricValue> metricValues )
        {
            var rockContext = new RockContext();
            rockContext.WrapTransaction( () =>
            {
                rockContext.MetricValues.AddRange( metricValues );
                rockContext.SaveChanges( DisableAuditing );
            } );
        }

        private int ImportMetrics()
        {
            this.ReportProgress( 0, "Preparing Metric data for import..." );

            // Required variables
            
            var errors = string.Empty;
            var rockContext = new RockContext();
            var metricService = new MetricService( rockContext );
            var categoryService = new CategoryService( rockContext );
            var metricSourceTypes = DefinedTypeCache.Get( new Guid( Rock.SystemGuid.DefinedType.METRIC_SOURCE_TYPE ) ).DefinedValues;
            var metricManualSource = metricSourceTypes.FirstOrDefault( m => m.Guid == new Guid( Rock.SystemGuid.DefinedValue.METRIC_SOURCE_VALUE_TYPE_MANUAL ) );
            var entityTypes = EntityTypeCache.All().Where( e => e.IsEntity && e.IsSecured ).ToList();

            var metricCategoryEntityTypeId = EntityTypeCache.Get<MetricCategory>( false, rockContext ).Id;
            var metricCategories = categoryService.Queryable().AsNoTracking()
                .Where( c => c.EntityTypeId == metricCategoryEntityTypeId ).ToList();
            var existingCategoryForeignKeys = metricCategories.Select( c => c.ForeignKey ).ToList();
            var defaultCategoryForeignKey = $"{this.ImportInstanceFKPrefix}^{metricCategoryEntityTypeId}_Metrics";
            Category defaultMetricCategory = null;
            var defaultMatchingCategories = GetCategoriesByFKOrName( rockContext, defaultCategoryForeignKey, "Metrics", metricCategories );

            // If only one match is returned, select it
            // OR if multiple are returned but one matches an existing foreign key, select the first one to avoid duplicate foreign keys
            if ( defaultMatchingCategories.Count == 1 || defaultMatchingCategories.Any( mc => existingCategoryForeignKeys.Any( k => mc.ForeignKey == k ) ) )
            {
                defaultMetricCategory = defaultMatchingCategories.First();
            }

            if ( defaultMetricCategory == null )
            {
                defaultMetricCategory = new Category
                {
                    Name = "Metrics",
                    IsSystem = false,
                    EntityTypeId = metricCategoryEntityTypeId,
                    EntityTypeQualifierColumn = string.Empty,
                    EntityTypeQualifierValue = string.Empty,
                    IconCssClass = string.Empty,
                    Description = string.Empty,
                    ForeignKey = defaultCategoryForeignKey
                };

                rockContext.Categories.Add( defaultMetricCategory );
                rockContext.SaveChanges();

                metricCategories.Add( defaultMetricCategory );
                existingCategoryForeignKeys.Add( defaultCategoryForeignKey );
            }

            // Process metrics

            var newMetrics = new List<Metric>();
            var invalidPartitionMetricCsvs = new List<MetricCsv>();
            var metricLookup = metricService
                                .Queryable()
                                .AsNoTracking().Where( m => m.ForeignKey != null && m.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                .GroupBy( m => m.ForeignKey )
                                .ToDictionary( k => k.Key, v => v.FirstOrDefault() );

            var metricCsvsToProcess = this.MetricCsvList.Where( m => !metricLookup.ContainsKey( $"{this.ImportInstanceFKPrefix}^{m.Id}" ) );
            var csvCategoryIds = metricCsvsToProcess.Select( m => m.CategoryId ).Distinct().ToList();
            var existingMetricCount = this.MetricCsvList.Count() - metricCsvsToProcess.Count();
            if ( existingMetricCount > 0 )
            {
                this.ReportProgress( 0, $"{existingMetricCount} out of {this.MetricValueCsvList.Count()} Metric(s) from import already exist and will be skipped." );
            }

            foreach ( var metricCsv in metricCsvsToProcess )
            {
                var foreignKey = $"{this.ImportInstanceFKPrefix}^{metricCsv.Id}";
                if ( metricLookup.ContainsKey( foreignKey ) )
                {
                    continue;
                }

                var newMetric = new Metric
                {
                    Title = metricCsv.Name,
                    IsSystem = false,
                    IsCumulative = false,
                    SourceSql = string.Empty,
                    SourceValueTypeId = metricManualSource.Id,
                    CreatedByPersonAliasId = ImportPersonAliasId,
                    CreatedDateTime = ImportDateTime,
                    ForeignKey = foreignKey,
                    ForeignId = metricCsv.Id.AsIntegerOrNull()
                };

                if ( metricCsv.Subtitle.IsNotNullOrWhiteSpace() )
                {
                    newMetric.Subtitle = metricCsv.Subtitle;
                }

                if ( metricCsv.Description.IsNotNullOrWhiteSpace() )
                {
                    newMetric.Description = metricCsv.Description;
                }

                if ( metricCsv.IconCssClass.IsNotNullOrWhiteSpace() )
                {
                    newMetric.IconCssClass = metricCsv.IconCssClass;
                }

                newMetrics.Add( newMetric );
                metricLookup.Add( foreignKey, newMetric );
            }

            // Slice data into chunks and process
            var workingMetricImportList = newMetrics.ToList();
            var metricsRemainingToProcess = workingMetricImportList.Count;
            var completedMetrics = 0;

            while ( metricsRemainingToProcess > 0 )
            {
                if ( completedMetrics > 0 && completedMetrics % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedMetrics} Metrics processed." );
                }

                if ( completedMetrics % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingMetricImportList.Take( Math.Min( this.DefaultChunkSize, workingMetricImportList.Count ) ).ToList();
                    rockContext.BulkInsert( csvChunk );
                    completedMetrics += csvChunk.Count;
                    metricsRemainingToProcess -= csvChunk.Count;
                    workingMetricImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            // Now create MetricCategories to connect metrics with their category.

            ReportProgress( 0, "Processing metric categories..." );

            var newMetricCategories = new List<MetricCategory>();
            var missingCategories = new List<string>();
            var missingMetricIds = new List<string>();
            metricLookup = metricService
                            .Queryable()
                            .Where( m => m.ForeignKey != null && m.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                            .GroupBy( m => m.ForeignKey )
                            .ToDictionary( k => k.Key, v => v.FirstOrDefault() );

            foreach ( var categoryId in csvCategoryIds )
            {
                var categoryForeignKey = $"{this.ImportInstanceFKPrefix}^{categoryId}";
                var metricCategory = GetCategoriesByFKOrName( rockContext, categoryForeignKey, categoryId, metricCategories ).FirstOrDefault();
                if ( metricCategory == null )
                {
                    missingCategories.Add( categoryId );
                    continue;
                }
                var categoryMetricCsvs = this.MetricCsvList.Where( m => m.CategoryId == categoryId ).ToList();
                foreach ( var metricCsv in categoryMetricCsvs )
                {
                    var foreignKey = $"{this.ImportInstanceFKPrefix}^{metricCsv.Id}";
                    var metric = metricLookup.GetValueOrNull( foreignKey );
                    if ( metric == null )
                    {
                        missingMetricIds.Add( metricCsv.Id );
                        continue;
                    }
                    var newMetricCategory = new MetricCategory
                    {
                        CategoryId = metricCategory.Id,
                        MetricId = metric.Id
                    };
                    newMetricCategories.Add( newMetricCategory );
                }
            }
            if ( newMetricCategories.Count > 0 )
            {
                rockContext.BulkInsert( newMetricCategories );
            }

            if ( missingCategories.Count > 0 )
            {
                LogException( $"MetricImport", $"The following categories were not found, resulting in any related metrics not being added to them:\r\n{string.Join( ", ", missingCategories )}" );
            }

            if ( missingMetricIds.Count > 0 )
            {
                LogException( $"MetricImport", $"The following metric ids were not found while trying to add imported metrics to categories:\r\n{string.Join( ", ", missingMetricIds )}" );
            }

            ReportProgress( 0, "Begin processing metric partitions records" );

            var metricsWithPartitions = this.MetricCsvList.Where( m => m.Partition1Id.IsNotNullOrWhiteSpace() || m.Partition2Id.IsNotNullOrWhiteSpace() || m.Partition3Id.IsNotNullOrWhiteSpace() ).ToList();

            var metricPartitionLookup = new MetricPartitionService( rockContext )
                                                .Queryable()
                                                .AsNoTracking()
                                                .Where( p => p.ForeignKey != null && p.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                .GroupBy( p => p.ForeignKey )
                                                .ToDictionary( k => k.Key, v => v.FirstOrDefault() );


            // Slice data into chunks and process
            var workingMetricPartitionImportList = metricsWithPartitions.ToList();
            var metricPartitionsRemainingToProcess = workingMetricPartitionImportList.Count();
            var completedMetricPartitions = 0;

            while ( metricPartitionsRemainingToProcess > 0 )
            {
                if ( completedMetricPartitions > 0 && completedMetricPartitions % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedMetricPartitions} Metric partitions processed." );
                }

                if ( completedMetricPartitions % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingMetricPartitionImportList.Take( Math.Min( this.DefaultChunkSize, workingMetricPartitionImportList.Count ) ).ToList();
                    completedMetricPartitions += BulkMetricPartitionImport( rockContext, csvChunk, metricLookup, metricPartitionLookup, invalidPartitionMetricCsvs, entityTypes );
                    metricPartitionsRemainingToProcess -= csvChunk.Count;
                    workingMetricPartitionImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            if ( invalidPartitionMetricCsvs.Count > 0 )
            {
                LogException( $"MetricImport", $"The following metrics had an invalid PartitionXEntityTypeName provided, resulting in one or more partitions not being created:\r\n{string.Join( ", ", invalidPartitionMetricCsvs.Select( m => m.Id ).Distinct().OrderBy( m => m ) ) }" );
            }
            return completedMetrics;
        }

        /// <summary>
        /// Bulk import of Metric Partitions.
        /// </summary>
        /// <param name="rockContext">The RockContext.</param>
        /// <param name="metricCsvs">The list of MetricCsv records to process.</param>
        /// <param name="metricLookup">The Dictionary of existing Metrics.</param>
        /// <param name="metricPartitionLookup">The Dictionary of existing Metric Partitions.</param>
        /// <param name="invalidPartitionMetricCsvs">The list of MetricCsv records with invalid partition information.</param>
        /// <param name="entityTypes">The list of existing EntityTypeCache records.</param>
        /// <returns></returns>
        public int BulkMetricPartitionImport( RockContext rockContext, List<MetricCsv> metricCsvs, Dictionary<string,Metric> metricLookup, Dictionary<string, MetricPartition> metricPartitionLookup, List<MetricCsv> invalidPartitionMetricCsvs, List<EntityTypeCache> entityTypes )
        {
            var importedDateTime = RockDateTime.Now;
            var metricPartitionsToInsert = new List<MetricPartition>();

            foreach ( var metricCsv in metricCsvs )
            {
                var metric = metricLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricCsv.Id}" );
                var entityTypeName = string.Empty;
                var partition1ForeignKey = $"{this.ImportInstanceFKPrefix}^{metricCsv.Id}_1";
                var partition2ForeignKey = $"{this.ImportInstanceFKPrefix}^{metricCsv.Id}_2";
                var partition3ForeignKey = $"{this.ImportInstanceFKPrefix}^{metricCsv.Id}_3";

                if ( metricCsv.Partition1Id.IsNotNullOrWhiteSpace() && !metricPartitionLookup.ContainsKey( partition1ForeignKey ) )
                {
                    var newMetricPartition = CreateMetricPartition( metricCsv.Partition1EntityTypeName, metric.Id, metricCsv.Partition1Label, partition1ForeignKey, entityTypes, metricCsv.Partition1IsRequired.GetValueOrDefault(), 0 );
                    if ( newMetricPartition == null )
                    {
                        invalidPartitionMetricCsvs.Add( metricCsv );
                    }
                    else
                    {
                        metricPartitionsToInsert.Add( newMetricPartition );
                        metricPartitionLookup.Add( newMetricPartition.ForeignKey, newMetricPartition );
                    }
                }

                if ( metricCsv.Partition2Id.IsNotNullOrWhiteSpace() && !metricPartitionLookup.ContainsKey( partition2ForeignKey ) )
                {
                    var newMetricPartition = CreateMetricPartition( metricCsv.Partition2EntityTypeName, metric.Id, metricCsv.Partition2Label, partition2ForeignKey, entityTypes, metricCsv.Partition2IsRequired.GetValueOrDefault(), 1 );
                    if ( newMetricPartition == null )
                    {
                        invalidPartitionMetricCsvs.Add( metricCsv );
                    }
                    else
                    {
                        metricPartitionsToInsert.Add( newMetricPartition );
                        metricPartitionLookup.Add( newMetricPartition.ForeignKey, newMetricPartition );
                    }
                }

                if ( metricCsv.Partition3Id.IsNotNullOrWhiteSpace() && !metricPartitionLookup.ContainsKey( partition3ForeignKey ) )
                {
                    var newMetricPartition = CreateMetricPartition( metricCsv.Partition3EntityTypeName, metric.Id, metricCsv.Partition3Label, partition3ForeignKey, entityTypes, metricCsv.Partition3IsRequired.GetValueOrDefault(), 2 );
                    if ( newMetricPartition == null )
                    {
                        invalidPartitionMetricCsvs.Add( metricCsv );
                    }
                    else
                    {
                        metricPartitionsToInsert.Add( newMetricPartition );
                        metricPartitionLookup.Add( newMetricPartition.ForeignKey, newMetricPartition );
                    }
                }
            }

            rockContext.BulkInsert( metricPartitionsToInsert );

            return metricCsvs.Count;
        }

        private int ImportMetricValues()
        {
            this.ReportProgress( 0, "Preparing Metric Value data for import..." );

            // Required variables
            var errors = string.Empty;
            var rockContext = new RockContext();
            var metricService = new MetricService( rockContext );
            var metricValueService = new MetricValueService( rockContext );
            
            var metricLookup = metricService
                                .Queryable()
                                .AsNoTracking()
                                .Where( m => m.ForeignKey != null && m.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                .GroupBy( m => m.ForeignKey )
                                .ToDictionary( k => k.Key, v => v.FirstOrDefault() );
            var metricValueLookup = metricValueService
                                    .Queryable()
                                    .AsNoTracking()
                                    .Where( m => m.ForeignKey != null && m.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                    .GroupBy( m => m.ForeignKey )
                                    .ToDictionary( k => k.Key, v => v.FirstOrDefault() );

            var newMetricValueCsvs = this.MetricValueCsvList.Where( mv => !metricValueLookup.ContainsKey( $"{this.ImportInstanceFKPrefix}^{mv.Id}" ) );
            var existingMetricValueCount = this.MetricValueCsvList.Count() - newMetricValueCsvs.Count();
            if ( existingMetricValueCount > 0 )
            {
                this.ReportProgress( 0, $"{existingMetricValueCount} out of {this.MetricValueCsvList.Count()} Metric Value(s) from import already exist and will be skipped." );
            }

            var valuesWithInvalidMetricIds = newMetricValueCsvs.Where( mv => !metricLookup.ContainsKey( $"{this.ImportInstanceFKPrefix}^{mv.MetricId}" ) );
            var metricValueCsvsToProcess = newMetricValueCsvs.Where( mv => metricLookup.ContainsKey( $"{this.ImportInstanceFKPrefix}^{mv.MetricId}" ) ).ToList();
            if ( valuesWithInvalidMetricIds.Any() )
            {
                this.ReportProgress( 0, $"{valuesWithInvalidMetricIds.Count()} Metric Value(s) from import have invalid MetricId values and will be skipped.." );
            }

            var metrics = new List<Metric>();
            var metricValuesToInsert = new List<MetricValue>();
            foreach ( var metricValueCsv in metricValueCsvsToProcess )
            {
                var metric = metricLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricValueCsv.MetricId}" );
                var foreignKey = $"{this.ImportInstanceFKPrefix}^{metricValueCsv.Id}";
                var newMetricValue = new MetricValue
                {
                    MetricValueType = MetricValueType.Measure,
                    CreatedByPersonAliasId = ImportPersonAliasId,
                    CreatedDateTime = ImportDateTime,
                    MetricValueDateTime = metricValueCsv.ValueDateTime,
                    MetricId = metric.Id,
                    YValue = metricValueCsv.YValue,
                    ForeignKey = foreignKey
                };

                if ( metricValueCsv.XValue.IsNotNullOrWhiteSpace() )
                {
                    newMetricValue.XValue = metricValueCsv.XValue;
                }
                if ( metricValueCsv.Note.IsNotNullOrWhiteSpace() )
                {
                    newMetricValue.Note = metricValueCsv.Note;
                }

                metricValuesToInsert.Add( newMetricValue );
            }

            // Slice Metric Value data into chunks and process
            var workingMetricValueImportList = metricValuesToInsert;
            var metricValuesRemainingToProcess = workingMetricValueImportList.Count();
            var completedMetricValues = 0;

            while ( metricValuesRemainingToProcess > 0 )
            {
                if ( completedMetricValues > 0 && completedMetricValues % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completedMetricValues} Metric Values processed." );
                }

                if ( completedMetricValues % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingMetricValueImportList.Take( Math.Min( this.DefaultChunkSize, workingMetricValueImportList.Count ) ).ToList();
                    rockContext.BulkInsert( csvChunk );
                    completedMetricValues += csvChunk.Count;
                    metricValuesRemainingToProcess -= csvChunk.Count;
                    workingMetricValueImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }

            // Now we deal with value partitions.

            // Refresh metric value lookup to include newly created metric values
            metricValueLookup = metricValueService
                                    .Queryable()
                                    .AsNoTracking()
                                    .Where( m => m.ForeignKey != null && m.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                    .GroupBy( m => m.ForeignKey )
                                    .ToDictionary( k => k.Key, v => v.FirstOrDefault() );

            ReportProgress( 0, $"Begin processing Metric Value Partitions." );
            var metricValueCsvsWithPartition = metricValueCsvsToProcess.Where( mv => mv.Partition1EntityId.IsNotNullOrWhiteSpace() || mv.Partition2EntityId.IsNotNullOrWhiteSpace() || mv.Partition3EntityId.IsNotNullOrWhiteSpace() );
            var metricPartitionLookup = new MetricPartitionService( rockContext )
                                                .Queryable()
                                                .AsNoTracking()
                                                .Where( p => p.ForeignKey != null && p.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                .GroupBy( m => m.ForeignKey )
                                                .ToDictionary( k => k.Key, v => v.FirstOrDefault() );
            var metricValuePartitionLookup = new MetricValuePartitionService( rockContext )
                                                .Queryable()
                                                .AsNoTracking()
                                                .Where( vp => vp.ForeignKey != null && vp.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                                .GroupBy( vp => vp.ForeignKey )
                                                .ToDictionary( k => k.Key, v => v.FirstOrDefault() );
            var metricValuePartitionImportsToProcess = new List<MetricValuePartitionImport>();
            
            foreach( var metricPartitonCsv in metricValueCsvsWithPartition )
            {
                var metric = metricLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.MetricId}" );
                var metricValue = metricValueLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.Id}" );
                if ( metric == null || metricValue == null )
                {
                    continue;
                }
                if ( metricPartitonCsv.Partition1EntityId.IsNotNullOrWhiteSpace() )
                {
                    var metricPartition = metricPartitionLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.MetricId}_1" );
                    var metricValuePartitionForeignKey = $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.MetricId}_1_{metricPartitonCsv.Id}";
                    var existingMetricValuePartion = metricValuePartitionLookup.GetValueOrNull( metricValuePartitionForeignKey );
                    if ( metricPartition != null && existingMetricValuePartion == null )
                    {
                        var newMetricValuePartitionImport = new MetricValuePartitionImport
                        {
                            MetricValue = metricValue,
                            MetricPartition = metricPartition,
                            EntityId = metricPartitonCsv.Partition1EntityId,
                            CsvMetricValueId = metricPartitonCsv.Id,
                            ForeignKey = metricValuePartitionForeignKey
                        };

                        metricValuePartitionImportsToProcess.Add( newMetricValuePartitionImport );
                    }
                }
                if ( metricPartitonCsv.Partition2EntityId.IsNotNullOrWhiteSpace() )
                {
                    var metricPartition = metricPartitionLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.MetricId}_2" );
                    var metricValuePartitionForeignKey = $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.MetricId}_2_{metricPartitonCsv.Id}";
                    var existingMetricValuePartion = metricValuePartitionLookup.GetValueOrNull( metricValuePartitionForeignKey );
                    if ( metricPartition != null && existingMetricValuePartion == null )
                    {
                        var newMetricValuePartitionImport = new MetricValuePartitionImport
                        {
                            MetricValue = metricValue,
                            MetricPartition = metricPartition,
                            EntityId = metricPartitonCsv.Partition2EntityId,
                            CsvMetricValueId = metricPartitonCsv.Id,
                            ForeignKey = metricValuePartitionForeignKey
                        };

                        metricValuePartitionImportsToProcess.Add( newMetricValuePartitionImport );
                    }
                }
                if ( metricPartitonCsv.Partition3EntityId.IsNotNullOrWhiteSpace() )
                {
                    var metricPartition = metricPartitionLookup.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.MetricId}_3" );
                    var metricValuePartitionForeignKey = $"{this.ImportInstanceFKPrefix}^{metricPartitonCsv.MetricId}_3_{metricPartitonCsv.Id}";
                    var existingMetricValuePartion = metricValuePartitionLookup.GetValueOrNull( metricValuePartitionForeignKey );
                    if ( metricPartition != null && existingMetricValuePartion == null )
                    {
                        var newMetricValuePartitionImport = new MetricValuePartitionImport
                        {
                            MetricValue = metricValue,
                            MetricPartition = metricPartition,
                            EntityId = metricPartitonCsv.Partition3EntityId,
                            CsvMetricValueId = metricPartitonCsv.Id,
                            ForeignKey = metricValuePartitionForeignKey
                        };

                        metricValuePartitionImportsToProcess.Add( newMetricValuePartitionImport );
                    }
                }
            }

            //  Now We will process them by entitytype so we only have to use reflection once per entity type.

            var campusEntityType = EntityTypeCache.Get( new Guid( Rock.SystemGuid.EntityType.CAMPUS ) );
            var metricPartitionEntityTypes = metricPartitionLookup.Select( mp => mp.Value.EntityType ).DistinctBy( e => e.Id ).ToList();
            foreach ( var entityType in metricPartitionEntityTypes )
            {
                var metricValuePartitionsToInsert = new List<MetricValuePartition>();
                var entityMetricValuePartitionImports = metricValuePartitionImportsToProcess.Where( mp => mp.MetricPartition.EntityTypeId == entityType.Id );
                
                if ( entityMetricValuePartitionImports.Count() == 0 )
                {
                    continue;
                }

                // Handle campus entity type uniquely when UseExistingCampusIds is set to true.

                if ( entityType.Id == campusEntityType.Id && UseExistingCampusIds )
                {
                    if ( this.CampusesDict == null )
                    {
                        LoadCampusDict();
                    }
                    foreach ( var metricValuePartitionImport in entityMetricValuePartitionImports )
                    {
                        var partitionCampusId = metricValuePartitionImport.EntityId.AsIntegerOrNull();
                        if ( !partitionCampusId.HasValue )
                        {
                            errors += $"{DateTime.Now}, MetricValuePartition, Invalid Campus Id {metricValuePartitionImport.EntityId} provided as EntityId for MetricPartitionValue {metricValuePartitionImport.CsvMetricValueId}. Metric value was skipped.\r\n";
                            continue;
                        }
                      var campus = this.CampusesDict.GetValueOrNull( partitionCampusId.Value );
                      if ( campus == null )
                      {
                          errors += $"{DateTime.Now}, MetricValuePartition, Campus {partitionCampusId.Value} not found. MetricPartitionValue for {metricValuePartitionImport.CsvMetricValueId} metric value was skipped.\r\n";
                          continue;
                      }
                      var newMetricValuePartition = new MetricValuePartition
                      {
                          MetricValueId = metricValuePartitionImport.MetricValue.Id,
                          MetricPartitionId = metricValuePartitionImport.MetricPartition.Id,
                          EntityId = campus.Id,
                          ForeignKey = metricValuePartitionImport.ForeignKey
                      };

                      metricValuePartitionsToInsert.Add( newMetricValuePartition );
                    }
                }
                else
                {
                    // Need to use reflection to get the correct EnityId value based on the entity type.

                    IService contextService = null;
                    var contextModelType = Type.GetType( entityType.AssemblyName );
                    var contextDbContext = Reflection.GetDbContextForEntityType( contextModelType );
                    if ( contextDbContext != null )
                    {
                        contextService = Reflection.GetServiceForEntityType( contextModelType, contextDbContext );
                    }

                    if ( contextService != null )
                    {
                        MethodInfo qryMethod = contextService.GetType().GetMethod( "Queryable", new Type[] { } );
                        var entityQry = qryMethod.Invoke( contextService, new object[] { } ) as IQueryable<IEntity>;
                        var entityIdDict = entityQry
                                            .Where( e => e.ForeignKey != null && e.ForeignKey.StartsWith( this.ImportInstanceFKPrefix + "^" ) )
                                            .GroupBy( e => e.ForeignKey )
                                            .ToDictionary( k => k.Key, v => v.FirstOrDefault() );

                        foreach ( var metricValuePartitionImport in entityMetricValuePartitionImports )
                        {
                            var entity = entityIdDict.GetValueOrNull( $"{this.ImportInstanceFKPrefix}^{metricValuePartitionImport.EntityId}" );
                            if ( entity == null )
                            {
                                errors += $"{DateTime.Now}, MetricValuePartition, EntityId {metricValuePartitionImport.EntityId} not found for EntityTypeName {entityType.Name}. MetricPartitionValue for {metricValuePartitionImport.CsvMetricValueId} metric value was skipped.\r\n";
                                continue;
                            }
                            var newMetricValuePartition = new MetricValuePartition
                            {
                                MetricValueId = metricValuePartitionImport.MetricValue.Id,
                                MetricPartitionId = metricValuePartitionImport.MetricPartition.Id,
                                EntityId = entity.Id,
                                ForeignKey = metricValuePartitionImport.ForeignKey
                            };

                            metricValuePartitionsToInsert.Add( newMetricValuePartition );
                        }

                    }
                }

                if ( metricValuePartitionsToInsert.Count > 0 )
                {
                    ReportProgress( 0, $"Begin processing { entityType.FriendlyName } type Metric Value Partitions." );
                }

                // Slice Metric Value Partition data into chunks and process
                var workingMetricValuePartitionImportList = metricValuePartitionsToInsert;
                var metricValuePartitionsRemainingToProcess = workingMetricValuePartitionImportList.Count();
                var completedMetricValuePartitions = 0;

                while ( metricValuePartitionsRemainingToProcess > 0 )
                {
                    if ( completedMetricValuePartitions > 0 && completedMetricValuePartitions % ( this.DefaultChunkSize * 10 ) < 1 )
                    {
                        ReportProgress( 0, $"{completedMetricValuePartitions} Metric Value Partitions processed." );
                    }

                    if ( completedMetricValuePartitions % this.DefaultChunkSize < 1 )
                    {
                        var csvChunk = workingMetricValuePartitionImportList.Take( Math.Min( this.DefaultChunkSize, workingMetricValuePartitionImportList.Count ) ).ToList();
                        rockContext.BulkInsert( csvChunk );
                        completedMetricValuePartitions += csvChunk.Count;
                        metricValuePartitionsRemainingToProcess -= csvChunk.Count;
                        workingMetricValuePartitionImportList.RemoveRange( 0, csvChunk.Count );
                        ReportPartialProgress();
                    }
                }
            }
            if ( errors.IsNotNullOrWhiteSpace() )
            {
                LogException( null, errors, hasMultipleErrors: true );
            }

            return completedMetricValues;
        }
    }
}