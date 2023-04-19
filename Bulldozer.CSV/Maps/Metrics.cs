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
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using static Bulldozer.Utility.Extensions;

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
            var campusEntityTypeId = EntityTypeCache.Get<Campus>( false, lookupContext ).Id;
            var scheduleEntityTypeId = EntityTypeCache.Get<Schedule>( false, lookupContext ).Id;

            var allMetrics = metricService.Queryable().AsNoTracking().ToList();
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
            // Uses a look-ahead enumerator: this call will move to the next record immediately
            while ( ( row = csvData.Database.FirstOrDefault() ) != null )
            {
                string metricCampus = row[MetricCampus];
                string metricName = row[MetricName];
                string metricCategoryString = row[MetricCategory];
                string metricNote = row[MetricNote];

                if ( !string.IsNullOrEmpty( metricName ) )
                {
                    decimal? value = row[MetricValue].AsDecimalOrNull();
                    DateTime? valueDate = row[MetricService].AsDateTime();
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
                    currentMetric = allMetrics.FirstOrDefault( m => m.Title == metricName && m.MetricCategories.Any( c => c.CategoryId == metricCategoryId ) );
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
                        currentMetric.ForeignKey = string.Format( "Metric imported {0}", ImportDateTime );

                        currentMetric.MetricPartitions = new List<MetricPartition>();
                        currentMetric.MetricPartitions.Add( new MetricPartition { Label = "Campus", EntityTypeId = campusEntityTypeId, Metric = currentMetric, Order = 0 } );
                        currentMetric.MetricPartitions.Add( new MetricPartition { Label = "Service", EntityTypeId = scheduleEntityTypeId, Metric = currentMetric, Order = 1 } );

                        metricService.Add( currentMetric );
                        lookupContext.SaveChanges();

                        if ( currentMetric.MetricCategories == null || !currentMetric.MetricCategories.Any( a => a.CategoryId == metricCategoryId ) )
                        {
                            metricCategoryService.Add( new MetricCategory { CategoryId = metricCategoryId, MetricId = currentMetric.Id } );
                            lookupContext.SaveChanges();
                        }

                        allMetrics.Add( currentMetric );
                    }

                    // create values for this metric
                    var metricValue = new MetricValue();
                    metricValue.MetricValueType = MetricValueType.Measure;
                    metricValue.CreatedByPersonAliasId = ImportPersonAliasId;
                    metricValue.CreatedDateTime = ImportDateTime;
                    metricValue.MetricValueDateTime = valueDate.Value.Date;
                    metricValue.MetricId = currentMetric.Id;
                    metricValue.Note = string.Empty;
                    metricValue.XValue = string.Empty;
                    metricValue.YValue = value;
                    metricValue.ForeignKey = string.Format( "Metric Value imported {0}", ImportDateTime );
                    metricValue.Note = metricNote;

                    if ( !string.IsNullOrWhiteSpace( metricCampus ) )
                    {
                        var campus = CampusDict.Values.Where( c => c.Name.Equals( metricCampus, StringComparison.OrdinalIgnoreCase )
                                || c.ShortCode.Equals( metricCampus, StringComparison.OrdinalIgnoreCase ) ).FirstOrDefault();

                        if ( campus == null )
                        {
                            var newCampus = new Campus();
                            newCampus.IsSystem = false;
                            newCampus.Name = metricCampus;
                            newCampus.ShortCode = metricCampus.RemoveWhitespace();
                            newCampus.IsActive = true;
                            lookupContext.Campuses.Add( newCampus );
                            lookupContext.SaveChanges( DisableAuditing );
                            CampusDict.Add( newCampus.ForeignKey, newCampus );
                            campus = newCampus;
                        }

                        if ( campus != null )
                        {
                            var metricPartitionCampusId = currentMetric.MetricPartitions.FirstOrDefault( p => p.Label == "Campus" ).Id;

                            metricValue.MetricValuePartitions.Add( new MetricValuePartition { MetricPartitionId = metricPartitionCampusId, EntityId = campus.Id } );
                        }
                    }

                    if ( valueDate.HasValue )
                    {
                        var metricPartitionScheduleId = currentMetric.MetricPartitions.FirstOrDefault( p => p.Label == "Service" ).Id;

                        var date = ( DateTime ) valueDate;
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
    }
}