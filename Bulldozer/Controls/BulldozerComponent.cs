﻿// <copyright>
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
using System.Data;
using System.Linq;
using Rock;

namespace Bulldozer
{
    /// <summary>
    /// Provides a type-safe reference to report progress to the UI
    /// </summary>
    /// <param name="value">The value.</param>
    /// <param name="status">The status.</param>
    public delegate void ReportProgress( long value, string status );

    /// <summary>
    /// Bulldozer holds the base methods and properties needed to convert data to Rock
    /// </summary>
    public abstract class BulldozerComponent
    {
        #region Fields

        /// <summary>
        /// Gets the full name of the bulldozer type.
        /// </summary>
        /// <value>
        /// The name of the database being imported.
        /// </value>
        public abstract string FullName
        {
            get;
        }

        /// <summary>
        /// Gets the supported file extension type.
        /// </summary>
        /// <value>
        /// The supported extension type.
        /// </value>
        public abstract string ExtensionType
        {
            get;
        }

        /// <summary>
        /// Report progress when a multiple of this number has been imported
        /// </summary>
        public int ReportingNumber = 100;

        /// <summary>
        /// Determine if the anonymous giver should be required
        /// </summary>
        public Boolean requireAnonymousGiver = true;

        /// <summary>
        /// Determine if CSV Batch Map should use ForeignKey. If false, ForeignId will be used.
        /// </summary>
        public Boolean csvBatchUseForeignKey = true;

        /// <summary>
        /// Determine if the Individual List should be refreshed after every save. Warning, this will slow the process.
        /// </summary>
        public Boolean refreshIndividualListEachCycle = false;

        /// <summary>
        /// Holds a reference to the data nodes loaded in memory
        /// </summary>
        public List<DataNode> DataNodes;

        /// <summary>
        /// Flag to set postprocessing audits on save
        /// </summary>
        public static bool DisableAuditing = true;

        /// <summary>
        /// Gets the import date and time
        /// </summary>
        public static DateTime ImportDateTime = RockDateTime.Now;

        #endregion Fields

        #region Methods

        /// <summary>
        /// Returns the full name of this bulldozer type.
        /// </summary>
        /// <returns>
        /// A <see cref="System.String" /> that represents this instance.
        /// </returns>
        public override string ToString()
        {
            return FullName;
        }

        /// <summary>
        /// Loads the database into memory and fills a DataNode instance.
        /// </summary>
        /// <param name="fileName">Name of the file.</param>
        /// <returns></returns>
        public abstract bool LoadSchema( string fileName );

        /// <summary>
        /// Previews the data.
        /// </summary>
        /// <param name="nodeId">The node identifier.</param>
        /// <returns></returns>
        public virtual DataTable PreviewData( string nodeId )
        {
            var node = DataNodes.FirstOrDefault( n => n.Id.Equals( nodeId ) || n.Children.Any( c => c.Id == nodeId ) );
            if ( node != null && node.Children.Any() )
            {
                var dataTable = new DataTable();
                foreach ( var column in node.Children )
                {
                    dataTable.Columns.Add( column.Name, column.NodeType );
                }

                var rowPreview = dataTable.NewRow();
                foreach ( var column in node.Children )
                {
                    rowPreview[column.Name] = column.Value ?? DBNull.Value;
                }

                dataTable.Rows.Add( rowPreview );
                return dataTable;
            }

            return null;
        }

        /// <summary>
        /// Transforms the data.
        /// </summary>
        /// <param name="settings">The settings.</param>
        /// <returns></returns>
        public abstract int TransformData( Dictionary<string, string> settings );

        #endregion Methods

        #region Events

        /// <summary>
        /// Occurs when progress updated.
        /// </summary>
        public event ReportProgress ProgressUpdated;

        /// <summary>
        /// Reports the progress with a custom status.
        /// </summary>
        /// <param name="progress">The progress.</param>
        /// <param name="status">The status.</param>
        public void ReportProgress( long progress, string status )
        {
            ProgressUpdated?.Invoke( progress, Environment.NewLine + DateTime.Now.ToLongTimeString() + "  " + status );
        }

        /// <summary>
        /// Reports a partial progress with extra ellipses
        /// </summary>
        /// <param name="progress">The progress.</param>
        /// <param name="status">The status.</param>
        public void ReportPartialProgress()
        {
            if ( ProgressUpdated != null )
            {
                ProgressUpdated( 0, "." );
            }
        }

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="category">The category.</param>
        /// <param name="message">The message.</param>
        public static void LogException( string category, string message )
        {
            App.LogException( category, message );
        }

        #endregion Events
    }
}
