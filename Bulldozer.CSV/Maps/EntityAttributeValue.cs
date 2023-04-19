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
using Rock.Data;
using Rock.Model;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;

namespace Bulldozer.CSV
{
    /// <summary>
    /// Partial of CSVComponent that holds the People import methods
    /// </summary>
    partial class CSVComponent
    {
        /// <summary>
        /// Processes Entity Attributes.
        /// </summary>
        private int ImportAttributeValues( List<AttributeValueImport> attributeValueImports, RockContext rockContext = null )
        {
            if ( rockContext == null )
            {
                rockContext = new RockContext();
            }
            var attributeValueLookup = new AttributeValueService( rockContext ).Queryable()
                                               .Where( v => !string.IsNullOrEmpty( v.ForeignKey ) && v.ForeignKey.StartsWith( ImportInstanceFKPrefix + "^" ) )
                                               .Select( a => new
                                               {
                                                   AttributeValue = a,
                                                   a.ForeignKey
                                               } )
                                               .ToDictionary( k => k.ForeignKey, v => v.AttributeValue );

            // Slice data into chunks and process
            var attributeValuesRemainingToProcess = attributeValueImports.Count;
            var workingAVImportList = attributeValueImports.ToList();
            var completed = 0;

            while ( attributeValuesRemainingToProcess > 0 )
            {
                if ( completed > 0 && completed % ( this.DefaultChunkSize * 10 ) < 1 )
                {
                    ReportProgress( 0, $"{completed} AttributeValues processed." );
                }

                if ( completed % this.DefaultChunkSize < 1 )
                {
                    var csvChunk = workingAVImportList.Take( Math.Min( this.DefaultChunkSize, workingAVImportList.Count ) ).ToList();
                    var imported = BulkAttributeValueImport( rockContext, csvChunk, attributeValueLookup );
                    completed += imported;
                    attributeValuesRemainingToProcess -= csvChunk.Count;
                    workingAVImportList.RemoveRange( 0, csvChunk.Count );
                    ReportPartialProgress();
                }
            }
            return completed;
        }

        public int BulkAttributeValueImport( RockContext rockContext, List<AttributeValueImport> attributeValueImports, Dictionary<string, AttributeValue> attributeValueLookup )
        {
            var importedDateTime = RockDateTime.Now;
            var attributeValuesToInsert = new List<AttributeValue>();
            var attributeValues = attributeValueImports.Where( v => !attributeValueLookup.ContainsKey( v.AttributeValueForeignKey ) ).ToList();

            foreach ( var attributeValue in attributeValues )
            {
                var newAttributeValue = new AttributeValue
                {
                    EntityId = attributeValue.EntityId,
                    AttributeId = attributeValue.AttributeId,
                    Value = attributeValue.Value,
                    CreatedDateTime = importedDateTime,
                    ModifiedDateTime = importedDateTime,
                    ForeignKey = attributeValue.AttributeValueForeignKey
                };
                attributeValuesToInsert.Add( newAttributeValue );
            }
            rockContext.BulkInsert( attributeValuesToInsert );

            return attributeValueImports.Count;
        }
    }
}
