using Bulldozer.Model;
using Bulldozer.Utility;
using Rock;
using Rock.Data;
using Rock.Model;
using Rock.Web.Cache;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using static Bulldozer.CSV.CSVInstance;

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

        public int BulkAttributeValueImport( RockContext rockContext, List<AttributeValueImport> attributeValueImports, Dictionary<string,AttributeValue> attributeValueLookup )
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
