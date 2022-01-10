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
using System.Text;
using Rock.Data;

namespace Bulldozer.Utility
{
    /// <summary>
    /// Custom Migration methods
    /// </summary>
    public class Migration
    {
        private static void Sql( string sql )
        {
            using ( var context = new RockContext() )
            {
                context.Database.ExecuteSqlCommand( sql );
            }
        }

        #region Block Methods

        /// <summary>
        /// Adds a new Block of the given block type to the given page (optional) and layout (optional),
        /// setting its values with the given parameter values. If only the layout is given,
        /// edit/configuration authorization will also be inserted into the Auth table
        /// for the admin role (GroupId 2).
        /// </summary>
        /// <param name="pageGuid">The page GUID.</param>
        /// <param name="layoutGuid">The layout GUID.</param>
        /// <param name="blockTypeGuid">The block type GUID.</param>
        /// <param name="name">The name.</param>
        /// <param name="zone">The zone.</param>
        /// <param name="preHtml">The pre HTML.</param>
        /// <param name="postHtml">The post HTML.</param>
        /// <param name="order">The order.</param>
        /// <param name="guid">The unique identifier.</param>
        public static void AddBlock( string pageGuid, string layoutGuid, string blockTypeGuid, string name, string zone, string preHtml, string postHtml, int order, string guid )
        {
            AddBlock( false, pageGuid, layoutGuid, blockTypeGuid, name, zone, preHtml, postHtml, order, guid );
        }

        /// <summary>
        /// Adds a new Block of the given block type to the given page (optional) and layout (optional),
        /// setting its values with the given parameter values. If only the layout is given,
        /// edit/configuration authorization will also be inserted into the Auth table
        /// for the admin role (GroupId 2).
        /// </summary>
        /// <param name="skipIfAlreadyExists">if set to <c>true</c>, the block will only be added if it doesn't already exist </param>
        /// <param name="pageGuid">The page GUID.</param>
        /// <param name="layoutGuid">The layout GUID.</param>
        /// <param name="blockTypeGuid">The block type GUID.</param>
        /// <param name="name">The name.</param>
        /// <param name="zone">The zone.</param>
        /// <param name="preHtml">The pre HTML.</param>
        /// <param name="postHtml">The post HTML.</param>
        /// <param name="order">The order.</param>
        /// <param name="guid">The unique identifier.</param>
        public static void AddBlock( bool skipIfAlreadyExists, string pageGuid, string layoutGuid, string blockTypeGuid, string name, string zone, string preHtml, string postHtml, int order, string guid )
        {
            var sb = new StringBuilder();
            sb.Append( @"
                DECLARE @PageId int
                SET @PageId = null
                DECLARE @LayoutId int
                SET @LayoutId = null
" );

            if ( !string.IsNullOrWhiteSpace( pageGuid ) )
            {
                sb.AppendFormat( @"
                SET @PageId = (SELECT [Id] FROM [Page] WHERE [Guid] = '{0}')
", pageGuid );
            }

            if ( !string.IsNullOrWhiteSpace( layoutGuid ) )
            {
                sb.AppendFormat( @"
                SET @LayoutId = (SELECT [Id] FROM [Layout] WHERE [Guid] = '{0}')
", layoutGuid );
            }

            sb.AppendFormat( @"
                DECLARE @BlockTypeId int
                SET @BlockTypeId = (SELECT [Id] FROM [BlockType] WHERE [Guid] = '{0}')
                DECLARE @EntityTypeId int
                SET @EntityTypeId = (SELECT [Id] FROM [EntityType] WHERE [Name] = 'Rock.Model.Block')
                DECLARE @BlockId int
                INSERT INTO [Block] (
                    [IsSystem],[PageId],[LayoutId],[BlockTypeId],[Zone],
                    [Order],[Name],[PreHtml],[PostHtml],[OutputCacheDuration],
                    [Guid])
                VALUES(
                    1,@PageId,@LayoutId,@BlockTypeId,'{1}',
                    {2},'{3}','{4}','{5}',0,
                    '{6}')
                SET @BlockId = SCOPE_IDENTITY()
",
                    blockTypeGuid,
                    zone,
                    order,
                    name.Replace( "'", "''" ),
                    preHtml.Replace( "'", "''" ),
                    postHtml.Replace( "'", "''" ),
                    guid );

            // If adding a layout block, give edit/configuration authorization to admin role
            if ( string.IsNullOrWhiteSpace( pageGuid ) )
                sb.Append( @"
                INSERT INTO [Auth] ([EntityTypeId],[EntityId],[Order],[Action],[AllowOrDeny],[SpecialRole],[GroupId],[Guid])
                    VALUES(@EntityTypeId,@BlockId,0,'Edit','A',0,2,NEWID())
                INSERT INTO [Auth] ([EntityTypeId],[EntityId],[Order],[Action],[AllowOrDeny],[SpecialRole],[GroupId],[Guid])
                    VALUES(@EntityTypeId,@BlockId,0,'Configure','A',0,2,NEWID())
" );

            var addBlockSQL = sb.ToString();

            if ( skipIfAlreadyExists )
            {
                addBlockSQL = $"if not exists (select * from [Block] where [Guid] = '{guid}') begin\n" + addBlockSQL + "\nend";
            }

            Sql( addBlockSQL );
        }

        #endregion

        #region Block Attribute Value Methods

        /// <summary>
        /// Adds a new block attribute value for the given block guid and attribute guid,
        /// deleting any previously existing attribute value first.
        /// </summary>
        /// <param name="blockGuid">The block GUID.</param>
        /// <param name="attributeGuid">The attribute GUID.</param>
        /// <param name="value">The value.</param>
        /// <param name="appendToExisting">if set to <c>true</c> appends the value to the existing value instead of replacing.</param>
        public static void AddBlockAttributeValue( string blockGuid, string attributeGuid, string value, bool appendToExisting = false )
        {
            AddBlockAttributeValue( false, blockGuid, attributeGuid, value, appendToExisting );
        }

        /// <summary>
        /// Adds a new block attribute value for the given block guid and attribute guid,
        /// deleting any previously existing attribute value first.
        /// </summary>
        /// <param name="skipIfAlreadyExists">if set to <c>true</c>, the block attribute value will only be set if it doesn't already exist (based on Attribute.Guid and Block.Guid)</param>
        /// <param name="blockGuid">The block GUID.</param>
        /// <param name="attributeGuid">The attribute GUID.</param>
        /// <param name="value">The value.</param>
        /// <param name="appendToExisting">if set to <c>true</c> appends the value to the existing value instead of replacing.</param>
        public static void AddBlockAttributeValue( bool skipIfAlreadyExists, string blockGuid, string attributeGuid, string value, bool appendToExisting = false )
        {
            var addBlockValueSQL = string.Format( @"
                DECLARE @BlockId int
                SET @BlockId = (SELECT [Id] FROM [Block] WHERE [Guid] = '{0}')
                DECLARE @AttributeId int
                SET @AttributeId = (SELECT [Id] FROM [Attribute] WHERE [Guid] = '{1}')
                IF @BlockId IS NOT NULL AND @AttributeId IS NOT NULL
                BEGIN
                    DECLARE @TheValue NVARCHAR(MAX) = '{2}'
                    -- If appendToExisting (and any current value exists), get the current value before we delete it...
                    IF 1 = {3} AND EXISTS (SELECT 1 FROM [AttributeValue] WHERE [AttributeId] = @AttributeId AND [EntityId] = @BlockId )
                    BEGIN
                        SET @TheValue = (SELECT [Value] FROM [AttributeValue] WHERE [AttributeId] = @AttributeId AND [EntityId] = @BlockId )
                        -- If the new value is not in the old value, append it.
                        IF CHARINDEX( '{2}', @TheValue ) = 0
                        BEGIN
                            SET @TheValue = (SELECT @TheValue + ',' + '{2}' )
                        END
                    END
                    -- Delete existing attribute value first (might have been created by Rock system)
                    DELETE [AttributeValue]
                    WHERE [AttributeId] = @AttributeId
                    AND [EntityId] = @BlockId
                    INSERT INTO [AttributeValue] (
                        [IsSystem],[AttributeId],[EntityId],
                        [Value],
                        [Guid])
                    VALUES(
                        1,@AttributeId,@BlockId,
                        @TheValue,
                        NEWID())
                END
",
                    blockGuid,
                    attributeGuid,
                    value.Replace( "'", "''" ),
                    ( appendToExisting ? "1" : "0" )
                );

            if ( skipIfAlreadyExists )
            {
                addBlockValueSQL = $@"IF NOT EXISTS (
		SELECT *
		FROM [AttributeValue] av
		INNER JOIN [Attribute] a ON av.AttributeId = a.Id
		INNER JOIN [Block] b ON av.EntityId = b.Id
		WHERE b.[Guid] = '{blockGuid}'
			AND a.[Guid] = '{attributeGuid}'
		)
BEGIN
" + addBlockValueSQL + "\nEND";
            }

            Sql( addBlockValueSQL );
        }

        #endregion
    }
}
