using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CdcTools.CdcReader.Tables
{
    internal class TableSchemaQueryBuilder
    {
        public const string GetColumnsQuery = @"SELECT TABLE_SCHEMA	
	,TABLE_NAME
	,COLUMN_NAME
	,ORDINAL_POSITION
	,DATA_TYPE
	,CHARACTER_MAXIMUM_LENGTH
    ,IS_NULLABLE
    ,NUMERIC_SCALE
    ,NUMERIC_PRECISION
FROM INFORMATION_SCHEMA.COLUMNS";

        public static string GetColumnsOfTableQuery()
        {
            return GetColumnsQuery 
                + Environment.NewLine + "WHERE TABLE_NAME = @TableName"
                + Environment.NewLine + "AND TABLE_SCHEMA = @SchemaName";
        }

        public const string GetPrimaryKeysQuery = @"SELECT OBJECT_NAME(IC.OBJECT_ID) As TableName
    ,COL_NAME(IC.OBJECT_ID, IC.COLUMN_ID) AS ColumnName
    ,IC.KEY_ORDINAL AS OrdinalPosition
FROM SYS.INDEXES AS I
JOIN SYS.INDEX_COLUMNS AS IC ON  I.OBJECT_ID = IC.OBJECT_ID
     AND I.INDEX_ID = IC.INDEX_ID
WHERE I.IS_PRIMARY_KEY = 1";

        public static string GetPrimaryKeyColumnsOfTableQuery()
        {
            return GetPrimaryKeysQuery + Environment.NewLine + "AND OBJECT_NAME(IC.OBJECT_ID) = @TableName";
        }

        public static string GetExtractQueryUsingAllKeys(TableSchema tableSchema, int batchSize)
        {
            var sb = new StringBuilder();
            sb.Append($"SELECT TOP {batchSize} * FROM {tableSchema.Schema}.{tableSchema.TableName}");
            sb.Append(" WHERE ");
           
            foreach (var pk in tableSchema.PrimaryKeys.OrderBy(x => x.OrdinalPosition))
            {
                if (pk.OrdinalPosition > 1)
                    sb.Append(" AND ");
                sb.Append(pk.ColumnName);
                sb.Append(" > @p" + pk.OrdinalPosition);
            }
           
            sb.Append(" OPTION(RECOMPILE)");

            return sb.ToString();
        }
    }
}
