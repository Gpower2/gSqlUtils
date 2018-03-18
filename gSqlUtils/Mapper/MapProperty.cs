using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace gpower2.gSqlUtils.Mapper
{
    public enum DatabasePropertyType
    {
        INT
        ,BIGINT
        ,FLOAT
        ,NUMERIC
        ,VARCHAR
        ,NVARCHAR
        ,TEXT
        ,NTEXT
        ,VARBINARY
    }

    public class MapProperty
    {
        public String ObjectName { get; set; }

        public String DatabaseName { get; set; }

        public bool IsNullable { get; set; }

        public bool IsID { get; set; }

        public bool IsUnique { get; set; }

        public DatabasePropertyType DatabaseType { get; set; }

        public int? Length { get; set; }

        public int? Decimals { get; set; }
    }
}
