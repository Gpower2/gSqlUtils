﻿using System;
using System.Collections.Generic;

namespace gpower2.gSqlUtils.Mapper
{
    public class MapObject
    {
        public String ObjectName { get; set; }

        public String DatabaseName { get; set; }

        public List<MapProperty> ValueProperties { get; set; }
    }
}
