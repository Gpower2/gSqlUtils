using System;
using System.Collections.Generic;
using System.Text;

namespace gpower2.gSqlUtils.QueryBuilder
{
    public enum gSqlOperator
    {
        Equals,
        NotEquals,
        GreaterThan,
        GreaterThanOrEquals,
        NotGreaterThan,
        LessThan,
        LessThanOrEquals,
        NotLessThan,
        Like,
        In,
        Is
    }

    public class gSqlOperatorTranslator
    {
        private static gSqlOperatorTranslator _Instance = null;
        public static String Translate(gSqlOperator operat)
        {
            if (_Instance == null)
            {
                _Instance = new gSqlOperatorTranslator();
            }
            return _Instance._Dictionary[operat];
        }

        private Dictionary<gSqlOperator, String> _Dictionary;
        private gSqlOperatorTranslator()
        {
            _Dictionary = new Dictionary<gSqlOperator, string>();
            _Dictionary.Add(gSqlOperator.Equals, "=");
            _Dictionary.Add(gSqlOperator.NotEquals, "<>");
            _Dictionary.Add(gSqlOperator.GreaterThan, ">");
            _Dictionary.Add(gSqlOperator.GreaterThanOrEquals, ">=");
            _Dictionary.Add(gSqlOperator.NotGreaterThan, "!>");
            _Dictionary.Add(gSqlOperator.LessThan, "<");
            _Dictionary.Add(gSqlOperator.LessThanOrEquals, "<=");
            _Dictionary.Add(gSqlOperator.NotLessThan, "!<");
            _Dictionary.Add(gSqlOperator.Like, "LIKE");
            _Dictionary.Add(gSqlOperator.Is, "IS");
            _Dictionary.Add(gSqlOperator.In, "IN");
        }
    }
}
