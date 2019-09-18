using System;
using System.Collections.Generic;
using System.Text;

namespace gpower2.gSqlUtils.QueryBuilder
{
    public enum gSqlPredicateParticipation
    {
        AND,
        OR,
        NOT,
        AND_NOT,
        OR_NOT
    }

    public class gSqlPredicateParticipationTranslator
    {
        private static gSqlPredicateParticipationTranslator _Instance = null;
        public static String Translate(gSqlPredicateParticipation participation){
            if(_Instance == null){
                _Instance = new gSqlPredicateParticipationTranslator();
            }
            return _Instance._Dictionary[participation];
        }

        private Dictionary<gSqlPredicateParticipation, String> _Dictionary;
        private gSqlPredicateParticipationTranslator()
        {
            _Dictionary = new Dictionary<gSqlPredicateParticipation, string>();
            _Dictionary.Add(gSqlPredicateParticipation.AND, "AND");
            _Dictionary.Add(gSqlPredicateParticipation.OR, "OR");
            _Dictionary.Add(gSqlPredicateParticipation.NOT, "NOT");
            _Dictionary.Add(gSqlPredicateParticipation.AND_NOT, "AND NOT");
            _Dictionary.Add(gSqlPredicateParticipation.OR_NOT, "OR NOT");
        }

    }
}
