using System;
using System.Collections.Generic;
using System.Text;

namespace gpower2.gSqlUtils.QueryBuilder
{
    public class gSqlSearchCondition
    {
        public List<gSqlPredicate> Predicates { get; set; }
        public gSqlSearchCondition()
        {
            Predicates = new List<gSqlPredicate>();
        }

        public override string ToString()
        {
            if (Predicates.Count == 0)
            {
                return String.Empty;
            }
            StringBuilder searchConditionBuilder = new StringBuilder();
            for (Int32 i = 0; i < Predicates.Count; i++)
            {
                gSqlPredicate predicate = Predicates[i];
                if (i == 0)
                {
                    searchConditionBuilder.AppendFormat("({0} {1} {2}) \r\n",
                        predicate.LeftOperand
                        , gSqlOperatorTranslator.Translate(predicate.Operator)
                        , predicate.RightOperand);
                }
                else
                {
                    searchConditionBuilder.AppendFormat("{0} ({1} {2} {3}) \r\n",
                        gSqlPredicateParticipationTranslator.Translate(predicate.Participation)
                        , predicate.LeftOperand
                        , gSqlOperatorTranslator.Translate(predicate.Operator)
                        , predicate.RightOperand);
                }
            }
            return searchConditionBuilder.ToString();
        }
    }
}
