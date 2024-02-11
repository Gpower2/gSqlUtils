using System;

namespace gpower2.gSqlUtils.QueryBuilder
{
    public class gSqlPredicate
    {
        public gSqlPredicateParticipation Participation { get; set; }
        public String LeftOperand { get; set; }
        public gSqlOperator Operator { get; set; }
        public String RightOperand { get; set; }

    }
}
