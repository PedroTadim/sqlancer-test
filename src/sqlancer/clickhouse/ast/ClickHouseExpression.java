package sqlancer.clickhouse.ast;

import sqlancer.common.visitor.UnaryOperation;

public abstract class ClickHouseExpression {

    public ClickHouseConstant getExpectedValue() {
        return null;
    }

    public enum TypeAffinity {
        NOTHING, UINT8, UINT16, UINT32, UINT64, UINT128, INT8, INT16, INT32, INT64, INT128, FLOAT32, FLOAT64, DATE,
        DATETIME, DATETIME64, STRING, FIXEDSTRING, ENUM8, ENUM16, DECIMAL32, DECIMAL64, DECIMAL128, UUID, ARRAY, TUPLE,
        SET, INTERVAL;
        // NULLABLE, FUNCTION, AGGREGATEFUNCTION, LOWCARDINALITY;

        public boolean isNumeric() {
            return this == UINT8 || this == UINT16 || this == UINT32 || this == UINT64 || this == UINT128
                    || this == INT8 || this == INT16 || this == INT32 || this == INT64 || this == INT128
                    || this == FLOAT32 || this == FLOAT64;
        }
    }

    public static class ClickHouseExist extends ClickHouseExpression {

        private final ClickHouseExpression select;

        public ClickHouseExist(ClickHouseExpression select) {
            this.select = select;
        }

        public ClickHouseExpression getExpression() {
            return select;
        }
    }

    public static class ClickHouseJoin extends ClickHouseExpression {
        public enum JoinType {
            NONE, INNER, CROSS, LEFT, RIGHT, FULL;
        }
        // LEFT_SEMI, RIGHT_SEMI are not deterministic as ClickHouse allows to read columns from
        // whitelist table as well
        public enum JoinModifier {
            NONE, OUTER, ANTI, ANY, ALL, ASOF;
        }

        private final ClickHouseTableReference leftTable;
        private final ClickHouseTableReference rightTable;
        private ClickHouseBinaryComparisonOperation onClause;
        private final ClickHouseJoin.JoinType type;
        private final ClickHouseJoin.JoinModifier modifier;

        /* This method checks for compatability between the JoinType and JoinModifier */
        private ClickHouseJoin.JoinModifier setModifierBasedOnType(ClickHouseJoin.JoinType type, ClickHouseJoin.JoinModifier modifier) {
            if ((modifier == ClickHouseJoin.JoinModifier.OUTER && (type == ClickHouseJoin.JoinType.INNER || type == ClickHouseJoin.JoinType.CROSS || type == ClickHouseJoin.JoinType.NONE)) ||
                (modifier == ClickHouseJoin.JoinModifier.ANTI && type != ClickHouseJoin.JoinType.LEFT && type != ClickHouseJoin.JoinType.RIGHT) ||
                ((modifier == ClickHouseJoin.JoinModifier.ANY || modifier == ClickHouseJoin.JoinModifier.ALL) && (type == ClickHouseJoin.JoinType.CROSS || type == ClickHouseJoin.JoinType.NONE)) ||
                (modifier == ClickHouseJoin.JoinModifier.ASOF && (type != ClickHouseJoin.JoinType.LEFT && type != ClickHouseJoin.JoinType.NONE))) {
                return JoinModifier.NONE;
            }
            return modifier;
        }

        public ClickHouseJoin(ClickHouseTableReference leftTable, ClickHouseTableReference rightTable,
                ClickHouseJoin.JoinType type, ClickHouseJoin.JoinModifier modifier, ClickHouseBinaryComparisonOperation onClause) {
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.onClause = onClause;
            this.type = type;
            this.modifier = setModifierBasedOnType(type, modifier);
        }

        public ClickHouseJoin(ClickHouseTableReference leftTable, ClickHouseTableReference rightTable,
                ClickHouseJoin.JoinType type, ClickHouseJoin.JoinModifier modifier) {
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.onClause = null;
            this.type = type;
            this.modifier = setModifierBasedOnType(type, modifier);
        }

        public ClickHouseTableReference getLeftTable() {
            return leftTable;
        }

        public ClickHouseTableReference getRightTable() {
            return rightTable;
        }

        public ClickHouseExpression getOnClause() {
            return onClause;
        }

        public ClickHouseJoin.JoinType getType() {
            return type;
        }

        public ClickHouseJoin.JoinModifier getModifier() {
            return modifier;
        }

        public void setOnClause(ClickHouseBinaryComparisonOperation onClause) {
            this.onClause = onClause;
        }

    }

    public static class ClickHouseSubquery extends ClickHouseExpression {

        private final String query;

        public ClickHouseSubquery(String query) {
            this.query = query;
        }

        public static ClickHouseExpression create(String query) {
            return new ClickHouseSubquery(query);
        }

        public String getQuery() {
            return query;
        }
    }

    public static class ClickHousePostfixText extends ClickHouseExpression
            implements UnaryOperation<ClickHouseExpression> {

        private final ClickHouseExpression expr;
        private final String text;
        private ClickHouseConstant expectedValue;

        public ClickHousePostfixText(ClickHouseExpression expr, String text, ClickHouseConstant expectedValue) {
            this.expr = expr;
            this.text = text;
            this.expectedValue = expectedValue;
        }

        public ClickHousePostfixText(String text, ClickHouseConstant expectedValue) {
            this(null, text, expectedValue);
        }

        public String getText() {
            return text;
        }

        @Override
        public ClickHouseConstant getExpectedValue() {
            return expectedValue;
        }

        @Override
        public ClickHouseExpression getExpression() {
            return expr;
        }

        @Override
        public String getOperatorRepresentation() {
            return getText();
        }

        @Override
        public OperatorKind getOperatorKind() {
            return OperatorKind.POSTFIX;
        }

        @Override
        public boolean omitBracketsWhenPrinting() {
            return true;
        }
    }
}
