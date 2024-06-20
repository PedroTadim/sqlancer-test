package sqlancer.clickhouse.ast;

import sqlancer.Randomly;
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
        private ClickHouseExpression onClause;
        private final ClickHouseJoin.JoinType type;
        private final ClickHouseJoin.JoinModifier modifier;

        /* This method checks for compatability between the JoinType and JoinModifier */
        private ClickHouseJoin.JoinModifier setModifierBasedOnType(ClickHouseJoin.JoinType type, ClickHouseJoin.JoinModifier modifier) {
            if ((modifier == ClickHouseJoin.JoinModifier.OUTER && (type == ClickHouseJoin.JoinType.INNER || type == ClickHouseJoin.JoinType.CROSS || type == ClickHouseJoin.JoinType.NONE)) ||
                (modifier == ClickHouseJoin.JoinModifier.ANTI && type != ClickHouseJoin.JoinType.LEFT && type != ClickHouseJoin.JoinType.RIGHT) ||
                (modifier == ClickHouseJoin.JoinModifier.ALL && (type == ClickHouseJoin.JoinType.CROSS || type == ClickHouseJoin.JoinType.NONE)) ||
                (modifier == ClickHouseJoin.JoinModifier.ANY && (type == ClickHouseJoin.JoinType.CROSS || type == ClickHouseJoin.JoinType.NONE || type == ClickHouseJoin.JoinType.FULL)) ||
                (modifier == ClickHouseJoin.JoinModifier.ASOF && (type != ClickHouseJoin.JoinType.LEFT && type != ClickHouseJoin.JoinType.NONE))) {
                return JoinModifier.NONE;
            }
            return modifier;
        }

        public ClickHouseJoin(ClickHouseTableReference leftTable, ClickHouseTableReference rightTable,
                ClickHouseJoin.JoinType type, ClickHouseJoin.JoinModifier modifier, ClickHouseExpression onClause) {
            this.leftTable = leftTable;
            this.rightTable = rightTable;
            this.type = type;
            this.modifier = setModifierBasedOnType(type, modifier);
            if (type != JoinType.CROSS) {
                this.onClause = onClause;
            }
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

        public void setOnClause(ClickHouseExpression onClause) {
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

    public static class ClickHouseSetting extends ClickHouseExpression {

        public enum ClickHouseSingleSetting {
            JOIN_ALGORITHM("join_algorithm", new String[]{"'default'", "'grace_hash'", "'hash'", "'parallel_hash'", "'parallel_hash'",
                                                                             "'partial_merge'", "'direct'", "'auto'", "'full_sorting_merge'", "'prefer_partial_merge'"}),
            JOIN_ANY_TAKE_LAST_ROW("join_any_take_last_row", new String[]{"0", "1"}),
            JOIN_USE_NULLS("join_use_nulls", new String[]{"0", "1"}),
            PARTIAL_MERGE_JOIN_OPTIMIZATIONS("partial_merge_join_optimizations", new String[]{"0", "1"}),
            PARTIAL_MERGE_JOIN_ROWS_IN_RIGHT_BLOCKS("partial_merge_join_rows_in_right_blocks", new String[]{"1000", "10000", "32000", "65536", "100000"}),
            JOIN_ON_DISK_MAX_FILES_TO_MERGE("join_on_disk_max_files_to_merge", new String[]{"2", "3", "4", "64"}),
            ANY_JOIN_DISTINCT_RIGHT_TABLE_KEYS("any_join_distinct_right_table_keys", new String[]{"0", "1"}),
            AGGREGATE_FUNCTIONS_NULL_FOR_EMPTY("aggregate_functions_null_for_empty", new String[]{"0", "1"}),
            ENABLE_OPTIMIZE_PREDICATE_EXPRESSION("enable_optimize_predicate_expression", new String[]{"0", "1"});

            private final String textRepresentation;

            private final String[] possibleValues;

            ClickHouseSingleSetting(String textRepresentation, String[] possibleValues) {
                this.textRepresentation = textRepresentation;
                this.possibleValues = possibleValues.clone();
            }

            public String getPossibleValue() {
                return Randomly.fromOptions(this.possibleValues);
            }

            public String getTextRepresentation() {
                return this.textRepresentation;
            }
        }

        public static ClickHouseSingleSetting[] possibleSettings = {
            ClickHouseSingleSetting.JOIN_ALGORITHM,
            ClickHouseSingleSetting.JOIN_ANY_TAKE_LAST_ROW,
            ClickHouseSingleSetting.JOIN_USE_NULLS,
            ClickHouseSingleSetting.PARTIAL_MERGE_JOIN_OPTIMIZATIONS,
            ClickHouseSingleSetting.PARTIAL_MERGE_JOIN_ROWS_IN_RIGHT_BLOCKS,
            ClickHouseSingleSetting.JOIN_ON_DISK_MAX_FILES_TO_MERGE,
            ClickHouseSingleSetting.ANY_JOIN_DISTINCT_RIGHT_TABLE_KEYS,
            ClickHouseSingleSetting.AGGREGATE_FUNCTIONS_NULL_FOR_EMPTY,
            ClickHouseSingleSetting.ENABLE_OPTIMIZE_PREDICATE_EXPRESSION
        };

        private final ClickHouseSingleSetting key;

        private final String value;

        public ClickHouseSetting(ClickHouseSingleSetting setting) {
            this.key = setting;
            this.value = key.getPossibleValue();
        }

        public ClickHouseSetting(ClickHouseSingleSetting setting, String value) {
            this.key = setting;
            this.value = value;
        }

        public ClickHouseSingleSetting getKey() {
            return key;
        }

        public String getValue() {
            return value;
        }
    }
}
