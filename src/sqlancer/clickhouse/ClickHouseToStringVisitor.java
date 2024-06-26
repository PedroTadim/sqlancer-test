package sqlancer.clickhouse;

import java.util.List;

import sqlancer.clickhouse.ast.ClickHouseAggregate;
import sqlancer.clickhouse.ast.ClickHouseAliasOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryFunctionOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryLogicalOperation;
import sqlancer.clickhouse.ast.ClickHouseCastOperation;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import sqlancer.clickhouse.ast.ClickHouseExpression.ClickHouseSetting;
import sqlancer.common.visitor.ToStringVisitor;

public class ClickHouseToStringVisitor extends ToStringVisitor<ClickHouseExpression> implements ClickHouseVisitor {

    @Override
    public void visitSpecific(ClickHouseExpression expr) {
        ClickHouseVisitor.super.visit(expr);
    }

    @Override
    public void visit(ClickHouseBinaryLogicalOperation op) {
        sb.append("(");
        visit(op.getLeft());
        sb.append(") ");
        sb.append(op.getTextRepresentation());
        sb.append(" (");
        visit(op.getRight());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseSetting op) {
        sb.append(op.getKey().getTextRepresentation());
        sb.append(" = ");
        sb.append(op.getValue());
    }

    @Override
    public void visit(ClickHouseUnaryPrefixOperation op) {
        sb.append(op.getOperatorRepresentation());
        sb.append(" (");
        visit(op.getExpression());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseUnaryPostfixOperation op) {
        sb.append("(");
        visit(op.getExpression());
        sb.append(")");
        sb.append(" ");
        sb.append(op.getOperatorRepresentation());
    }

    @Override
    public void visit(ClickHouseConstant constant) {
        sb.append(constant.toString());
    }

    @Override
    public void visit(ClickHouseSelect select, boolean inner) {
        if (inner) {
            sb.append("(");
        }
        sb.append("SELECT ");
        switch (select.getFromOptions()) {
        case DISTINCT:
            sb.append("DISTINCT ");
            break;
        case ALL:
            sb.append("");
            break;
        default:
            throw new AssertionError(select.getFromOptions());
        }

        visit(select.getFetchColumns());
        ClickHouseExpression fromClause = select.getFromClause();
        if (fromClause != null) {
            sb.append(" FROM ");
            visit(fromClause);
        }
        List<ClickHouseExpression.ClickHouseJoin> joins = select.getJoinClauses();
        if (!joins.isEmpty()) {
            for (ClickHouseExpression.ClickHouseJoin join : joins) {
                visit(join);
            }
        }
        if (select.getWhereClause() != null) {
            sb.append(" WHERE ");
            visit(select.getWhereClause());
        }
        if (!select.getGroupByClause().isEmpty()) {
            sb.append(" GROUP BY ");
            visit(select.getGroupByClause());
        }
        if (select.getHavingClause() != null) {
            sb.append(" HAVING ");
            visit(select.getHavingClause());
        }
        if (!select.getOrderByClauses().isEmpty()) {
            sb.append(" ORDER BY ");
            visit(select.getOrderByClauses());
        }
        if (!select.getSettings().isEmpty()) {
            int i = 0;
            List<ClickHouseExpression.ClickHouseSetting> settings = select.getSettings();

            sb.append(" SETTINGS ");
            for (ClickHouseExpression.ClickHouseSetting setting : settings) {
                if (i++ != 0) {
                    sb.append(", ");
                }
                visit(setting);
            }
        }
        if (inner) {
            sb.append(")");
        }
    }

    @Override
    public void visit(ClickHouseTableReference tableReference) {
        sb.append(tableReference.getTable().getName()); // Original name, not alias.
        String alias = tableReference.getAlias();
        if (alias != null) {
            sb.append(" AS " + alias);
        }

    }

    @Override
    public void visit(ClickHouseAggregate aggregate) {
        sb.append(aggregate.getFunc());
        sb.append("(");
        visit(aggregate.getExpr());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseCastOperation cast) {
        sb.append("CAST(");
        visit(cast.getExpression());
        sb.append(" AS ");
        sb.append(cast.getType().toString());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseExpression.ClickHouseJoin join) {
        ClickHouseExpression.ClickHouseJoin.JoinType type = join.getType();
        ClickHouseExpression.ClickHouseJoin.JoinModifier modifier = join.getModifier();
        if (type != ClickHouseExpression.ClickHouseJoin.JoinType.NONE) {
            sb.append(" ");
            switch (type) {
                case CROSS:
                    sb.append("CROSS");
                    break;
                case INNER:
                    sb.append("INNER");
                    break;
                case LEFT:
                    sb.append("LEFT");
                    break;
                case RIGHT:
                    sb.append("RIGHT");
                    break;
                case FULL:
                    sb.append("FULL");
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        if (modifier != ClickHouseExpression.ClickHouseJoin.JoinModifier.NONE) {
            sb.append(" ");
            switch (modifier) {
                case OUTER:
                    sb.append("OUTER");
                    break;
                case ANTI:
                    sb.append("ANTI");
                    break;
                case ANY:
                    sb.append("ANY");
                    break;
                case ALL:
                    sb.append("ALL");
                    break;
                case ASOF:
                    sb.append("ASOF");
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        sb.append(" JOIN ");
        visit(join.getRightTable());
        ClickHouseExpression onClause = join.getOnClause();
        if (onClause != null) {
            sb.append(" ON ");
            visit(onClause);
        }
    }

    @Override
    public void visit(ClickHouseColumnReference c) {
        if (c.getTableAlias() != null) {
            sb.append(c.getTableAlias());
            sb.append(".");
            sb.append(c.getColumn().getName());
        } else if (c.getColumn().getTable() == null) {
            sb.append(c.getColumn().getName());
        } else {
            sb.append(c.getColumn().getFullQualifiedName());
        }
        if (c.getAlias() != null) {
            sb.append(" AS " + c.getAlias());
        }
    }

    @Override
    public void visit(ClickHouseBinaryFunctionOperation func) {
        sb.append(func.getOperatorRepresentation());
        sb.append("(");
        visit(func.getLeft());
        sb.append(",");
        visit(func.getRight());
        sb.append(")");
    }

    @Override
    public void visit(ClickHouseAliasOperation alias) {
        visit(alias.getExpression());
        sb.append(" AS `");
        sb.append(alias.getAlias());
        sb.append("`");
    }

    public static String asString(ClickHouseExpression expr) {
        ClickHouseToStringVisitor visitor = new ClickHouseToStringVisitor();
        visitor.visit(expr);
        return visitor.get();
    }
}
