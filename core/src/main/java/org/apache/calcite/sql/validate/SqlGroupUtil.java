package org.apache.calcite.sql.validate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.validate.SqlValidatorUtil.GroupAnalyzer;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class SqlGroupUtil {

	public static int lookupGroupExpr(GroupAnalyzer groupAnalyzer, SqlNode expr) {
		for (Ord<SqlNode> node : Ord.zip(groupAnalyzer.groupExprs)) {
			if (node.e.equalsDeep(expr, Litmus.IGNORE)) {
				return node.i;
			}
		}

		switch (expr.getKind()) {
		case HOP:
		case TUMBLE:
		case SESSION:
			groupAnalyzer.extraExprs.add(expr);
			break;
		}
		groupAnalyzer.groupExprs.add(expr);
		return groupAnalyzer.groupExprs.size() - 1;
	}

	/** Analyzes a component of a tuple in a GROUPING SETS clause. */
	public static ImmutableBitSet analyzeGroupExpr(SqlValidatorScope scope, GroupAnalyzer groupAnalyzer,
			SqlNode groupExpr) {
		final SqlNode expandedGroupExpr = scope.getValidator().expand(groupExpr, scope);

		switch (expandedGroupExpr.getKind()) {
		case ROW:
			return ImmutableBitSet
					.union(analyzeGroupTuple(scope, groupAnalyzer, ((SqlCall) expandedGroupExpr).getOperandList()));
		case OTHER:
			if (expandedGroupExpr instanceof SqlNodeList && ((SqlNodeList) expandedGroupExpr).size() == 0) {
				return ImmutableBitSet.of();
			}
		}

		final int ref = SqlGroupUtil.lookupGroupExpr(groupAnalyzer, groupExpr);
		if (expandedGroupExpr instanceof SqlIdentifier) {
			// SQL 2003 does not allow expressions of column references
			SqlIdentifier expr = (SqlIdentifier) expandedGroupExpr;

			// column references should be fully qualified.
			assert expr.names.size() == 2;
			String originalRelName = expr.names.get(0);
			String originalFieldName = expr.names.get(1);

			final SqlNameMatcher nameMatcher = scope.getValidator().getCatalogReader().nameMatcher();
			final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
			scope.resolve(ImmutableList.of(originalRelName), nameMatcher, false, resolved);

			assert resolved.count() == 1;
			final SqlValidatorScope.Resolve resolve = resolved.only();
			final RelDataType rowType = resolve.rowType();
			final int childNamespaceIndex = resolve.path.steps().get(0).i;

			int namespaceOffset = 0;

			if (childNamespaceIndex > 0) {
				// If not the first child, need to figure out the width of
				// output types from all the preceding namespaces
				final SqlValidatorScope ancestorScope = resolve.scope;
				assert ancestorScope instanceof ListScope;
				List<SqlValidatorNamespace> children = ((ListScope) ancestorScope).getChildren();

				for (int j = 0; j < childNamespaceIndex; j++) {
					namespaceOffset += children.get(j).getRowType().getFieldCount();
				}
			}

			RelDataTypeField field = nameMatcher.field(rowType, originalFieldName);
			int origPos = namespaceOffset + field.getIndex();

			groupAnalyzer.groupExprProjection.put(origPos, ref);
		}

		return ImmutableBitSet.of(ref);
	}
	
	  /** Analyzes a tuple in a GROUPING SETS clause.
	   *
	   * <p>For example, in {@code GROUP BY GROUPING SETS ((a, b), a, c)},
	   * {@code (a, b)} is a tuple.
	   *
	   * <p>Gathers into {@code groupExprs} the set of distinct expressions being
	   * grouped, and returns a bitmap indicating which expressions this tuple
	   * is grouping. */
	  public static List<ImmutableBitSet> analyzeGroupTuple(SqlValidatorScope scope,
	       GroupAnalyzer groupAnalyzer, List<SqlNode> operandList) {
	    List<ImmutableBitSet> list = new ArrayList<>();
	    for (SqlNode operand : operandList) {
	      list.add(
	          SqlGroupUtil.analyzeGroupExpr(scope, groupAnalyzer, operand));
	    }
	    return list;
	  }
	  
	  
	  /** Analyzes an expression in a GROUP BY clause.
	   *
	   * <p>It may be an expression, an empty list (), or a call to
	   * {@code GROUPING SETS}, {@code CUBE}, {@code ROLLUP},
	   * {@code TUMBLE}, {@code HOP} or {@code SESSION}.
	   *
	   * <p>Each group item produces a list of group sets, which are written to
	   * {@code topBuilder}. To find the grouping sets of the query, we will take
	   * the cartesian product of the group sets. */
	  public static void analyzeGroupItem(SqlValidatorScope scope,
	      GroupAnalyzer groupAnalyzer,
	      ImmutableList.Builder<ImmutableList<ImmutableBitSet>> topBuilder,
	      SqlNode groupExpr) {
	    final ImmutableList.Builder<ImmutableBitSet> builder;
	    switch (groupExpr.getKind()) {
	    case CUBE:
	    case ROLLUP:
	      // E.g. ROLLUP(a, (b, c)) becomes [{0}, {1, 2}]
	      // then we roll up to [(0, 1, 2), (0), ()]  -- note no (0, 1)
	      List<ImmutableBitSet> bitSets =
	          analyzeGroupTuple(scope, groupAnalyzer,
	              ((SqlCall) groupExpr).getOperandList());
	      switch (groupExpr.getKind()) {
	      case ROLLUP:
	        topBuilder.add(rollup(bitSets));
	        return;
	      default:
	        topBuilder.add(cube(bitSets));
	        return;
	      }
	    case OTHER:
	      if (groupExpr instanceof SqlNodeList) {
	        SqlNodeList list = (SqlNodeList) groupExpr;
	        for (SqlNode node : list) {
	          analyzeGroupItem(scope, groupAnalyzer, topBuilder,
	              node);
	        }
	        return;
	      }
	      // fall through
	    case HOP:
	    case TUMBLE:
	    case SESSION:
	    case GROUPING_SETS:
	    default:
	      builder = ImmutableList.builder();
	      convertGroupSet(scope, groupAnalyzer, builder,
	          groupExpr);
	      topBuilder.add(builder.build());
	    }
	  }
	  /** Analyzes a GROUPING SETS item in a GROUP BY clause. */
	  private static void convertGroupSet(SqlValidatorScope scope,
	      GroupAnalyzer groupAnalyzer,
	      ImmutableList.Builder<ImmutableBitSet> builder, SqlNode groupExpr) {
	    switch (groupExpr.getKind()) {
	    case GROUPING_SETS:
	      final SqlCall call = (SqlCall) groupExpr;
	      for (SqlNode node : call.getOperandList()) {
	        convertGroupSet(scope, groupAnalyzer, builder, node);
	      }
	      return;
	    case ROW:
	      final List<ImmutableBitSet> bitSets =
	          analyzeGroupTuple(scope, groupAnalyzer,
	              ((SqlCall) groupExpr).getOperandList());
	      builder.add(ImmutableBitSet.union(bitSets));
	      return;
	    case ROLLUP:
	    case CUBE: {
	      // GROUPING SETS ( (a), ROLLUP(c,b), CUBE(d,e) )
	      // is EQUIVALENT to
	      // GROUPING SETS ( (a), (c,b), (b) ,(), (d,e), (d), (e) ).
	      // Expand all ROLLUP/CUBE nodes
	      List<ImmutableBitSet> operandBitSet =
	          analyzeGroupTuple(scope, groupAnalyzer,
	              ((SqlCall) groupExpr).getOperandList());
	      switch (groupExpr.getKind()) {
	      case ROLLUP:
	        builder.addAll(rollup(operandBitSet));
	        return;
	      default:
	        builder.addAll(cube(operandBitSet));
	        return;
	      }
	    }
	    default:
	      builder.add(
	         SqlGroupUtil.analyzeGroupExpr(scope, groupAnalyzer, groupExpr));
	      return;
	    }
	  }
	  /** Computes the rollup of bit sets.
	   *
	   * <p>For example, <code>rollup({0}, {1})</code>
	   * returns <code>({0, 1}, {0}, {})</code>.
	   *
	   * <p>Bit sets are not necessarily singletons:
	   * <code>rollup({0, 2}, {3, 5})</code>
	   * returns <code>({0, 2, 3, 5}, {0, 2}, {})</code>. */
	  @VisibleForTesting
	  public static ImmutableList<ImmutableBitSet> rollup(
	      List<ImmutableBitSet> bitSets) {
	    Set<ImmutableBitSet> builder = new LinkedHashSet<>();
	    for (;;) {
	      final ImmutableBitSet union = ImmutableBitSet.union(bitSets);
	      builder.add(union);
	      if (union.isEmpty()) {
	        break;
	      }
	      bitSets = bitSets.subList(0, bitSets.size() - 1);
	    }
	    return ImmutableList.copyOf(builder);
	  }
	  
	  
	  /** Computes the cube of bit sets.
	   *
	   * <p>For example,  <code>rollup({0}, {1})</code>
	   * returns <code>({0, 1}, {0}, {})</code>.
	   *
	   * <p>Bit sets are not necessarily singletons:
	   * <code>rollup({0, 2}, {3, 5})</code>
	   * returns <code>({0, 2, 3, 5}, {0, 2}, {})</code>. */
	  @VisibleForTesting
	  public static ImmutableList<ImmutableBitSet> cube(
	      List<ImmutableBitSet> bitSets) {
	    // Given the bit sets [{1}, {2, 3}, {5}],
	    // form the lists [[{1}, {}], [{2, 3}, {}], [{5}, {}]].
	    final Set<List<ImmutableBitSet>> builder = new LinkedHashSet<>();
	    for (ImmutableBitSet bitSet : bitSets) {
	      builder.add(Arrays.asList(bitSet, ImmutableBitSet.of()));
	    }
	    Set<ImmutableBitSet> flattenedBitSets = new LinkedHashSet<>();
	    for (List<ImmutableBitSet> o : Linq4j.product(builder)) {
	      flattenedBitSets.add(ImmutableBitSet.union(o));
	    }
	    return ImmutableList.copyOf(flattenedBitSets);
	  }
}
