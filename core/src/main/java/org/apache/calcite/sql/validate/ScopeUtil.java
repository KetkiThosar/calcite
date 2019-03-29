package org.apache.calcite.sql.validate;

import java.util.List;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import com.google.common.collect.ImmutableList;

public class ScopeUtil {
	  public static SelectScope getEnclosingSelectScope(SqlValidatorScope scope) {
		    while (scope instanceof DelegatingScope) {
		      if (scope instanceof SelectScope) {
		        return (SelectScope) scope;
		      }
		      scope = ((DelegatingScope) scope).getParent();
		    }
		    return null;
		  }
	  
	  
	  public static AggregatingSelectScope getEnclosingAggregateSelectScope(
		      SqlValidatorScope scope) {
		    while (scope instanceof DelegatingScope) {
		      if (scope instanceof AggregatingSelectScope) {
		        return (AggregatingSelectScope) scope;
		      }
		      scope = ((DelegatingScope) scope).getParent();
		    }
		    return null;
		  }
	  
	  
	  private static List<SqlValidatorNamespace> children(SqlValidatorScope scope) {
		    return scope instanceof ListScope
		        ? ((ListScope) scope).getChildren()
		        : ImmutableList.of();
		  }

		  /**
		   * Returns whether any of the given expressions are sorted.
		   *
		   * <p>If so, it can be the default ORDER BY clause for a WINDOW specification.
		   * (This is an extension to the SQL standard for streaming.)
		   */
		 public static boolean containsMonotonic(SelectScope scope, SqlNodeList nodes) {
		    for (SqlNode node : nodes) {
		      if (!scope.getMonotonicity(node).mayRepeat()) {
		        return true;
		      }
		    }
		    return false;
		  }
		 
		 
		 /**
		   * Returns whether there are any input columns that are sorted.
		   *
		   * <p>If so, it can be the default ORDER BY clause for a WINDOW specification.
		   * (This is an extension to the SQL standard for streaming.)
		   */
		  public static boolean containsMonotonic(SqlValidatorScope scope) {
		    for (SqlValidatorNamespace ns : children(scope)) {
		      ns = ns.resolve();
		      for (String field : ns.getRowType().getFieldNames()) {
		        if (!ns.getMonotonicity(field).mayRepeat()) {
		          return true;
		        }
		      }
		    }
		    return false;
		  }

	  
}
