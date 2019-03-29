package org.apache.calcite.sql.validate;

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.Suggester;
import org.apache.calcite.util.Util;

import com.google.common.collect.Lists;

public class NameUtils {
	/**
	   * Derives an alias for a node, and invents a mangled identifier if it
	   * cannot.
	   *
	   * <p>Examples:
	   *
	   * <ul>
	   * <li>Alias: "1 + 2 as foo" yields "foo"
	   * <li>Identifier: "foo.bar.baz" yields "baz"
	   * <li>Anything else yields "expr$<i>ordinal</i>"
	   * </ul>
	   *
	   * @return An alias, if one can be derived; or a synthetic alias
	   * "expr$<i>ordinal</i>" if ordinal &lt; 0; otherwise null
	   */
	  public static String getAlias(SqlNode node, int ordinal) {
	    switch (node.getKind()) {
	    case AS:
	      // E.g. "1 + 2 as foo" --> "foo"
	      return ((SqlCall) node).operand(1).toString();

	    case OVER:
	      // E.g. "bids over w" --> "bids"
	      return getAlias(((SqlCall) node).operand(0), ordinal);

	    case IDENTIFIER:
	      // E.g. "foo.bar" --> "bar"
	      return Util.last(((SqlIdentifier) node).names);

	    default:
	      if (ordinal < 0) {
	        return null;
	      } else {
	        return SqlUtil.deriveAliasFromOrdinal(ordinal);
	      }
	    }
	  }
	  /**
	   * Makes a name distinct from other names which have already been used, adds
	   * it to the list, and returns it.
	   *
	   * @param name      Suggested name, may not be unique
	   * @param usedNames  Collection of names already used
	   * @param suggester Base for name when input name is null
	   * @return Unique name
	   */
	  public static String uniquify(String name, Set<String> usedNames,
	      Suggester suggester) {
	    if (name != null) {
	      if (usedNames.add(name)) {
	        return name;
	      }
	    }
	    final String originalName = name;
	    for (int j = 0;; j++) {
	      name = suggester.apply(originalName, j, usedNames.size());
	      if (usedNames.add(name)) {
	        return name;
	      }
	    }
	  }
	  
	  /**
	   * Converts an expression "expr" into "expr AS alias".
	   */
	  public static SqlNode addAlias(
	      SqlNode expr,
	      String alias) {
	    final SqlParserPos pos = expr.getParserPosition();
	    final SqlIdentifier id = new SqlIdentifier(alias, pos);
	    return SqlStdOperatorTable.AS.createCall(pos, expr, id);
	  }
	  
	  
	  /**
	   * Makes sure that the names in a list are unique.
	   *
	   * <p>Does not modify the input list. Returns the input list if the strings
	   * are unique, otherwise allocates a new list.
	   *
	   * @param nameList List of strings
	   * @param suggester How to generate new names if duplicate names are found
	   * @param caseSensitive Whether upper and lower case names are considered
	   *     distinct
	   * @return List of unique strings
	   */
	  public static List<String> uniquify(
	      List<String> nameList,
	      Suggester suggester,
	      boolean caseSensitive) {
	    final Set<String> used = caseSensitive
	        ? new LinkedHashSet<>()
	        : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
	    int changeCount = 0;
	    final List<String> newNameList = new ArrayList<>();
	    for (String name : nameList) {
	      String uniqueName = NameUtils.uniquify(name, used, suggester);
	      if (!uniqueName.equals(name)) {
	        ++changeCount;
	      }
	      newNameList.add(uniqueName);
	    }
	    return changeCount == 0
	        ? nameList
	        : newNameList;
	  }


	   /**
	   * Checks that there are no duplicates in a list of {@link SqlIdentifier}.
	   */
	 public static void checkIdentifierListForDuplicates(List<SqlNode> columnList,
	      SqlValidatorImpl.ValidationErrorFunction validationErrorFunction) {
	    final List<List<String>> names = Lists.transform(columnList,
	        o -> ((SqlIdentifier) o).names);
	    final int i = Util.firstDuplicate(names);
	    if (i >= 0) {
	      throw validationErrorFunction.apply(columnList.get(i),
	          RESOURCE.duplicateNameInColumnList(Util.last(names.get(i))));
	    }
	  }
	 /**
	   * Makes sure that the names in a list are unique.
	   *
	   * <p>Does not modify the input list. Returns the input list if the strings
	   * are unique, otherwise allocates a new list.
	   *
	   * @param nameList List of strings
	   * @param caseSensitive Whether upper and lower case names are considered
	   *     distinct
	   * @return List of unique strings
	   */
	  public static List<String> uniquify(List<String> nameList,
	      boolean caseSensitive) {
	    return uniquify(nameList, Suggester.EXPR_SUGGESTER, caseSensitive);
	  }

	  
}
