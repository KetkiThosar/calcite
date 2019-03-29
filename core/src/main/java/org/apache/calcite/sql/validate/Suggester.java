package org.apache.calcite.sql.validate;

import org.apache.calcite.util.Util;

public interface Suggester {
	 /** Suggests candidates for unique names, given the number of attempts so far
	   * and the number of expressions in the project list. */
	    String apply(String original, int attempt, int size);

	  public static final Suggester EXPR_SUGGESTER =
	      (original, attempt, size) -> Util.first(original, "EXPR$") + attempt;

	  public static final Suggester F_SUGGESTER =
	      (original, attempt, size) -> Util.first(original, "$f")
	          + Math.max(size, attempt);

	  public static final Suggester ATTEMPT_SUGGESTER =
	      new Suggester() {
	        public String apply(String original, int attempt, int size) {
	          return Util.first(original, "$") + attempt;
	        }
	      };

}
