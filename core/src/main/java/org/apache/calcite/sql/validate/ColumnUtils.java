package org.apache.calcite.sql.validate;

import static org.apache.calcite.util.Static.RESOURCE;

import java.util.List;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ColumnUtils {
	  /**
	   * Gets a list of extended columns with field indices to the underlying table.
	   */
	  public static List<RelDataTypeField> getExtendedColumns(
	      RelDataTypeFactory typeFactory, SqlValidatorTable table, SqlNodeList extendedColumns) {
	    final ImmutableList.Builder<RelDataTypeField> extendedFields =
	        ImmutableList.builder();
	    final ExtensibleTable extTable = table.unwrap(ExtensibleTable.class);
	    int extendedFieldOffset =
	        extTable == null
	            ? table.getRowType().getFieldCount()
	            : extTable.getExtendedColumnOffset();
	    for (final Pair<SqlIdentifier, SqlDataTypeSpec> pair : pairs(extendedColumns)) {
	      final SqlIdentifier identifier = pair.left;
	      final SqlDataTypeSpec type = pair.right;
	      extendedFields.add(
	          new RelDataTypeFieldImpl(identifier.toString(),
	              extendedFieldOffset++,
	              type.deriveType(typeFactory)));
	    }
	    return extendedFields.build();
	  }
	  
	  /** Converts a list of extended columns
	   * (of the form [name0, type0, name1, type1, ...])
	   * into a list of (name, type) pairs. */
	  private static List<Pair<SqlIdentifier, SqlDataTypeSpec>> pairs(
	      SqlNodeList extendedColumns) {
	    final List list = extendedColumns.getList();
	    //noinspection unchecked
	    return Util.pairs(list);
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
}
