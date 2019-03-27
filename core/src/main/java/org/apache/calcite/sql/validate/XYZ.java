package org.apache.calcite.sql.validate;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class XYZ {
	  public static RelDataType createTypeFromProjection(RelDataType type,
		      List<String> columnNameList, RelDataTypeFactory typeFactory,
		      boolean caseSensitive) {
		    // If the names in columnNameList and type have case-sensitive differences,
		    // the resulting type will use those from type. These are presumably more
		    // canonical.
		    final List<RelDataTypeField> fields =
		        new ArrayList<>(columnNameList.size());
		    for (String name : columnNameList) {
		      RelDataTypeField field = type.getField(name, caseSensitive, false);
		      fields.add(type.getFieldList().get(field.getIndex()));
		    }
		    return typeFactory.createStructType(fields);
		  }
	  
	  
	  /**
	   * Derives the list of column names suitable for NATURAL JOIN. These are the
	   * columns that occur exactly once on each side of the join.
	   *
	   * @param nameMatcher Whether matches are case-sensitive
	   * @param leftRowType  Row type of left input to the join
	   * @param rightRowType Row type of right input to the join
	   * @return List of columns that occur once on each side
	   */
	  public static List<String> deriveNaturalJoinColumnList(
	      SqlNameMatcher nameMatcher,
	      RelDataType leftRowType,
	      RelDataType rightRowType) {
	    final List<String> naturalColumnNames = new ArrayList<>();
	    final List<String> leftNames = leftRowType.getFieldNames();
	    final List<String> rightNames = rightRowType.getFieldNames();
	    for (String name : leftNames) {
	      if (nameMatcher.frequency(leftNames, name) == 1
	          && nameMatcher.frequency(rightNames, name) == 1) {
	        naturalColumnNames.add(name);
	      }
	    }
	    return naturalColumnNames;
	  }
	  
	  
	  
	  /**
	   * Resolve a target column name in the target table.
	   *
	   * @return the target field or null if the name cannot be resolved
	   * @param rowType the target row type
	   * @param id      the target column identifier
	   * @param table   the target table or null if it is not a RelOptTable instance
	   */
	  public static RelDataTypeField getTargetField(
	      RelDataType rowType, RelDataTypeFactory typeFactory,
	      SqlIdentifier id, SqlValidatorCatalogReader catalogReader,
	      RelOptTable table) {
	    final Table t = table == null ? null : table.unwrap(Table.class);
	    if (!(t instanceof CustomColumnResolvingTable)) {
	      final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
	      return nameMatcher.field(rowType, id.getSimple());
	    }

	    final List<Pair<RelDataTypeField, List<String>>> entries =
	        ((CustomColumnResolvingTable) t).resolveColumn(
	            rowType, typeFactory, id.names);
	    switch (entries.size()) {
	    case 1:
	      if (!entries.get(0).getValue().isEmpty()) {
	        return null;
	      }
	      return entries.get(0).getKey();
	    default:
	      return null;
	    }
	  }
	  /**
	   * Derives the type of a join relational expression.
	   *
	   * @param leftType        Row type of left input to join
	   * @param rightType       Row type of right input to join
	   * @param joinType        Type of join
	   * @param typeFactory     Type factory
	   * @param fieldNameList   List of names of fields; if null, field names are
	   *                        inherited and made unique
	   * @param systemFieldList List of system fields that will be prefixed to
	   *                        output row type; typically empty but must not be
	   *                        null
	   * @return join type
	   */
	  public static RelDataType deriveJoinRowType(
	      RelDataType leftType,
	      RelDataType rightType,
	      JoinRelType joinType,
	      RelDataTypeFactory typeFactory,
	      List<String> fieldNameList,
	      List<RelDataTypeField> systemFieldList) {
	    assert systemFieldList != null;
	    switch (joinType) {
	    case LEFT:
	      rightType = typeFactory.createTypeWithNullability(rightType, true);
	      break;
	    case RIGHT:
	      leftType = typeFactory.createTypeWithNullability(leftType, true);
	      break;
	    case FULL:
	      leftType = typeFactory.createTypeWithNullability(leftType, true);
	      rightType = typeFactory.createTypeWithNullability(rightType, true);
	      break;
	    default:
	      break;
	    }
	    return createJoinType(typeFactory, leftType, rightType, fieldNameList,
	        systemFieldList);
	  }  
	  /**
	   * Returns the type the row which results when two relations are joined.
	   *
	   * <p>The resulting row type consists of
	   * the system fields (if any), followed by
	   * the fields of the left type, followed by
	   * the fields of the right type. The field name list, if present, overrides
	   * the original names of the fields.
	   *
	   * @param typeFactory     Type factory
	   * @param leftType        Type of left input to join
	   * @param rightType       Type of right input to join, or null for semi-join
	   * @param fieldNameList   If not null, overrides the original names of the
	   *                        fields
	   * @param systemFieldList List of system fields that will be prefixed to
	   *                        output row type; typically empty but must not be
	   *                        null
	   * @return type of row which results when two relations are joined
	   */
	  public static RelDataType createJoinType(
	      RelDataTypeFactory typeFactory,
	      RelDataType leftType,
	      RelDataType rightType,
	      List<String> fieldNameList,
	      List<RelDataTypeField> systemFieldList) {
	    assert (fieldNameList == null)
	        || (fieldNameList.size()
	        == (systemFieldList.size()
	        + leftType.getFieldCount()
	        + rightType.getFieldCount()));
	    List<String> nameList = new ArrayList<>();
	    final List<RelDataType> typeList = new ArrayList<>();

	    // Use a set to keep track of the field names; this is needed
	    // to ensure that the contains() call to check for name uniqueness
	    // runs in constant time; otherwise, if the number of fields is large,
	    // doing a contains() on a list can be expensive.
	    final Set<String> uniqueNameList =
	        typeFactory.getTypeSystem().isSchemaCaseSensitive()
	            ? new HashSet<>()
	            : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
	    addFields(systemFieldList, typeList, nameList, uniqueNameList);
	    addFields(leftType.getFieldList(), typeList, nameList, uniqueNameList);
	    if (rightType != null) {
	      addFields(
	          rightType.getFieldList(), typeList, nameList, uniqueNameList);
	    }
	    if (fieldNameList != null) {
	      assert fieldNameList.size() == nameList.size();
	      nameList = fieldNameList;
	    }
	    return typeFactory.createStructType(typeList, nameList);
	  }
	  
	  private static void addFields(List<RelDataTypeField> fieldList,
		      List<RelDataType> typeList, List<String> nameList,
		      Set<String> uniqueNames) {
		    for (RelDataTypeField field : fieldList) {
		      String name = field.getName();

		      // Ensure that name is unique from all previous field names
		      if (uniqueNames.contains(name)) {
		        String nameBase = name;
		        for (int j = 0;; j++) {
		          name = nameBase + j;
		          if (!uniqueNames.contains(name)) {
		            break;
		          }
		        }
		      }
		      nameList.add(name);
		      uniqueNames.add(name);
		      typeList.add(field.getType());
		    }
		  }
	  
	  /** Returns a map from field names to indexes. */
	  public static Map<String, Integer> mapNameToIndex(List<RelDataTypeField> fields) {
	    ImmutableMap.Builder<String, Integer> output = ImmutableMap.builder();
	    for (RelDataTypeField field : fields) {
	      output.put(field.getName(), field.getIndex());
	    }
	    return output.build();
	  }
	  @Deprecated // to be removed before 2.0
	  public static RelDataTypeField lookupField(boolean caseSensitive,
	      final RelDataType rowType, String columnName) {
	    return rowType.getField(columnName, caseSensitive, false);
	  }
	  
	  public static void checkCharsetAndCollateConsistentIfCharType(
		      RelDataType type) {
		    // (every charset must have a default collation)
		    if (SqlTypeUtil.inCharFamily(type)) {
		      Charset strCharset = type.getCharset();
		      Charset colCharset = type.getCollation().getCharset();
		      assert null != strCharset;
		      assert null != colCharset;
		      if (!strCharset.equals(colCharset)) {
		        if (false) {
		          // todo: enable this checking when we have a charset to
		          //   collation mapping
		          throw new Error(type.toString()
		              + " was found to have charset '" + strCharset.name()
		              + "' and a mismatched collation charset '"
		              + colCharset.name() + "'");
		        }
		      }
		    }
		  }
	  
	  
	  /**
	   * Gets the bit-set to the column ordinals in the source for columns that
	   * intersect in the target.
	   *
	   * @param sourceRowType The source upon which to ordinate the bit set.
	   * @param indexToField  The map of ordinals to target fields.
	   */
	  public static ImmutableBitSet getOrdinalBitSet(
	      RelDataType sourceRowType,
	      Map<Integer, RelDataTypeField> indexToField) {
	    ImmutableBitSet source = ImmutableBitSet.of(
	        Lists.transform(sourceRowType.getFieldList(),
	            RelDataTypeField::getIndex));
	    ImmutableBitSet target =
	        ImmutableBitSet.of(indexToField.keySet());
	    return source.intersect(target);
	  }
	  
	  

	  /**
	   * Gets the bit-set to the column ordinals in the source for columns that intersect in the target.
	   * @param sourceRowType The source upon which to ordinate the bit set.
	   * @param targetRowType The target to overlay on the source to create the bit set.
	   */
	  public static ImmutableBitSet getOrdinalBitSet(
	      RelDataType sourceRowType, RelDataType targetRowType) {
	    Map<Integer, RelDataTypeField> indexToField =
	        getIndexToFieldMap(sourceRowType.getFieldList(), targetRowType);
	    return getOrdinalBitSet(sourceRowType, indexToField);
	  }
	  
	  
	  /**
	   * Gets a map of indexes from the source to fields in the target for the
	   * intersecting set of source and target fields.
	   *
	   * @param sourceFields The source of column names that determine indexes
	   * @param targetFields The target fields to be indexed
	   */
	  public static ImmutableMap<Integer, RelDataTypeField> getIndexToFieldMap(
	      List<RelDataTypeField> sourceFields,
	      RelDataType targetFields) {
	    final ImmutableMap.Builder<Integer, RelDataTypeField> output =
	        ImmutableMap.builder();
	    for (final RelDataTypeField source : sourceFields) {
	      final RelDataTypeField target = targetFields.getField(source.getName(), true, false);
	      if (target != null) {
	        output.put(source.getIndex(), target);
	      }
	    }
	    return output.build();
	  }
}
