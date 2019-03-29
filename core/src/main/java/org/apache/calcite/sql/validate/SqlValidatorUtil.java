/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptSchemaWithSampling;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.schema.CustomColumnResolvingTable;
import org.apache.calcite.schema.ExtensibleTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Utility methods related to validation.
 */
public class SqlValidatorUtil {
  private SqlValidatorUtil() {}

  //~ Methods ----------------------------------------------------------------

  /**
   * Converts a {@link SqlValidatorScope} into a {@link RelOptTable}. This is
   * only possible if the scope represents an identifier, such as "sales.emp".
   * Otherwise, returns null.
   *
   * @param namespace     Namespace
   * @param catalogReader Schema
   * @param datasetName   Name of sample dataset to substitute, or null to use
   *                      the regular table
   * @param usedDataset   Output parameter which is set to true if a sample
   *                      dataset is found; may be null
   */
  public static RelOptTable getRelOptTable(
      SqlValidatorNamespace namespace,
      Prepare.CatalogReader catalogReader,
      String datasetName,
      boolean[] usedDataset) {
    if (namespace.isWrapperFor(TableNamespace.class)) {
      final TableNamespace tableNamespace =
          namespace.unwrap(TableNamespace.class);
      return getRelOptTable(tableNamespace, catalogReader, datasetName, usedDataset,
          tableNamespace.extendedFields);
    } else if (namespace.isWrapperFor(SqlValidatorImpl.DmlNamespace.class)) {
      final SqlValidatorImpl.DmlNamespace dmlNamespace = namespace.unwrap(
          SqlValidatorImpl.DmlNamespace.class);
      final SqlValidatorNamespace resolvedNamespace = dmlNamespace.resolve();
      if (resolvedNamespace.isWrapperFor(TableNamespace.class)) {
        final TableNamespace tableNamespace = resolvedNamespace.unwrap(TableNamespace.class);
        final SqlValidatorTable validatorTable = tableNamespace.getTable();
        final RelDataTypeFactory typeFactory = catalogReader.getTypeFactory();
        final List<RelDataTypeField> extendedFields = dmlNamespace.extendList == null
            ? ImmutableList.of()
            : ColumnUtils.getExtendedColumns(typeFactory, validatorTable, dmlNamespace.extendList);
        return getRelOptTable(
            tableNamespace, catalogReader, datasetName, usedDataset, extendedFields);
      }
    }
    return null;
  }

  private static RelOptTable getRelOptTable(
      TableNamespace tableNamespace,
      Prepare.CatalogReader catalogReader,
      String datasetName,
      boolean[] usedDataset,
      List<RelDataTypeField> extendedFields) {
    final List<String> names = tableNamespace.getTable().getQualifiedName();
    RelOptTable table;
    if (datasetName != null
        && catalogReader instanceof RelOptSchemaWithSampling) {
      final RelOptSchemaWithSampling reader =
          (RelOptSchemaWithSampling) catalogReader;
      table = reader.getTableForMember(names, datasetName, usedDataset);
    } else {
      // Schema does not support substitution. Ignore the data set, if any.
      table = catalogReader.getTableForMember(names);
    }
    if (!extendedFields.isEmpty()) {
      table = table.extend(extendedFields);
    }
    return table;
  }


  
  public static void getSchemaObjectMonikers(
      SqlValidatorCatalogReader catalogReader,
      List<String> names,
      List<SqlMoniker> hints) {
    // Assume that the last name is 'dummy' or similar.
    List<String> subNames = Util.skipLast(names);

    // Try successively with catalog.schema, catalog and no prefix
    for (List<String> x : catalogReader.getSchemaPaths()) {
      final List<String> names2 =
          ImmutableList.<String>builder().addAll(x).addAll(subNames).build();
      hints.addAll(catalogReader.getAllSchemaObjectNames(names2));
    }
  }

  /**
   * Finds a {@link org.apache.calcite.jdbc.CalciteSchema.TypeEntry} in a
   * given schema whose type has the given name, possibly qualified.
   *
   * @param rootSchema root schema
   * @param typeName name of the type, may be qualified or fully-qualified
   *
   * @return TypeEntry with a table with the given name, or null
   */
  public static CalciteSchema.TypeEntry getTypeEntry(
      CalciteSchema rootSchema, SqlIdentifier typeName) {
    final String name;
    final List<String> path;
    if (typeName.isSimple()) {
      path = ImmutableList.of();
      name = typeName.getSimple();
    } else {
      path = Util.skipLast(typeName.names);
      name = Util.last(typeName.names);
    }
    CalciteSchema schema = rootSchema;
    for (String p : path) {
      if (schema == rootSchema
          && SqlNameMatchers.withCaseSensitive(true).matches(p, schema.getName())) {
        continue;
      }
      schema = schema.getSubSchema(p, true);
    }
    return schema == null ? null : schema.getType(name, false);
  }

  /**
   * Finds a {@link org.apache.calcite.jdbc.CalciteSchema.TableEntry} in a
   * given catalog reader whose table has the given name, possibly qualified.
   *
   * <p>Uses the case-sensitivity policy of the specified catalog reader.
   *
   * <p>If not found, returns null.
   *
   * @param catalogReader accessor to the table metadata
   * @param names Name of table, may be qualified or fully-qualified
   *
   * @return TableEntry with a table with the given name, or null
   */
  public static CalciteSchema.TableEntry getTableEntry(
      SqlValidatorCatalogReader catalogReader, List<String> names) {
    // First look in the default schema, if any.
    // If not found, look in the root schema.
    for (List<String> schemaPath : catalogReader.getSchemaPaths()) {
      CalciteSchema schema =
          getSchema(catalogReader.getRootSchema(),
              Iterables.concat(schemaPath, Util.skipLast(names)),
              catalogReader.nameMatcher());
      if (schema == null) {
        continue;
      }
      CalciteSchema.TableEntry entry =
          getTableEntryFrom(schema, Util.last(names),
              catalogReader.nameMatcher().isCaseSensitive());
      if (entry != null) {
        return entry;
      }
    }
    return null;
  }

  /**
   * Finds and returns {@link CalciteSchema} nested to the given rootSchema
   * with specified schemaPath.
   *
   * <p>Uses the case-sensitivity policy of specified nameMatcher.
   *
   * <p>If not found, returns null.
   *
   * @param rootSchema root schema
   * @param schemaPath full schema path of required schema
   * @param nameMatcher name matcher
   *
   * @return CalciteSchema that corresponds specified schemaPath
   */
  public static CalciteSchema getSchema(CalciteSchema rootSchema,
      Iterable<String> schemaPath, SqlNameMatcher nameMatcher) {
    CalciteSchema schema = rootSchema;
    for (String schemaName : schemaPath) {
      if (schema == rootSchema
          && nameMatcher.matches(schemaName, schema.getName())) {
        continue;
      }
      schema = schema.getSubSchema(schemaName,
          nameMatcher.isCaseSensitive());
      if (schema == null) {
        return null;
      }
    }
    return schema;
  }

  private static CalciteSchema.TableEntry getTableEntryFrom(
      CalciteSchema schema, String name, boolean caseSensitive) {
    CalciteSchema.TableEntry entry =
        schema.getTable(name, caseSensitive);
    if (entry == null) {
      entry = schema.getTableBasedOnNullaryFunction(name,
          caseSensitive);
    }
    return entry;
  }

  


  //~ Inner Classes ----------------------------------------------------------

  /**
   * Walks over an expression, copying every node, and fully-qualifying every
   * identifier.
   */
  @Deprecated // to be removed before 2.0
  public static class DeepCopier extends SqlScopedShuttle {
    DeepCopier(SqlValidatorScope scope) {
      super(scope);
    }

    /** Copies a list of nodes. */
    public static SqlNodeList copy(SqlValidatorScope scope, SqlNodeList list) {
      //noinspection deprecation
      return (SqlNodeList) list.accept(new DeepCopier(scope));
    }

    public SqlNode visit(SqlNodeList list) {
      SqlNodeList copy = new SqlNodeList(list.getParserPosition());
      for (SqlNode node : list) {
        copy.add(node.accept(this));
      }
      return copy;
    }

    // Override to copy all arguments regardless of whether visitor changes
    // them.
    protected SqlNode visitScoped(SqlCall call) {
      ArgHandler<SqlNode> argHandler =
          new CallCopyingArgHandler(call, true);
      call.getOperator().acceptCall(this, call, false, argHandler);
      return argHandler.result();
    }

    public SqlNode visit(SqlLiteral literal) {
      return SqlNode.clone(literal);
    }

    public SqlNode visit(SqlIdentifier id) {
      // First check for builtin functions which don't have parentheses,
      // like "LOCALTIME".
      final SqlCall call = SqlUtil.makeCall(getScope().getValidator().getOperatorTable(), id);
      if (call != null) {
        return call;
      }

      return getScope().fullyQualify(id).identifier;
    }

    public SqlNode visit(SqlDataTypeSpec type) {
      return SqlNode.clone(type);
    }

    public SqlNode visit(SqlDynamicParam param) {
      return SqlNode.clone(param);
    }

    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
      return SqlNode.clone(intervalQualifier);
    }
  }

 
  /** Builds a list of GROUP BY expressions. */
  static class GroupAnalyzer {
    /** Extra expressions, computed from the input as extra GROUP BY
     * expressions. For example, calls to the {@code TUMBLE} functions. */
    final List<SqlNode> extraExprs = new ArrayList<>();
    final List<SqlNode> groupExprs;
    final Map<Integer, Integer> groupExprProjection = new HashMap<>();
    int groupCount;

    GroupAnalyzer(List<SqlNode> groupExprs) {
      this.groupExprs = groupExprs;
    }

    SqlNode createGroupExpr() {
      // TODO: create an expression that could have no other source
      return SqlLiteral.createCharString("xyz" + groupCount++,
          SqlParserPos.ZERO);
    }
  }
}

// End SqlValidatorUtil.java
