/**
 * MongoDB Aggregation Pipeline Builder
 *
 * This module provides a comprehensive solution for converting filter structures
 * into optimized MongoDB aggregation pipelines. It handles recursive dependencies
 * between arithmetic variables, list variables, switch cases, and schema fields.
 *
 * Key Features:
 * - Recursive dependency resolution for all variable types
 * - Proper pipeline staging to ensure variables are defined before use
 * - Type-aware value parsing (string vs number based on field types)
 * - Support for arithmetic expressions, list operations, and switch cases
 * - Custom blocks used 2+ times are defined in $addFields stage as boolean fields
 * - Custom blocks used once are inlined in $match stages
 * - Custom blocks with isTrue=false are matched against false, or wrapped in $nor if inlined
 */

import { latexToMongoConverter } from "./robustLatexConverter.js";

/**
 * Helper function to extract field name from various formats
 * Supports:
 * - String values (legacy): "fieldName"
 * - Object with metadata (new): { name: "fieldName", _meta: {...} }
 * - Object with type (list condition): { type: "array", field: "fieldName", name: "listName" }
 * - Old object format: { value: "fieldName" }
 *
 * @param {*} field - Field value in any supported format
 * @returns {string} Extracted field name
 */
const normalizeFieldName = (field) => {
  if (!field) return "";
  if (typeof field === "string") return field;
  if (typeof field === "object") {
    // New format with metadata
    if (field.name) return field.name;
    // Old formats
    if (field.value) return field.value;
    if (field.field) return field.field;
  }
  return String(field);
};

/**
 * Helper function to extract value from various formats
 * Handles the same formats as normalizeFieldName
 *
 * @param {*} value - Value in any supported format
 * @returns {*} Extracted value (preserves objects for list conditions)
 */
const normalizeValue = (value) => {
  // List condition objects should be preserved
  if (value && typeof value === "object" && value.type === "array") {
    return value;
  }
  // New format with metadata - extract name
  if (value && typeof value === "object" && value.name && value._meta) {
    return value.name;
  }
  // Otherwise return as-is
  return value;
};

/**
 * Main pipeline builder function
 * @param {Array} filters - Filter conditions
 * @param {Object} schema - Schema definition
 * @param {Array} fieldOptions - Available fields
 * @param {Array} customVariables - Arithmetic variables
 * @param {Array} customListVariables - List variables
 * @param {Array} customSwitchCases - Switch cases
 * @param {Array} additionalFieldsToProject - Extra fields to project
 * @returns {Array} MongoDB aggregation pipeline
 */
export const buildMongoAggregationPipeline = (
  filters,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
  additionalFieldsToProject = [],
) => {
  try {
    const pipeline = [];

    // Step 1: Analyze dependencies
    const dependencyGraph = buildDependencyGraph(
      filters,
      customVariables,
      customListVariables,
      customSwitchCases,
      fieldOptions,
    );

    // Step 2: Determine pipeline stages needed
    const stages = determineRequiredStages(dependencyGraph);

    // Step 3: Extract early match conditions (simple filters on base fields) - FIRST!
    const { earlyMatch, remainingFilters } = extractEarlyMatchConditions(
      filters,
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
    );

    // Step 3a: Insert early $match stage FIRST if we have simple conditions
    if (earlyMatch && Object.keys(earlyMatch).length > 0) {
      pipeline.push({ $match: earlyMatch });
    }

    // Step 4: Build initial project stage for base variables
    if (stages.needsInitialProject) {
      const initialProject = buildInitialProjectStage(
        dependencyGraph,
        customVariables,
        customListVariables,
        customSwitchCases,
        fieldOptions,
      );
      if (Object.keys(initialProject.$project).length > 1) {
        // More than just _id
        pipeline.push(initialProject);
      }
    }

    // Step 5: Build stages for variables in dependency order (by level)
    const variableStages = buildVariableStagesByLevel(
      dependencyGraph,
      customVariables,
      customListVariables,
      customSwitchCases,
      fieldOptions,
    );
    pipeline.push(...variableStages);

    // Step 6: Build custom block definitions (only for blocks used 2+ times)
    const customBlockStage = buildCustomBlockStage(
      dependencyGraph,
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
    );
    if (customBlockStage) {
      pipeline.push(customBlockStage);
    }

    // Step 7: Build match stages for remaining filters (lookup-dependent)
    const matchStages = buildMatchStages(
      remainingFilters,
      dependencyGraph,
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
    );
    pipeline.push(...matchStages);

    // Step 7: Build final project stage
    const finalProject = buildFinalProjectStage(
      dependencyGraph,
      additionalFieldsToProject,
      fieldOptions,
    );
    if (finalProject) {
      pipeline.push(finalProject);
    }

    return pipeline;
  } catch (error) {
    return [];
  }
};

/**
 * Builds a comprehensive dependency graph for all variables
 */
const buildDependencyGraph = (
  filters,
  customVariables,
  customListVariables,
  customSwitchCases,
  fieldOptions,
) => {
  const graph = {
    variables: new Map(), // variable name -> dependencies
    reverseDeps: new Map(), // variable name -> variables that depend on it
    levels: new Map(), // variable name -> dependency level
    usedFields: {
      baseFields: new Set(),
      customVariables: new Set(),
      listVariables: new Set(),
      switchCases: new Set(),
    },
    customBlockUsage: new Map(), // custom block name -> { count, block }
    listVariableUsage: new Map(), // list variable name -> { count, variable, mustMaterialize }
  };

  // Initialize all variables in graph
  [...customVariables, ...customListVariables, ...customSwitchCases].forEach(
    (varDef) => {
      graph.variables.set(varDef.name, new Set());
      graph.reverseDeps.set(varDef.name, new Set());
    },
  );

  // Analyze filters to find used fields
  analyzeFiltersForUsage(
    filters,
    graph,
    customVariables,
    customListVariables,
    customSwitchCases,
    fieldOptions,
  );

  // Build dependencies for each variable type
  buildArithmeticDependencies(
    customVariables,
    graph,
    customListVariables,
    customSwitchCases,
    fieldOptions,
  );
  buildListDependencies(
    customListVariables,
    graph,
    customVariables,
    customSwitchCases,
    fieldOptions,
  );
  buildSwitchDependencies(
    customSwitchCases,
    graph,
    customVariables,
    customListVariables,
    fieldOptions,
  );

  // Recursively mark dependencies as used
  markDependenciesAsUsed(
    graph,
    customVariables,
    customListVariables,
    customSwitchCases,
  );

  // Calculate dependency levels using topological sort
  calculateDependencyLevels(graph);

  return graph;
};

/**
 * Recursively marks dependencies of used variables as used
 */
const markDependenciesAsUsed = (
  graph,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  const visited = new Set();

  const markDeps = (varName) => {
    if (visited.has(varName)) return;
    visited.add(varName);

    const deps = graph.variables.get(varName) || new Set();
    for (const dep of deps) {
      // If it's a variable (not a schema field), mark it as used in the appropriate category
      if (graph.variables.has(dep)) {
        if (customVariables.some((v) => v.name === dep)) {
          graph.usedFields.customVariables.add(dep);
        } else if (customListVariables.some((lv) => lv.name === dep)) {
          graph.usedFields.listVariables.add(dep);
          // If this is a list variable dependency, ensure it's tracked for usage
          const listVar = customListVariables.find((lv) => lv.name === dep);
          if (listVar) {
            const current = graph.listVariableUsage.get(dep) || {
              count: 0,
              variable: listVar,
              mustMaterialize: false,
            };
            // Mark as must materialize because it's a dependency
            graph.listVariableUsage.set(dep, {
              count: Math.max(current.count, 1),
              variable: listVar,
              mustMaterialize: true,
            });
          }
        } else if (customSwitchCases.some((v) => v.name === dep)) {
          graph.usedFields.switchCases.add(dep);
        }
        markDeps(dep); // Recursively mark dependencies
      } else {
        // It's a schema field
        graph.usedFields.baseFields.add(dep);
      }
    }
  };

  // Mark dependencies for all initially used variables
  graph.usedFields.customVariables.forEach(markDeps);
  graph.usedFields.listVariables.forEach(markDeps);
  graph.usedFields.switchCases.forEach(markDeps);
};

/**
 * Analyzes filters to determine which fields/variables are used
 */
const analyzeFiltersForUsage = (
  filters,
  graph,
  customVariables,
  customListVariables,
  customSwitchCases,
  fieldOptions,
) => {
  const analyzeBlock = (block, parentCustomBlockName = null) => {
    if (!block) return;

    // Track custom block usage for optimization
    // Only count blocks that are actual block types (not conditions inside the block)
    // AND are not the same custom block as the parent (those are part of the parent block's definition)
    if (
      block.customBlockName &&
      (block.type === "block" || block.category === "block")
    ) {
      // Only count if this is a different custom block than the parent
      // (same customBlockName means it's part of the parent's internal structure)
      if (block.customBlockName !== parentCustomBlockName) {
        const blockName = block.customBlockName;
        const current = graph.customBlockUsage.get(blockName) || {
          count: 0,
          block: block,
        };
        graph.customBlockUsage.set(blockName, {
          count: current.count + 1,
          block: block,
        });

        // Analyze children, passing this block's customBlockName so children with the same name are skipped
        if (block.children) {
          block.children.forEach((child) =>
            analyzeBlock(child, block.customBlockName),
          );
        }
      } else {
        // Still analyze children for field/variable dependencies
        if (block.children) {
          block.children.forEach((child) =>
            analyzeBlock(child, parentCustomBlockName),
          );
        }
      }
      return;
    }

    // For non-custom blocks, analyze children and propagate parent's customBlockName
    if (block.children) {
      block.children.forEach((child) =>
        analyzeBlock(child, parentCustomBlockName),
      );
    }

    if (block.field) {
      // Normalize field to handle both string and object formats
      const fieldName = normalizeFieldName(block.field);
      const fieldType = block.fieldType;

      // If fieldType is explicitly set, use it
      if (fieldType) {
        switch (fieldType) {
          case "schema":
            graph.usedFields.baseFields.add(fieldName);
            break;
          case "variable":
            graph.usedFields.customVariables.add(fieldName);
            break;
          case "listVariable":
            graph.usedFields.listVariables.add(fieldName);
            break;
          case "switchCase":
            graph.usedFields.switchCases.add(fieldName);
            break;
        }
      }
      // Check if field has metadata (new format: {name: "...", _meta: {...}})
      // Use metadata to determine exact field type when available (solves name collision issues)
      else if (
        block.field &&
        typeof block.field === "object" &&
        block.field._meta
      ) {
        const meta = block.field._meta;

        // Route based on metadata flags
        if (meta.isSwitchCase) {
          graph.usedFields.switchCases.add(fieldName);
        } else if (meta.isListVariable) {
          graph.usedFields.listVariables.add(fieldName);
          // Track list variable usage count
          const listVar = customListVariables.find((v) => v.name === fieldName);
          if (listVar) {
            const current = graph.listVariableUsage.get(fieldName) || {
              count: 0,
              variable: listVar,
              mustMaterialize: false,
            };
            graph.listVariableUsage.set(fieldName, {
              count: current.count + 1,
              variable: listVar,
              mustMaterialize: current.mustMaterialize,
            });
          }
        } else if (meta.isVariable) {
          graph.usedFields.customVariables.add(fieldName);
        } else if (meta.isSchemaField) {
          graph.usedFields.baseFields.add(fieldName);
        }
        // If no metadata flags match, fall through to legacy resolution below
        else {
          // Fallback: check what type it actually is
          if (
            fieldOptions.some(
              (f) => f.value === fieldName || f.label === fieldName,
            )
          ) {
            graph.usedFields.baseFields.add(fieldName);
          } else if (customVariables.some((v) => v.name === fieldName)) {
            graph.usedFields.customVariables.add(fieldName);
          } else if (customListVariables.some((v) => v.name === fieldName)) {
            graph.usedFields.listVariables.add(fieldName);
            // Track list variable usage count
            const listVar = customListVariables.find(
              (v) => v.name === fieldName,
            );
            if (listVar) {
              const current = graph.listVariableUsage.get(fieldName) || {
                count: 0,
                variable: listVar,
                mustMaterialize: false,
              };
              graph.listVariableUsage.set(fieldName, {
                count: current.count + 1,
                variable: listVar,
                mustMaterialize: current.mustMaterialize,
              });
            }
          } else if (customSwitchCases.some((v) => v.name === fieldName)) {
            graph.usedFields.switchCases.add(fieldName);
          }
        }
      }
      // Fallback to legacy precedence-based resolution if no fieldType and no metadata
      else {
        // Check if it's a schema field first (prefer over variables)
        if (
          fieldOptions.some(
            (f) => f.value === fieldName || f.label === fieldName,
          )
        ) {
          graph.usedFields.baseFields.add(fieldName);
        }
        // Check if it's a custom variable
        else if (customVariables.some((v) => v.name === fieldName)) {
          graph.usedFields.customVariables.add(fieldName);
        }
        // Check if it's a list variable
        else if (customListVariables.some((v) => v.name === fieldName)) {
          graph.usedFields.listVariables.add(fieldName);
          // Track list variable usage count
          const listVar = customListVariables.find((v) => v.name === fieldName);
          if (listVar) {
            const current = graph.listVariableUsage.get(fieldName) || {
              count: 0,
              variable: listVar,
              mustMaterialize: false,
            };
            graph.listVariableUsage.set(fieldName, {
              count: current.count + 1,
              variable: listVar,
              mustMaterialize: current.mustMaterialize,
            });
          }
        }
        // Check if it's a switch case
        else if (customSwitchCases.some((v) => v.name === fieldName)) {
          graph.usedFields.switchCases.add(fieldName);
        }
      }
    }

    // Check values for field references
    if (block.value) {
      // Check if value has metadata (new format: {name: "...", _meta: {...}})
      if (
        block.value &&
        typeof block.value === "object" &&
        block.value._meta &&
        block.value.name
      ) {
        const valueName = block.value.name;
        const meta = block.value._meta;

        // Route based on metadata flags
        if (meta.isSwitchCase) {
          graph.usedFields.switchCases.add(valueName);
        } else if (meta.isListVariable) {
          graph.usedFields.listVariables.add(valueName);
          // Track list variable usage count
          const listVar = customListVariables.find((v) => v.name === valueName);
          if (listVar) {
            const current = graph.listVariableUsage.get(valueName) || {
              count: 0,
              variable: listVar,
              mustMaterialize: false,
            };
            graph.listVariableUsage.set(valueName, {
              count: current.count + 1,
              variable: listVar,
              mustMaterialize: current.mustMaterialize,
            });
          }
        } else if (meta.isVariable) {
          graph.usedFields.customVariables.add(valueName);
        } else if (meta.isSchemaField) {
          graph.usedFields.baseFields.add(valueName);
        }
      }
      // Fallback: normalize and check by string name
      else {
        const normalizedVal = normalizeValue(block.value);
        if (typeof normalizedVal === "string") {
          const value = normalizedVal;
          // Check schema fields first
          if (
            fieldOptions.some((f) => f.value === value || f.label === value)
          ) {
            graph.usedFields.baseFields.add(value);
          }
          // Then check variables
          else if (customVariables.some((v) => v.name === value)) {
            graph.usedFields.customVariables.add(value);
          } else if (customListVariables.some((v) => v.name === value)) {
            graph.usedFields.listVariables.add(value);
            // Track list variable usage count
            const listVar = customListVariables.find((v) => v.name === value);
            if (listVar) {
              const current = graph.listVariableUsage.get(value) || {
                count: 0,
                variable: listVar,
                mustMaterialize: false,
              };
              graph.listVariableUsage.set(value, {
                count: current.count + 1,
                variable: listVar,
                mustMaterialize: current.mustMaterialize,
              });
            }
          } else if (customSwitchCases.some((v) => v.name === value)) {
            graph.usedFields.switchCases.add(value);
          }
        }
      }
    }

    // Scan for variable references in raw MongoDB expressions
    // This handles cases where filters contain operators like $filter, $map with inline expressions
    // that reference variables directly (e.g., "$jd_min_prv" in a $subtract expression)
    if (block.operator && block.value && typeof block.value === "object") {
      const allVarNames = [];
      const findVarRefs = (obj) => {
        if (typeof obj === "string" && obj.startsWith("$")) {
          const varName = obj.substring(1);
          // Filter out MongoDB operators and array element references ($$this, $$ROOT, etc.)
          if (!varName.startsWith("$") && !varName.includes(".")) {
            allVarNames.push(varName);
          }
        } else if (typeof obj === "object" && obj !== null) {
          Object.values(obj).forEach(findVarRefs);
        }
      };
      findVarRefs(block.value);

      allVarNames.forEach((varName) => {
        if (customVariables.some((v) => v.name === varName)) {
          graph.usedFields.customVariables.add(varName);
        } else if (customListVariables.some((v) => v.name === varName)) {
          graph.usedFields.listVariables.add(varName);
          const listVar = customListVariables.find((v) => v.name === varName);
          if (listVar) {
            const current = graph.listVariableUsage.get(varName) || {
              count: 0,
              variable: listVar,
              mustMaterialize: false,
            };
            graph.listVariableUsage.set(varName, {
              count: current.count + 1,
              variable: listVar,
              mustMaterialize: true, // Field reference in MongoDB expression requires materialization
            });
          }
        } else if (customSwitchCases.some((v) => v.name === varName)) {
          graph.usedFields.switchCases.add(varName);
        } else if (
          fieldOptions.some((f) => f.value === varName || f.label === varName)
        ) {
          graph.usedFields.baseFields.add(varName);
        }
      });
    }
  };

  filters.forEach(analyzeBlock);
};

/**
 * Builds dependencies for arithmetic variables
 */
const buildArithmeticDependencies = (
  customVariables,
  graph,
  customListVariables,
  customSwitchCases,
  fieldOptions,
) => {
  customVariables.forEach((varDef) => {
    const deps = new Set();

    // Parse the arithmetic expression to find dependencies
    if (varDef.variable && varDef.variable.includes("=")) {
      const expression = varDef.variable.split("=")[1].trim();

      // Check for variable references in the expression
      [
        ...customVariables,
        ...customListVariables,
        ...customSwitchCases,
      ].forEach((otherVar) => {
        if (otherVar.name !== varDef.name) {
          // Use word boundary regex to avoid substring matches
          const regex = new RegExp(`\\b${otherVar.name}\\b`, "g");
          if (regex.test(expression)) {
            deps.add(otherVar.name);
          }
        }
      });

      // Check for schema field references using the LaTeX converter's field extraction
      // This properly handles fields inside complex expressions, functions, and operators
      const extractedFields =
        latexToMongoConverter.extractFieldDependencies(expression);

      extractedFields.forEach((extractedField) => {
        // Verify this is actually a schema field (not a variable or function name)
        const isSchemaField = fieldOptions.some(
          (f) => f.value === extractedField || f.label === extractedField,
        );
        const isVariable = [
          ...customVariables,
          ...customListVariables,
          ...customSwitchCases,
        ].some((v) => v.name === extractedField);

        // Only add if it's a schema field and not already added as a variable
        if (isSchemaField && !isVariable) {
          deps.add(extractedField);
          // IMPORTANT: Also mark this field as used so it gets projected
          // This ensures the field is available for the arithmetic calculation
          graph.usedFields.baseFields.add(extractedField);
        }
      });
    }

    graph.variables.set(varDef.name, deps);
    deps.forEach((dep) => {
      if (graph.reverseDeps.has(dep)) {
        graph.reverseDeps.get(dep).add(varDef.name);
      }
    });
  });
};

/**
 * Builds dependencies for list variables
 */
const buildListDependencies = (
  customListVariables,
  graph,
  customVariables,
  customSwitchCases,
  fieldOptions,
) => {
  customListVariables.forEach((varDef) => {
    const deps = new Set();

    if (varDef.listCondition) {
      const condition = varDef.listCondition;

      // Normalize field to handle both legacy string format and new object format with metadata
      const normalizedField = normalizeFieldName(condition.field);

      // IMPORTANT: Add the array field itself as a dependency
      // This is the actual data source that needs to be projected (e.g., cross_matches.NED_BetaV3, prv_candidates)
      if (normalizedField) {
        deps.add(normalizedField);
      }

      // Check field reference in condition.field (used in filter/map conditions)
      if (normalizedField) {
        if (
          fieldOptions.some(
            (f) => f.value === normalizedField || f.label === normalizedField,
          )
        ) {
          // Already added above
        } else {
          // Check if it's a variable name
          const allVars = [
            ...customVariables,
            ...customListVariables,
            ...customSwitchCases,
          ];
          if (allVars.some((v) => v.name === normalizedField)) {
            deps.add(normalizedField);
          }
        }
      }

      // Check for dependencies in map expressions
      if (condition.operator === "$map") {
        const normalizedValue = normalizeValue(condition.value);
        const mapExpr =
          normalizedValue?.mapExpression ||
          condition.mapExpression ||
          normalizedValue;
        if (mapExpr) {
          const allVarNames = [];
          const findVarRefs = (obj) => {
            if (typeof obj === "string" && obj.startsWith("$")) {
              allVarNames.push(obj.substring(1));
            } else if (typeof obj === "object" && obj !== null) {
              Object.values(obj).forEach(findVarRefs);
            }
          };
          findVarRefs(mapExpr);
          allVarNames.forEach((varName) => {
            if (customVariables.some((v) => v.name === varName)) {
              deps.add(varName);
            }
            if (
              varName !== varDef.name &&
              customListVariables.some((v) => v.name === varName)
            ) {
              deps.add(varName);
            }
            if (
              varName !== varDef.name &&
              customSwitchCases.some((v) => v.name === varName)
            ) {
              deps.add(varName);
            }
          });
        }
      }

      // Check for dependencies in filter conditions
      if (condition.operator === "$filter" && condition.value) {
        // If the filter condition has a children structure (block format), analyze it
        if (condition.value.children) {
          analyzeBlockForDeps(
            { children: condition.value.children },
            deps,
            customVariables,
            customListVariables,
            customSwitchCases,
            fieldOptions,
            varDef.name,
          );
        }

        // Also scan for direct MongoDB expression references (e.g., raw $gt, $subtract expressions)
        // This is needed when filter conditions contain raw MongoDB syntax with variable references
        const allVarNames = [];
        const findVarRefs = (obj) => {
          if (typeof obj === "string" && obj.startsWith("$")) {
            allVarNames.push(obj.substring(1));
          } else if (typeof obj === "object" && obj !== null) {
            Object.values(obj).forEach(findVarRefs);
          }
        };
        findVarRefs(condition.value);
        allVarNames.forEach((varName) => {
          if (customVariables.some((v) => v.name === varName)) {
            deps.add(varName);
          }
          if (
            varName !== varDef.name &&
            customListVariables.some((v) => v.name === varName)
          ) {
            deps.add(varName);
          }
          if (
            varName !== varDef.name &&
            customSwitchCases.some((v) => v.name === varName)
          ) {
            deps.add(varName);
          }
        });
      }

      // Check for dependencies in $anyElementTrue and $allElementsTrue conditions
      if (
        (condition.operator === "$anyElementTrue" ||
          condition.operator === "$allElementTrue") &&
        condition.value
      ) {
        // If the condition has a children structure (block format), analyze it
        if (condition.value.children) {
          analyzeBlockForDeps(
            { children: condition.value.children },
            deps,
            customVariables,
            customListVariables,
            customSwitchCases,
            fieldOptions,
            varDef.name,
          );
        }

        // Also scan for direct MongoDB expression references (e.g., raw $gt, $subtract expressions)
        const allVarNames = [];
        const findVarRefs = (obj) => {
          if (typeof obj === "string" && obj.startsWith("$")) {
            allVarNames.push(obj.substring(1));
          } else if (typeof obj === "object" && obj !== null) {
            Object.values(obj).forEach(findVarRefs);
          }
        };
        findVarRefs(condition.value);
        allVarNames.forEach((varName) => {
          if (customVariables.some((v) => v.name === varName)) {
            deps.add(varName);
          }
          if (
            varName !== varDef.name &&
            customListVariables.some((v) => v.name === varName)
          ) {
            deps.add(varName);
          }
          if (
            varName !== varDef.name &&
            customSwitchCases.some((v) => v.name === varName)
          ) {
            deps.add(varName);
          }
        });
      }

      // Check for subfield dependencies in aggregations
      if (
        ["$min", "$max", "$avg", "$sum"].includes(condition.operator) &&
        condition.subField
      ) {
        // subField might reference other variables
        const subField = condition.subField;
        [
          ...customVariables,
          ...customListVariables,
          ...customSwitchCases,
        ].forEach((otherVar) => {
          if (
            otherVar.name !== varDef.name &&
            subField.includes(otherVar.name)
          ) {
            deps.add(otherVar.name);
          }
        });
      }
    }

    graph.variables.set(varDef.name, deps);
    deps.forEach((dep) => {
      if (graph.reverseDeps.has(dep)) {
        graph.reverseDeps.get(dep).add(varDef.name);
      }
    });
  });
};

/**
 * Builds dependencies for switch cases
 */
const buildSwitchDependencies = (
  customSwitchCases,
  graph,
  customVariables,
  customListVariables,
  fieldOptions,
) => {
  customSwitchCases.forEach((varDef) => {
    const deps = new Set();

    const switchDef = varDef.switchCondition || varDef.switch;
    // switchCondition has structure: { operator: "$switch", value: { cases: [...], default: ... } }
    // Extract the value if it exists, otherwise use switchDef directly (for backward compatibility)
    const switchValue = switchDef?.value || switchDef;

    if (switchValue?.cases) {
      switchValue.cases.forEach((caseItem) => {
        // Analyze the condition block
        if (caseItem.block) {
          analyzeBlockForDeps(
            caseItem.block,
            deps,
            customVariables,
            customListVariables,
            customSwitchCases,
            fieldOptions,
            varDef.name,
          );
        }

        // Analyze the 'then' value
        if (caseItem.then) {
          // Check if it's an object with metadata
          if (
            typeof caseItem.then === "object" &&
            caseItem.then._meta &&
            caseItem.then.name
          ) {
            const thenName = caseItem.then.name;
            const meta = caseItem.then._meta;

            // Add as dependency based on type
            if (meta.isSchemaField) {
              deps.add(thenName);
            } else if (meta.isListVariable || meta.isSwitchCase) {
              if (thenName !== varDef.name) {
                deps.add(thenName);
              }
            } else {
              // Fallback: check if it's actually a schema field even without the flag
              if (isSchemaField(thenName, fieldOptions)) {
                deps.add(thenName);
              }
            }
          }
          // Check if it's a string
          else if (typeof caseItem.then === "string") {
            // Check if it's a variable reference
            const isVariable = [
              ...customVariables,
              ...customListVariables,
              ...customSwitchCases,
            ].some(
              (otherVar) =>
                otherVar.name === caseItem.then &&
                otherVar.name !== varDef.name,
            );

            if (isVariable) {
              deps.add(caseItem.then);
            }
            // Check if it's a schema field
            else if (isSchemaField(caseItem.then, fieldOptions)) {
              deps.add(caseItem.then);
            }
          }
        }
      });

      // Check default value
      if (switchValue.default) {
        // Check if it's an object with metadata
        if (
          typeof switchValue.default === "object" &&
          switchValue.default._meta &&
          switchValue.default.name
        ) {
          const defaultName = switchValue.default.name;
          const meta = switchValue.default._meta;

          // Add as dependency based on type
          if (meta.isSchemaField) {
            deps.add(defaultName);
          } else if (meta.isListVariable || meta.isSwitchCase) {
            if (defaultName !== varDef.name) {
              deps.add(defaultName);
            }
          } else {
            // Fallback: check if it's actually a schema field even without the flag
            if (isSchemaField(defaultName, fieldOptions)) {
              deps.add(defaultName);
            }
          }
        }
        // Check if it's a string
        else if (typeof switchValue.default === "string") {
          // Check if it's a variable reference
          const isVariable = [
            ...customVariables,
            ...customListVariables,
            ...customSwitchCases,
          ].some(
            (otherVar) =>
              otherVar.name === switchValue.default &&
              otherVar.name !== varDef.name,
          );

          if (isVariable) {
            deps.add(switchValue.default);
          }
          // Check if it's a schema field
          else if (isSchemaField(switchValue.default, fieldOptions)) {
            deps.add(switchValue.default);
          }
        }
      }
    }
    graph.variables.set(varDef.name, deps);
    deps.forEach((dep) => {
      if (graph.reverseDeps.has(dep)) {
        graph.reverseDeps.get(dep).add(varDef.name);
      }
    });
  });
};

/**
 * Analyzes a block for dependencies
 */
const analyzeBlockForDeps = (
  block,
  deps,
  customVariables,
  customListVariables,
  customSwitchCases,
  fieldOptions,
  currentVarName = null,
) => {
  if (!block) return;

  if (block.children) {
    block.children.forEach((child) =>
      analyzeBlockForDeps(
        child,
        deps,
        customVariables,
        customListVariables,
        customSwitchCases,
        fieldOptions,
        currentVarName,
      ),
    );
  }

  if (block.field) {
    // Check if field has metadata (new format: {name: "...", _meta: {...}})
    if (block.field && typeof block.field === "object" && block.field._meta) {
      const fieldName = normalizeFieldName(block.field);
      const meta = block.field._meta;

      // Route based on metadata flags to avoid adding wrong dependencies when names collide
      if (meta.isListVariable) {
        deps.add(fieldName);
      } else if (meta.isSwitchCase && fieldName !== currentVarName) {
        deps.add(fieldName);
      } else if (meta.isSwitchCase && fieldName === currentVarName) {
        // Special case: switch case referencing itself
        // Check if it's also a schema field - if so, we need it as a dependency
        // because we're reading from the original schema field
        if (isSchemaField(fieldName, fieldOptions)) {
          deps.add(fieldName);
        }
      } else if (meta.isSchemaField) {
        deps.add(fieldName);
      } else if (meta.isVariable) {
        // Arithmetic variables need to be defined as fields when used in list variable contexts
        deps.add(fieldName);
      } else {
        // Fallback: metadata exists but no flags are set to true
        // Check if it's actually a schema field (the metadata might be incorrect)
        if (isSchemaField(fieldName, fieldOptions)) {
          deps.add(fieldName);
        }
      }
    }
    // Fallback: legacy string-based checking (may add multiple if names collide)
    else {
      // Normalize field to handle both string and object formats
      const fieldName = normalizeFieldName(block.field);
      // Add arithmetic variables as dependencies when used in list variable filters
      if (customVariables.some((v) => v.name === fieldName))
        deps.add(fieldName);
      if (customListVariables.some((v) => v.name === fieldName))
        deps.add(fieldName);
      // For switch cases, don't add self-references as dependencies...
      // UNLESS it's also a schema field (meaning we're reading from the original field)
      const isSwitchCase = customSwitchCases.some((v) => v.name === fieldName);
      const isCurrentVar = fieldName === currentVarName;
      if (isSwitchCase && !isCurrentVar) {
        deps.add(fieldName);
      }
      // Always check if it's a schema field (even if it matches a switch case name)
      if (
        fieldOptions.some((f) => f.value === fieldName || f.label === fieldName)
      ) {
        deps.add(fieldName);
      }
    }
  }

  if (block.value) {
    // Check if value has metadata (new format: {name: "...", _meta: {...}})
    if (
      block.value &&
      typeof block.value === "object" &&
      block.value._meta &&
      block.value.name
    ) {
      const valueName = block.value.name;
      const meta = block.value._meta;

      // Route based on metadata flags to avoid adding wrong dependencies when names collide
      if (meta.isListVariable) {
        deps.add(valueName);
      } else if (meta.isSwitchCase && valueName !== currentVarName) {
        deps.add(valueName);
      } else if (meta.isSwitchCase && valueName === currentVarName) {
        // Special case: switch case referencing itself
        // Check if it's also a schema field - if so, we need it as a dependency
        // because we're reading from the original schema field
        if (isSchemaField(valueName, fieldOptions)) {
          deps.add(valueName);
        }
      } else if (meta.isSchemaField) {
        deps.add(valueName);
      } else if (meta.isVariable) {
        // Arithmetic variables need to be defined as fields when used in list variable contexts
        deps.add(valueName);
      } else {
        // Fallback: metadata exists but no flags are set to true
        // Check if it's actually a schema field (the metadata might be incorrect)
        if (isSchemaField(valueName, fieldOptions)) {
          deps.add(valueName);
        }
      }
    }
    // Fallback: legacy string-based checking
    else {
      // Normalize value to handle both string and object formats
      const normalizedVal = normalizeValue(block.value);
      if (typeof normalizedVal === "string") {
        const value = normalizedVal;
        // Add arithmetic variables as dependencies when used in list variable filters
        if (customVariables.some((v) => v.name === value)) deps.add(value);
        if (customListVariables.some((v) => v.name === value)) deps.add(value);
        // For switch cases, don't add self-references as dependencies...
        // UNLESS it's also a schema field (meaning we're reading from the original field)
        const isSwitchCase = customSwitchCases.some((v) => v.name === value);
        const isCurrentVar = value === currentVarName;
        if (isSwitchCase && !isCurrentVar) deps.add(value);
        // Always check if it's a schema field (even if it matches a switch case name)
        if (fieldOptions.some((f) => f.value === value || f.label === value))
          deps.add(value);
      }
    }
  }
};

/**
 * Calculates dependency levels using topological sort
 */
const calculateDependencyLevels = (graph) => {
  const visited = new Set();
  const visiting = new Set();
  const levels = new Map();

  const visit = (node, currentLevel = 0) => {
    if (visiting.has(node)) {
      // Circular dependency detected - assign a high level to break the cycle
      return currentLevel + 10; // Assign high level to process later
    }
    if (visited.has(node)) {
      return Math.max(currentLevel, levels.get(node) || 0);
    }

    visiting.add(node);
    let maxDepLevel = 0;

    const deps = graph.variables.get(node) || new Set();
    for (const dep of deps) {
      if (graph.variables.has(dep)) {
        // Only for variables, not schema fields
        maxDepLevel = Math.max(maxDepLevel, visit(dep, currentLevel) + 1);
      }
    }

    visiting.delete(node);
    visited.add(node);
    levels.set(node, maxDepLevel);
    return maxDepLevel;
  };

  // Calculate levels for all variables
  for (const varName of graph.variables.keys()) {
    if (!visited.has(varName)) {
      visit(varName);
    }
  }

  graph.levels = levels;
};

/**
 * Determines what stages are needed in the pipeline
 */
const determineRequiredStages = (dependencyGraph) => {
  const usedFields = dependencyGraph.usedFields;

  return {
    needsInitialProject:
      usedFields.baseFields.size > 0 ||
      usedFields.customVariables.size > 0 ||
      usedFields.switchCases.size > 0,
    needsListStages: usedFields.listVariables.size > 0,
    needsMatchStages: true, // Always include match stages for filters
    needsFinalProject: true, // Always include final project
  };
};

/**
 * Builds the initial project stage for base variables
 */
const buildInitialProjectStage = (
  dependencyGraph,
  customVariables,
  customListVariables,
  customSwitchCases,
  fieldOptions,
) => {
  const project = { objectId: 1, "candidate.jd": 1 };

  // Add base fields only - computed variables will be added in separate $addFields stages
  // Filter out redundant parent paths when children are projected (e.g., exclude "prv_candidates" if "prv_candidates.isdiffpos" is projected)
  const baseFieldsArray = Array.from(dependencyGraph.usedFields.baseFields);
  const fieldsToProject = baseFieldsArray.filter((field) => {
    // Check if any other field is a child of this field
    // If this field has children being projected, exclude this parent path
    return !baseFieldsArray.some((otherField) => {
      return otherField !== field && otherField.startsWith(`${field}.`);
    });
  });

  fieldsToProject.forEach((field) => {
    project[field] = 1;
  });

  // Add dependencies of switch cases that are schema fields
  // This is needed when a switch case reads from and overwrites the same field
  dependencyGraph.usedFields.switchCases.forEach((switchCaseName) => {
    const deps = dependencyGraph.variables.get(switchCaseName);
    if (deps) {
      deps.forEach((dep) => {
        // Check if this dependency is a schema field
        // It might also be a variable name, but if it's a schema field, we need it in the projection
        const isSchemaFieldDep = fieldOptions.some(
          (f) => f.value === dep || f.label === dep,
        );

        if (isSchemaFieldDep) {
          // It's a schema field, add it to the projection
          project[dep] = 1;
        }
      });
    }
  });

  // Add dependencies of list variables that are schema fields
  dependencyGraph.usedFields.listVariables.forEach((listVarName) => {
    const deps = dependencyGraph.variables.get(listVarName);
    if (deps) {
      deps.forEach((dep) => {
        // Check if this dependency is a schema field
        // It might also be a variable name, but if it's a schema field, we need it in the projection
        const isSchemaFieldDep = fieldOptions.some(
          (f) => f.value === dep || f.label === dep,
        );

        if (isSchemaFieldDep) {
          // It's a schema field, add it to the projection
          project[dep] = 1;
        }
      });
    }
  });

  // Note: Level 0 list variables and switch cases are now added via $addFields
  // in buildVariableStagesByLevel to ensure proper stage separation

  // Final pass: Remove parent paths if any of their children are in the project
  // This handles cases where dependencies might conflict with already projected fields
  const allProjectedFields = Object.keys(project);
  const filteredProject = {};

  allProjectedFields.forEach((field) => {
    // Check if any other projected field is a child of this field
    const hasChildProjected = allProjectedFields.some((otherField) => {
      return otherField !== field && otherField.startsWith(`${field}.`);
    });

    // Only add this field if it doesn't have children being projected
    if (!hasChildProjected) {
      filteredProject[field] = project[field];
    }
  });

  return { $project: filteredProject };
};

/**
 * Builds stages for variables (list, arithmetic, switch) in dependency order by level
 */
const buildVariableStagesByLevel = (
  dependencyGraph,
  customVariables,
  customListVariables,
  customSwitchCases,
  fieldOptions,
) => {
  const stages = [];
  const maxLevel = Math.max(...Array.from(dependencyGraph.levels.values()), 0);
  const materializedListVars = new Set(); // Track which list variables are actually materialized

  // Start from level 0 to include all variables (including those with no dependencies)
  for (let level = 0; level <= maxLevel; level++) {
    // NOTE: Arithmetic variables are ALWAYS inlined, never materialized in $addFields
    // This ensures full recursive expansion of nested variable references
    // Skip arithmetic variable materialization entirely

    // Build switch variables at this level (use $addFields)
    const levelSwitchVars = customSwitchCases.filter(
      (varDef) =>
        (dependencyGraph.levels.get(varDef.name) || 0) === level &&
        dependencyGraph.usedFields.switchCases.has(varDef.name),
    );

    if (levelSwitchVars.length > 0) {
      const addFields = { $addFields: {} };

      levelSwitchVars.forEach((varDef) => {
        try {
          const switchDef = varDef.switchCondition || varDef.switch;
          // switchCondition has structure: { operator: "$switch", value: { cases: [...], default: ... } }
          // convertSwitchExpression expects: { cases: [...], default: ... }
          const switchValue = switchDef?.value || switchDef;
          const switchExpr = convertSwitchExpression(
            switchValue,
            fieldOptions,
            customVariables,
            customListVariables,
            customSwitchCases,
          );
          addFields.$addFields[varDef.name] = switchExpr;
        } catch (error) {
          // Skip invalid switch case
        }
      });

      if (Object.keys(addFields.$addFields).length > 0) {
        stages.push(addFields);
      }
    }

    // Then, build list variables at this level (use $addFields)
    // Always define list variables at their level, regardless of usage count
    const levelListVars = customListVariables.filter((varDef) => {
      const varLevel = dependencyGraph.levels.get(varDef.name) || 0;
      const isUsed = dependencyGraph.usedFields.listVariables.has(varDef.name);
      return varLevel === level && isUsed;
    });

    if (levelListVars.length > 0) {
      const addFields = { $addFields: {} };

      levelListVars.forEach((varDef) => {
        addFields.$addFields[varDef.name] = generateListVariableExpression(
          varDef.listCondition,
          customListVariables,
          dependencyGraph,
          fieldOptions,
          customVariables,
          customSwitchCases,
        );
        // Track that this list variable was materialized
        materializedListVars.add(varDef.name);
      });

      stages.push(addFields);
    }
  }

  // Store materialized list variables in the dependency graph for later use
  dependencyGraph.materializedListVars = materializedListVars;

  return stages;
};

/**
 * Builds custom block definitions stage (only for blocks used 2+ times)
 * Custom blocks used multiple times are defined as boolean variables to avoid duplication
 */
const buildCustomBlockStage = (
  dependencyGraph,
  schema,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  if (
    !dependencyGraph.customBlockUsage ||
    dependencyGraph.customBlockUsage.size === 0
  ) {
    return null;
  }

  const addFields = { $addFields: {} };

  // Only define blocks that are used 2 or more times
  for (const [blockName, usage] of dependencyGraph.customBlockUsage.entries()) {
    if (usage.count >= 2 && usage.block) {
      // Convert the block's children to a boolean expression
      // Pass null for dependencyGraph to prevent infinite recursion (inline child blocks)
      const blockCondition = convertBlockToMongoExpr(
        usage.block,
        null, // Don't reference custom blocks within custom block definitions
        fieldOptions,
        customVariables,
        customListVariables,
        customSwitchCases,
        null,
        null,
        true, // expressionContext - $expr requires expression syntax
      );

      if (blockCondition && Object.keys(blockCondition).length > 0) {
        // Wrap in $expr to make it a boolean field
        addFields.$addFields[blockName] = { $expr: blockCondition };
      }
    }
  }

  if (Object.keys(addFields.$addFields).length === 0) {
    return null;
  }

  return addFields;
};

/**
 * Extracts simple conditions that can be moved to an early $match stage
 * Simple conditions only reference base fields and use basic operators
 * Respects block atomicity - doesn't split mixed blocks
 * For custom blocks at root, unwraps one level and extracts simple children
 */
const extractEarlyMatchConditions = (
  filters,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  if (!filters || filters.length === 0) {
    return { earlyMatch: {}, remainingFilters: [] };
  }

  const simpleConditions = [];
  const remainingFilters = [];

  filters.forEach((filter, index) => {
    // Check if this is a block at root level (custom or regular)
    if (
      (filter.type === "block" || filter.category === "block") &&
      filter.children &&
      filter.children.length > 0
    ) {
      const simpleChildren = [];
      const complexChildren = [];
      const parentLogic = (filter.logic || "and").toLowerCase();

      // Unwrap two levels: root block and its immediate child blocks
      filter.children.forEach((child, childIndex) => {
        // If child is also a block, unwrap it one level
        if (
          (child.type === "block" || child.category === "block") &&
          child.children &&
          child.children.length > 0
        ) {
          const childLogic = (child.logic || "and").toLowerCase();

          // If logical operators differ, check if the entire block is simple
          // and keep it intact (can't split conditions across different logical operators)
          if (childLogic !== parentLogic) {
            const childBlockIsSimple = isSimpleBlock(
              child,
              fieldOptions,
              customVariables,
              customListVariables,
              customSwitchCases,
            );

            if (childBlockIsSimple) {
              simpleChildren.push(child);
            } else {
              complexChildren.push(child);
            }
          } else {
            // Same logical operator - ALWAYS unwrap to grandchild level and check each individually
            // This ensures we properly unwrap 2 levels (root + imported custom block)
            const simpleGrandchildren = [];
            const complexGrandchildren = [];

            child.children.forEach((grandchild, grandchildIndex) => {
              const isSimple = isSimpleBlock(
                grandchild,
                fieldOptions,
                customVariables,
                customListVariables,
                customSwitchCases,
              );

              if (isSimple) {
                simpleGrandchildren.push(grandchild);
              } else {
                complexGrandchildren.push(grandchild);
              }
            });

            // Add simple grandchildren directly
            simpleChildren.push(...simpleGrandchildren);

            // If there are complex grandchildren, reconstruct child block with only them
            if (complexGrandchildren.length > 0) {
              complexChildren.push({
                ...child,
                children: complexGrandchildren,
              });
            }
          }
        } else {
          // It's a direct condition
          const isSimple = isSimpleBlock(
            child,
            fieldOptions,
            customVariables,
            customListVariables,
            customSwitchCases,
          );

          if (isSimple) {
            simpleChildren.push(child);
          } else {
            complexChildren.push(child);
          }
        }
      });

      // Add simple children directly to early match
      if (simpleChildren.length > 0) {
        simpleConditions.push(...simpleChildren);
      }

      // If there are complex children, reconstruct the block with only complex children
      if (complexChildren.length > 0) {
        const remainingBlock = {
          ...filter,
          children: complexChildren,
        };
        remainingFilters.push(remainingBlock);
      }
    }
    // Check if this is a simple block/condition
    else if (
      isSimpleBlock(
        filter,
        fieldOptions,
        customVariables,
        customListVariables,
        customSwitchCases,
      )
    ) {
      simpleConditions.push(filter);
    } else {
      remainingFilters.push(filter);
    }
  });

  // Convert simple conditions to MongoDB match expression
  let earlyMatch = {};
  if (simpleConditions.length > 0) {
    earlyMatch = convertFiltersToMatch(
      simpleConditions,
      null, // No dependency graph needed for simple conditions
      {}, // No schema needed
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
    );
  }

  return { earlyMatch, remainingFilters };
};

/**
 * Checks if a block/condition is simple (only uses base fields and basic operators)
 * Treats blocks as atomic - if any child is not simple, the whole block is not simple
 */
const isSimpleBlock = (
  block,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  if (!block) return false;

  // If it has children, check all children recursively
  if (block.children && block.children.length > 0) {
    return block.children.every((child) =>
      isSimpleBlock(
        child,
        fieldOptions,
        customVariables,
        customListVariables,
        customSwitchCases,
      ),
    );
  }

  // For leaf conditions, check if simple
  return isSimpleCondition(
    block,
    fieldOptions,
    customVariables,
    customListVariables,
    customSwitchCases,
  );
};

const isSimpleCondition = (
  condition,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  if (!condition || !condition.field || !condition.operator) {
    return false;
  }

  // Normalize field to handle both string and object formats
  const fieldName = normalizeFieldName(condition.field);
  const { operator, value, fieldType } = condition;

  // Check if operator is a basic operator (MongoDB operators and friendly names)
  const basicOperators = [
    "$eq",
    "equals",
    "=",
    "==",
    "$ne",
    "not equals",
    "!=",
    "<>",
    "$gt",
    "greater than",
    ">",
    "$gte",
    "greater than or equal to",
    ">=",
    "$lt",
    "less than",
    "<",
    "$lte",
    "less than or equal to",
    "<=",
    "$in",
    "in",
    "$exists",
    "exists",
  ];
  if (!basicOperators.includes(operator)) {
    return false;
  }

  // Check field type
  if (fieldType) {
    // Explicit field type - only 'schema' is simple
    if (fieldType !== "schema") {
      return false;
    }
  }
  // Check if field has metadata (new format: {name: "...", _meta: {...}})
  else if (
    condition.field &&
    typeof condition.field === "object" &&
    condition.field._meta
  ) {
    const meta = condition.field._meta;

    // Only schema fields are simple
    if (meta.isSchemaField) {
      // Continue to schema field validation below
    } else {
      // It's a variable/list variable/switch case - not simple
      return false;
    }
  }
  // Fallback: check by name (may be incorrect if names collide)
  else {
    // Implicit field type - check if it's a computed variable or list
    if (customVariables.some((v) => v.name === fieldName)) {
      return false; // Arithmetic variable - not simple
    }
    if (customListVariables.some((v) => v.name === fieldName)) {
      return false; // List variable - not simple
    }
    if (customSwitchCases.some((v) => v.name === fieldName)) {
      return false; // Switch case - not simple
    }
  }

  // Check if field exists in schema
  let fieldDef = fieldOptions.find(
    (f) => f.value === fieldName || f.label === fieldName,
  );

  // For array subfields (e.g., "fp_hists.procstatus"), also try just the field name ("procstatus")
  if (!fieldDef && fieldName.includes(".")) {
    const simpleFieldName = fieldName.split(".").pop();
    fieldDef = fieldOptions.find(
      (f) => f.value === simpleFieldName || f.label === simpleFieldName,
    );
  }

  if (!fieldDef && fieldOptions.length > 0) {
    // For nested fields (e.g., "candidate.rb"), check if the parent path exists
    if (fieldName.includes(".")) {
      const parts = fieldName.split(".");
      // Check progressively: "candidate.rb" -> check "candidate" or "candidate.rb"
      let found = false;
      for (let i = parts.length; i > 0; i--) {
        const partialPath = parts.slice(0, i).join(".");
        if (
          fieldOptions.some(
            (f) => f.value === partialPath || f.label === partialPath,
          )
        ) {
          found = true;
          break;
        }
      }
      if (!found) {
        return false; // Neither full path nor parent path found in schema
      }
    } else {
      // Simple field not found in fieldOptions
      return false;
    }
  }
  // If fieldOptions is empty, we can't validate, so assume it might be valid
  // (This happens in tests without full field metadata)

  // Check if value references computed fields
  if (typeof value === "string") {
    // Allow field references to other schema fields (these are valid in $expr)
    const valueFieldDef = fieldOptions.find(
      (f) => f.value === value || f.label === value,
    );
    if (valueFieldDef) {
      // It's a reference to another schema field - this is OK for early match
      return true;
    }

    // Check if value is a variable reference (not allowed)
    if (customVariables.some((v) => v.name === value)) {
      return false;
    }
    if (customListVariables.some((v) => v.name === value)) {
      return false;
    }
    if (customSwitchCases.some((v) => v.name === value)) {
      return false;
    }
  }

  return true;
};

/**
 * Builds match stages for filters
 */
const buildMatchStages = (
  filters,
  dependencyGraph,
  schema,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  const stages = [];

  // Convert filters to MongoDB match conditions
  const matchConditions = convertFiltersToMatch(
    filters,
    dependencyGraph,
    schema,
    fieldOptions,
    customVariables,
    customListVariables,
    customSwitchCases,
  );

  if (matchConditions && Object.keys(matchConditions).length > 0) {
    stages.push({ $match: matchConditions });
  }

  return stages;
};

/**
 * Builds the final project stage
 */
const buildFinalProjectStage = (
  dependencyGraph,
  additionalFieldsToProject,
  fieldOptions,
) => {
  const project = { objectId: 1, "candidate.jd": 1 }; // Always include objectId and candidate.jd for reference

  // Add all used fields
  // Filter out redundant child paths when parent is already projected (e.g., exclude "prv_candidates.isdiffpos" if "prv_candidates" is projected)
  const baseFieldsArray = Array.from(dependencyGraph.usedFields.baseFields);
  const fieldsToProject = baseFieldsArray.filter((field) => {
    // Check if any other field is a parent of this field
    // A parent would be a prefix followed by a dot (e.g., "prv_candidates" is parent of "prv_candidates.isdiffpos")
    return !baseFieldsArray.some((otherField) => {
      return otherField !== field && field.startsWith(`${otherField}.`);
    });
  });

  fieldsToProject.forEach((field) => {
    project[field] = 1;
  });

  // Note: Arithmetic variables are inlined, so they don't exist as document fields and shouldn't be projected
  // dependencyGraph.usedFields.customVariables.forEach(varName => {
  //   project[varName] = 1;
  // });

  // Only project list variables that were actually materialized
  const materializedListVars =
    dependencyGraph.materializedListVars || new Set();
  dependencyGraph.usedFields.listVariables.forEach((varName) => {
    // Only include in project if it was actually defined in an $addFields stage
    if (materializedListVars.has(varName)) {
      project[varName] = 1;
    }
  });

  dependencyGraph.usedFields.switchCases.forEach((varName) => {
    project[varName] = 1;
  });

  // Add additional fields
  additionalFieldsToProject.forEach((field) => {
    project[field] = 1;
  });

  // Final pass: Remove parent paths if any of their children are in the project
  // This handles cases where additionalFieldsToProject might conflict with already projected fields
  const allProjectedFields = Object.keys(project);
  const filteredProject = {};

  allProjectedFields.forEach((field) => {
    // Check if any other projected field is a child of this field
    const hasChildProjected = allProjectedFields.some((otherField) => {
      return otherField !== field && otherField.startsWith(`${field}.`);
    });

    // Only add this field if it doesn't have children being projected
    if (!hasChildProjected) {
      filteredProject[field] = project[field];
    }
  });

  return { $project: filteredProject };
};

/**
 * Converts arithmetic expression to MongoDB expression with variable inlining
 */
const convertArithmeticExpression = (
  variableDefinition,
  customVariables = [],
  processedVars = new Set(),
) => {
  if (!variableDefinition || !variableDefinition.includes("=")) {
    return null;
  }

  const expression = variableDefinition.split("=")[1].trim();

  try {
    // First, inline any variable references in the expression
    let inlinedExpression = inlineVariablesInExpression(
      expression,
      customVariables,
      processedVars,
    );
    return latexToMongoConverter.convertToMongo(inlinedExpression);
  } catch (error) {
    return null;
  }
};

/**
 * Inlines variable references in an expression recursively
 */
const inlineVariablesInExpression = (
  expression,
  customVariables,
  processedVars = new Set(),
) => {
  if (!expression || typeof expression !== "string") {
    return expression;
  }

  let result = expression;

  // Find all variable references (word characters that are not numbers)
  const varPattern = /\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g;
  const matches = [...result.matchAll(varPattern)];

  for (const match of matches) {
    const varName = match[1];

    // Skip if it's a number or if we've already processed this variable (to prevent infinite recursion)
    if (!isNaN(varName) || processedVars.has(varName)) {
      continue;
    }

    // Check if it's a custom variable
    const varDef = customVariables.find((v) => v.name === varName);
    if (varDef && varDef.variable) {
      // Prevent infinite recursion
      processedVars.add(varName);

      // Get the variable's expression
      const varExpr = varDef.variable.split("=")[1]?.trim();
      if (varExpr) {
        // Recursively inline variables in the variable's expression
        const inlinedVarExpr = inlineVariablesInExpression(
          varExpr,
          customVariables,
          processedVars,
        );

        // Replace the variable reference with the inlined expression, wrapped in parentheses
        result = result.replace(
          new RegExp(`\\b${varName}\\b`, "g"),
          `(${inlinedVarExpr})`,
        );
      }

      processedVars.delete(varName);
    }
  }

  return result;
};

/**
 * Replaces references to arrayField with $$this in a MongoDB expression
 */
const replaceArrayFieldInExpr = (expr, arrayField) => {
  if (!expr || !arrayField) return expr;

  if (typeof expr === "string") {
    // Replace $arrayField.subfield with $$this.subfield
    if (expr.startsWith(`$${arrayField}.`)) {
      const subfield = expr.substring(arrayField.length + 2); // +2 for $ and .
      return `$$this.${subfield}`;
    }
    return expr;
  }

  if (Array.isArray(expr)) {
    return expr.map((item) => replaceArrayFieldInExpr(item, arrayField));
  }

  if (typeof expr === "object") {
    const newObj = {};
    for (const key in expr) {
      newObj[key] = replaceArrayFieldInExpr(expr[key], arrayField);
    }
    return newObj;
  }

  return expr;
};

/**
 * Inlines arithmetic variables in MongoDB expressions recursively
 */
const inlineVariablesInMongoExpr = (
  expr,
  customVariables,
  processedVars = new Set(),
  arrayField = null,
) => {
  if (!expr) return expr;

  if (typeof expr === "string") {
    // Check if it's a field/variable reference like "$varName" or "$varName.field"
    if (expr.startsWith("$")) {
      const refName = expr.substring(1);

      // Check if this is a reference to the array field being mapped over (e.g., $10days.magpsf)
      if (arrayField && refName.startsWith(`${arrayField}.`)) {
        // Replace $arrayField.subfield with $$this.subfield
        const subfield = refName.substring(arrayField.length + 1);
        return `$$this.${subfield}`;
      }

      // Check if it's a variable reference ($$varName in some contexts, but here $varName)
      const arithVar = customVariables.find((v) => v.name === refName);
      if (arithVar && !processedVars.has(refName)) {
        processedVars.add(refName);
        const inlined = convertArithmeticExpression(
          arithVar.variable,
          customVariables,
          processedVars,
        );
        processedVars.delete(refName);
        // If we're in an array context, replace arrayField references in the inlined expression
        if (arrayField && inlined) {
          return replaceArrayFieldInExpr(inlined, arrayField);
        }
        return inlined;
      }
    }
    return expr;
  }

  if (Array.isArray(expr)) {
    return expr.map((item) =>
      inlineVariablesInMongoExpr(
        item,
        customVariables,
        processedVars,
        arrayField,
      ),
    );
  }

  if (typeof expr === "object") {
    const newObj = {};
    for (const key in expr) {
      newObj[key] = inlineVariablesInMongoExpr(
        expr[key],
        customVariables,
        processedVars,
        arrayField,
      );
    }
    return newObj;
  }

  return expr;
};

/**
 * Converts switch expression to MongoDB expression
 */
/**
 * Helper to check if a field name is a schema field
 * Handles nested fields by checking partial paths
 */
const isSchemaField = (fieldName, fieldOptions) => {
  if (!fieldName || typeof fieldName !== "string") {
    return false;
  }

  // Check exact match first (works when fieldOptions is populated)
  if (fieldOptions && fieldOptions.length > 0) {
    if (
      fieldOptions.some((f) => f.value === fieldName || f.label === fieldName)
    ) {
      return true;
    }

    // For nested fields (e.g., "candidate.magap"), check partial paths
    if (fieldName.includes(".")) {
      const parts = fieldName.split(".");
      // Check progressively: "candidate.magap" -> check "candidate" or "candidate.magap"
      for (let i = parts.length; i > 0; i--) {
        const partialPath = parts.slice(0, i).join(".");
        if (
          fieldOptions.some(
            (f) => f.value === partialPath || f.label === partialPath,
          )
        ) {
          return true;
        }
      }
    }
  }

  // Fallback: If it looks like a schema field path, assume it is
  // Common prefixes for schema fields in this application
  if (fieldName.includes(".")) {
    const prefix = fieldName.split(".")[0];
    const knownPrefixes = [
      "candidate",
      "annotations",
      "cross_matches",
      "fp_hists",
      "prv_candidates",
    ];
    if (knownPrefixes.includes(prefix)) {
      return true;
    }
  }

  return false;
};

/**
 * Unwraps $expr wrappers to get pure aggregation expressions
 * This is needed for contexts like $switch where expressions should not be wrapped
 */
const unwrapExprForAggregation = (condition) => {
  if (!condition || typeof condition !== "object") {
    return condition;
  }

  // If it's wrapped in $expr, unwrap it
  if (condition.$expr) {
    return condition.$expr;
  }

  // Handle $and and $or operators recursively
  if (condition.$and && Array.isArray(condition.$and)) {
    return {
      $and: condition.$and.map(unwrapExprForAggregation),
    };
  }

  if (condition.$or && Array.isArray(condition.$or)) {
    return {
      $or: condition.$or.map(unwrapExprForAggregation),
    };
  }

  if (condition.$nor && Array.isArray(condition.$nor)) {
    return {
      $nor: condition.$nor.map(unwrapExprForAggregation),
    };
  }

  // If it's already an unwrapped expression, return as-is
  return condition;
};

const convertSwitchExpression = (
  switchValue,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  if (!switchValue || !switchValue.cases || switchValue.cases.length === 0) {
    throw new Error(
      `Invalid switch expression: missing or empty cases for switch variable`,
    );
  }

  const branches = [];

  switchValue.cases.forEach((caseItem, index) => {
    if (caseItem.block && caseItem.then) {
      let condition = convertBlockToMongoExpr(
        caseItem.block,
        null, // dependencyGraph not available in switch expression context
        fieldOptions,
        customVariables,
        customListVariables,
        customSwitchCases,
        null, // arrayField
        null, // subFieldOptions
        true, // expressionContext - use expression syntax for $switch
      );

      // Unwrap $expr for switch case context (in case any conditions still have it)
      // MongoDB $switch expects unwrapped aggregation expressions in the case field
      condition = unwrapExprForAggregation(condition);

      let thenValue;
      // Handle object with metadata (new format: {name: "...", _meta: {...}})
      if (
        caseItem.then &&
        typeof caseItem.then === "object" &&
        caseItem.then._meta &&
        caseItem.then.name
      ) {
        const thenName = caseItem.then.name;
        const meta = caseItem.then._meta;

        // Route based on metadata
        if (meta.isSwitchCase) {
          thenValue = `$${thenName}`;
        } else if (meta.isListVariable) {
          thenValue = `$${thenName}`;
        } else if (meta.isVariable) {
          // Arithmetic variables need to be inlined
          const arithVar = customVariables.find((v) => v.name === thenName);
          if (arithVar) {
            thenValue = convertArithmeticExpression(
              arithVar.variable,
              customVariables,
            );
          } else {
            thenValue = thenName;
          }
        } else if (meta.isSchemaField) {
          thenValue = `$${thenName}`;
        } else {
          // Fallback: check if it's actually a schema field even without the flag
          if (isSchemaField(thenName, fieldOptions)) {
            thenValue = `$${thenName}`;
          } else {
            // If no metadata matches and not a schema field, treat as literal
            thenValue = isNaN(thenName) ? thenName : parseFloat(thenName);
          }
        }
      }
      // Handle string (legacy format)
      else if (typeof caseItem.then === "string") {
        // Check if it's a variable reference
        const varRef = [
          ...customVariables,
          ...customListVariables,
          ...customSwitchCases,
        ].find((v) => v.name === caseItem.then);
        if (varRef) {
          // If it's an arithmetic variable, inline its expression
          if (customVariables.some((v) => v.name === caseItem.then)) {
            const arithVar = customVariables.find(
              (v) => v.name === caseItem.then,
            );
            thenValue = convertArithmeticExpression(
              arithVar.variable,
              customVariables,
            );
          } else {
            thenValue = `$${caseItem.then}`;
          }
        } else {
          // Check if it's a schema field
          if (isSchemaField(caseItem.then, fieldOptions)) {
            thenValue = `$${caseItem.then}`;
          } else {
            // Parse as number if possible, otherwise keep as string literal
            thenValue = isNaN(caseItem.then)
              ? caseItem.then
              : parseFloat(caseItem.then);
          }
        }
      } else {
        // Fallback: use as-is for numbers, booleans, etc.
        thenValue = caseItem.then;
      }

      branches.push({
        case: condition,
        then: thenValue,
      });
    }
  });

  const switchExpr = {
    $switch: {
      branches,
    },
  };

  if (switchValue.default !== undefined && switchValue.default !== null) {
    // Handle object with metadata (new format: {name: "...", _meta: {...}})
    if (
      switchValue.default &&
      typeof switchValue.default === "object" &&
      switchValue.default._meta &&
      switchValue.default.name
    ) {
      const defaultName = switchValue.default.name;
      const meta = switchValue.default._meta;

      // Route based on metadata
      if (meta.isSwitchCase) {
        switchExpr.$switch.default = `$${defaultName}`;
      } else if (meta.isListVariable) {
        switchExpr.$switch.default = `$${defaultName}`;
      } else if (meta.isVariable) {
        // Arithmetic variables need to be inlined
        const arithVar = customVariables.find((v) => v.name === defaultName);
        if (arithVar) {
          switchExpr.$switch.default = convertArithmeticExpression(
            arithVar.variable,
            customVariables,
          );
        } else {
          switchExpr.$switch.default = defaultName;
        }
      } else if (meta.isSchemaField) {
        switchExpr.$switch.default = `$${defaultName}`;
      } else {
        // Fallback: check if it's actually a schema field even without the flag
        if (isSchemaField(defaultName, fieldOptions)) {
          switchExpr.$switch.default = `$${defaultName}`;
        } else {
          // If no metadata matches and not a schema field, treat as literal
          switchExpr.$switch.default = isNaN(defaultName)
            ? defaultName
            : parseFloat(defaultName);
        }
      }
    }
    // Handle string (legacy format)
    else if (typeof switchValue.default === "string") {
      const varRef = [
        ...customVariables,
        ...customListVariables,
        ...customSwitchCases,
      ].find((v) => v.name === switchValue.default);

      if (varRef) {
        // If it's an arithmetic variable, inline its expression
        if (customVariables.some((v) => v.name === switchValue.default)) {
          const arithVar = customVariables.find(
            (v) => v.name === switchValue.default,
          );
          switchExpr.$switch.default = convertArithmeticExpression(
            arithVar.variable,
            customVariables,
          );
        } else {
          switchExpr.$switch.default = `$${switchValue.default}`;
        }
      } else {
        // Check if it's a schema field
        const isSchema = isSchemaField(switchValue.default, fieldOptions);

        if (isSchema) {
          switchExpr.$switch.default = `$${switchValue.default}`;
        } else {
          // Parse as number if possible, otherwise keep as string literal
          switchExpr.$switch.default = isNaN(switchValue.default)
            ? switchValue.default
            : parseFloat(switchValue.default);
        }
      }
    } else {
      // Fallback: use as-is for numbers, booleans, etc.
      switchExpr.$switch.default = switchValue.default;
    }
  }

  return switchExpr;
};

const getBooleanSwitch = (condition, value) => {
  if (typeof value === "boolean") {
    return value;
  }

  if (value && typeof value === "object" && value.children) {
    return condition.booleanSwitch !== undefined
      ? condition.booleanSwitch
      : condition.isTrue !== undefined
        ? condition.isTrue
        : condition.switchValue !== undefined
          ? condition.switchValue
          : condition.booleanValue !== undefined
            ? condition.booleanValue
            : condition.not !== undefined
              ? !condition.not
              : condition.negate !== undefined
                ? !condition.negate
                : true; // Default to true for boolean variables
  }

  // Check for explicit booleanSwitch property on condition
  if (condition && condition.booleanSwitch !== undefined) {
    return condition.booleanSwitch;
  }

  // Default to true for boolean variables (backwards compatible behavior)
  return true;
};

const parseNumberIfNeeded = (value) => {
  if (typeof value === "string" && !isNaN(value) && !isNaN(parseFloat(value))) {
    return parseFloat(value);
  }
  return value;
};

/**
 * Generates MongoDB expression for list variables
 */
const generateListVariableExpression = (
  listCondition,
  customListVariables,
  dependencyGraph,
  fieldOptions,
  customVariables,
  customSwitchCases,
) => {
  if (!listCondition) return null;

  const { operator, field, value, subField, subFieldOptions } = listCondition;
  switch (operator) {
    case "$anyElementTrue":
      if (value && value.children) {
        const condition = convertBlockToMongoExpr(
          { children: value.children },
          null, // dependencyGraph not available in list condition context
          fieldOptions,
          customVariables,
          customListVariables,
          customSwitchCases,
          field, // arrayField
          subFieldOptions, // array subFieldOptions
        );
        if (condition && Object.keys(condition).length > 0) {
          return {
            $anyElementTrue: {
              $map: {
                input: { $ifNull: [`$${field}`, []] },
                in: condition,
              },
            },
          };
        }
      }
      return { $anyElementTrue: { $ifNull: [`$${field}`, []] } };

    case "$allElementTrue":
      if (value && value.children) {
        const condition = convertBlockToMongoExpr(
          { children: value.children },
          null, // dependencyGraph not available in list condition context
          fieldOptions,
          customVariables,
          customListVariables,
          customSwitchCases,
          field, // arrayField,
          subFieldOptions, // array subFieldOptions
        );
        if (condition && Object.keys(condition).length > 0) {
          return {
            $allElementTrue: {
              $map: {
                input: { $ifNull: [`$${field}`, []] },
                in: condition,
              },
            },
          };
        }
      }
      return { $allElementTrue: { $ifNull: [`$${field}`, []] } };

    case "$filter":
      if (value && value.children) {
        const condition = convertBlockToMongoExpr(
          { children: value.children },
          null, // dependencyGraph not available in list condition context
          fieldOptions,
          customVariables,
          customListVariables,
          customSwitchCases,
          field, // arrayField
          subFieldOptions, // array subFieldOptions
        );
        return {
          $filter: {
            input: `$${field}`,
            cond: condition,
          },
        };
      }
      return { $ifNull: [`$${field}`, []] };

    case "$map":
      if (value && value.mapExpression) {
        const inlinedMapExpression = inlineVariablesInMongoExpr(
          value.mapExpression,
          customVariables,
          new Set(),
          field,
        );
        // Only create $map if the inlined expression is valid
        if (
          inlinedMapExpression &&
          (typeof inlinedMapExpression === "object" ||
            typeof inlinedMapExpression === "string")
        ) {
          return {
            $map: {
              input: { $ifNull: [`$${field}`, []] },
              in: inlinedMapExpression,
            },
          };
        }
      }
      return { $ifNull: [`$${field}`, []] };

    case "$min":
      if (subField) {
        return { $min: `$${field}.${subField}` };
      }
      return { $min: `$${field}` };

    case "$max":
      if (subField) {
        return { $max: `$${field}.${subField}` };
      }
      return { $max: `$${field}` };

    case "$avg":
      if (subField) {
        return { $avg: `$${field}.${subField}` };
      }
      return { $avg: `$${field}` };

    case "$median":
      if (subField) {
        return {
          $median: {
            input: `$${field}.${subField}`,
            method: "approximate",
          },
        };
      }
      return {
        $median: {
          input: `$${field}`,
          method: "approximate",
        },
      };

    case "$sum":
      if (subField) {
        return { $sum: `$${field}.${subField}` };
      }
      return { $sum: `$${field}` };

    case "$size":
      return { $size: { $ifNull: [`$${field}`, []] } };

    default:
      return { $ifNull: [`$${field}`, []] };
  }
};

/**
 * Converts filters to MongoDB match conditions
 */
const convertFiltersToMatch = (
  filters,
  dependencyGraph,
  schema,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
) => {
  if (!filters || filters.length === 0) {
    return {};
  }

  const conditions = [];

  filters.forEach((filter) => {
    const condition = convertBlockToMongoExpr(
      filter,
      dependencyGraph,
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
    );
    if (condition && Object.keys(condition).length > 0) {
      conditions.push(condition);
    }
  });

  if (conditions.length === 0) {
    return {};
  }

  if (conditions.length === 1) {
    return conditions[0];
  }

  return { $and: conditions };
};

/**
 * Converts a block to MongoDB expression
 */
const convertBlockToMongoExpr = (
  block,
  dependencyGraph,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
  arrayField = null,
  subFieldOptions = null,
  expressionContext = false,
) => {
  if (!block) {
    return {};
  }

  // If this is a custom block that's been defined (used 2+ times), reference it by name
  if (block.customBlockName && dependencyGraph?.customBlockUsage) {
    const usage = dependencyGraph.customBlockUsage.get(block.customBlockName);
    if (usage && usage.count >= 2) {
      // Return a simple field reference with the boolean value
      const fieldValue = block.isTrue !== false;
      return { [block.customBlockName]: fieldValue };
    }
  }

  if (block.type === "condition" || block.category === "condition") {
    return convertConditionToMongoExpr(
      block,
      dependencyGraph,
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
      arrayField,
      subFieldOptions,
      expressionContext,
    );
  }

  if (!block.children || block.children.length === 0) {
    return {};
  }

  const conditions = [];

  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      const nestedCondition = convertBlockToMongoExpr(
        child,
        dependencyGraph,
        fieldOptions,
        customVariables,
        customListVariables,
        customSwitchCases,
        arrayField,
        subFieldOptions,
        expressionContext,
      );
      if (nestedCondition && Object.keys(nestedCondition).length > 0) {
        conditions.push(nestedCondition);
      }
    } else {
      const condition = convertConditionToMongoExpr(
        child,
        dependencyGraph,
        fieldOptions,
        customVariables,
        customListVariables,
        customSwitchCases,
        arrayField,
        subFieldOptions,
        expressionContext,
      );
      if (condition && Object.keys(condition).length > 0) {
        conditions.push(condition);
      }
    }
  });

  if (conditions.length === 0) {
    return {};
  }

  let result;
  if (conditions.length === 1) {
    result = conditions[0];
  } else {
    const logic = (block.logic || "and").toLowerCase();
    if (logic === "or") {
      result = { $or: conditions };
    } else {
      result = { $and: conditions };
    }
  }

  // Handle custom blocks with isTrue === false (inverted logic) - only for inline blocks
  // If the block is defined as a variable (usage.count >= 2), this is already handled above
  if (block.customBlockName && block.isTrue === false) {
    const usage = dependencyGraph?.customBlockUsage?.get(block.customBlockName);
    // Only apply $nor if this block is NOT defined as a variable (used < 2 times)
    if (!usage || usage.count < 2) {
      // Wrap the result in $nor to invert the logic
      return { $nor: [result] };
    }
  }

  return result;
};

/**
 * Converts a condition to MongoDB expression
 */
const convertConditionToMongoExpr = (
  condition,
  dependencyGraph,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
  arrayField = null,
  subFieldOptions = null,
  expressionContext = false,
) => {
  const operator = condition.operator;
  // Normalize field to handle both string and object formats
  const field = normalizeFieldName(condition.field);
  const fieldType = condition.fieldType;
  const value = condition.value;
  const booleanSwitch = condition.booleanSwitch;

  // Special handling for MongoDB expression operators ($expr, $filter)
  // These operators wrap raw MongoDB expressions and should pass through as-is
  if (operator === "$expr" && value && typeof value === "object") {
    return { $expr: value };
  }
  if (operator === "$filter" && value && typeof value === "object") {
    // For $filter, if there's a field, treat it as the input
    // The value should contain the filter spec
    if (field && value.cond) {
      return {
        $expr: {
          $filter: {
            input: `$${field}`,
            cond: value.cond,
          },
        },
      };
    }
    // If value is already a complete $filter spec, pass it through in $expr
    if (value.input || value.cond) {
      return { $expr: value };
    }
  }

  if (!field || !operator) {
    return {};
  }

  // If fieldType is explicitly set, use it to determine field resolution
  if (fieldType) {
    let listVar, arithVar, switchVar;
    switch (fieldType) {
      case "listVariable":
        listVar = customListVariables.find((v) => v.name === field);
        if (listVar) {
          return convertListVariableCondition(
            listVar,
            operator,
            value,
            fieldOptions,
            customVariables,
            customListVariables,
            customSwitchCases,
            field,
            subFieldOptions,
            condition,
            dependencyGraph,
          );
        }
        break;
      case "variable":
        arithVar = customVariables.find((v) => v.name === field);
        if (arithVar) {
          return convertArithmeticVariableCondition(
            arithVar,
            operator,
            value,
            arrayField,
            customVariables,
          );
        }
        break;
      case "switchCase":
        switchVar = customSwitchCases.find((v) => v.name === field);
        if (switchVar) {
          return convertSwitchVariableCondition(switchVar, operator, value);
        }
        break;
      case "schema":
      default:
        return convertSchemaFieldCondition(
          field,
          operator,
          value,
          fieldOptions,
          customVariables,
          customListVariables,
          customSwitchCases,
          arrayField,
          subFieldOptions,
          expressionContext,
          condition,
        );
    }
  }

  // Check if field has metadata (new format: {name: "...", _meta: {...}})
  // Use metadata to determine exact field type when available (solves name collision issues)
  if (
    condition.field &&
    typeof condition.field === "object" &&
    condition.field._meta
  ) {
    const meta = condition.field._meta;

    // Route based on metadata flags - check in priority order
    if (meta.isSwitchCase) {
      const switchVar = customSwitchCases.find((v) => v.name === field);
      if (switchVar) {
        return convertSwitchVariableCondition(switchVar, operator, value);
      }
    }

    if (meta.isListVariable) {
      const listVar = customListVariables.find((v) => v.name === field);
      if (listVar) {
        return convertListVariableCondition(
          listVar,
          operator,
          value,
          fieldOptions,
          customVariables,
          customListVariables,
          customSwitchCases,
          field,
          subFieldOptions,
          condition,
          dependencyGraph,
        );
      }
    }

    if (meta.isVariable) {
      const arithVar = customVariables.find((v) => v.name === field);
      if (arithVar) {
        return convertArithmeticVariableCondition(
          arithVar,
          operator,
          value,
          arrayField,
          customVariables,
        );
      }
    }

    if (meta.isSchemaField) {
      return convertSchemaFieldCondition(
        field,
        operator,
        value,
        fieldOptions,
        customVariables,
        customListVariables,
        customSwitchCases,
        arrayField,
        subFieldOptions,
        expressionContext,
        condition,
      );
    }
  }

  // Fallback to legacy precedence-based resolution if fieldType is not set and no metadata
  // Handle list variables
  const listVar = customListVariables.find((v) => v.name === field);
  if (listVar) {
    return convertListVariableCondition(
      listVar,
      operator,
      value,
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
      field,
      null,
      condition,
      dependencyGraph,
    );
  }

  // Handle arithmetic variables
  const arithVar = customVariables.find((v) => v.name === field);
  if (arithVar) {
    return convertArithmeticVariableCondition(
      arithVar,
      operator,
      value,
      arrayField,
      customVariables,
    );
  }

  // Check if this is a schema field first (prefer regular fields over switch cases)
  const fieldDef = fieldOptions.find(
    (f) => f.value === field || f.label === field,
  );
  if (fieldDef) {
    return convertSchemaFieldCondition(
      field,
      operator,
      value,
      fieldOptions,
      customVariables,
      customListVariables,
      customSwitchCases,
      arrayField,
      subFieldOptions,
      expressionContext,
      condition,
    );
  }

  // Handle switch cases (only if not a schema field)
  const switchVar = customSwitchCases.find((v) => v.name === field);
  if (switchVar) {
    return convertSwitchVariableCondition(switchVar, operator, value);
  }

  // Fallback to schema field handling if nothing else matches
  return convertSchemaFieldCondition(
    field,
    operator,
    value,
    fieldOptions,
    customVariables,
    customListVariables,
    customSwitchCases,
    arrayField,
    subFieldOptions,
    expressionContext,
    condition,
  );
};

/**
 * Converts list variable condition. Different from list variable creation.
 */
const convertListVariableCondition = (
  listVar,
  operator,
  value,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
  field,
  subFieldOptions,
  condition,
  dependencyGraph = null,
) => {
  let compareValue = parseValueForComparison(
    value,
    customVariables,
    customSwitchCases,
    customListVariables,
    fieldOptions,
    null, // arrayField - not in array context for list variables
    subFieldOptions, // Pass subFieldOptions so subfield references can be resolved
  );

  // If value is undefined or empty string AND this is a boolean-type variable, check for boolean switch properties
  if (
    (compareValue === undefined ||
      (typeof compareValue === "string" && compareValue.trim() === "")) &&
    condition &&
    (listVar?.listCondition?.operator === "$anyElementTrue" ||
      listVar?.listCondition?.operator === "$allElementTrue")
  ) {
    compareValue = getBooleanSwitch(condition, value);
  }

  // For list variables with $anyElementTrue or $allElementTrue, handle boolean comparisons
  // Only convert to boolean for equality checks, not for numeric comparisons
  if (
    (listVar?.listCondition?.operator === "$anyElementTrue" ||
      listVar?.listCondition?.operator === "$allElementTrue") &&
    (operator === "$eq" || operator === "equals")
  ) {
    if (compareValue === "true") {
      compareValue = true;
    } else if (compareValue === "false") {
      compareValue = false;
    }
    // If compareValue is already a boolean (from getBooleanSwitch), leave it as-is
  }

  // Skip condition if value is null or empty string
  // For boolean variables, compareValue should be a boolean at this point
  // Don't skip if compareValue is a boolean (including false)
  if (
    compareValue === null ||
    (typeof compareValue === "string" && compareValue.trim() === "") ||
    (compareValue === undefined &&
      listVar?.listCondition?.operator !== "$anyElementTrue" &&
      listVar?.listCondition?.operator !== "$allElementTrue")
  ) {
    // Allow boolean false values through
    if (typeof compareValue !== "boolean") {
      return {};
    }
  }

  // Always reference the field name (it should be defined in $addFields)
  switch (operator) {
    case "$eq":
    case "equals":
      // For boolean values, use direct comparison without $eq
      if (typeof compareValue === "boolean") {
        return { [listVar.name]: compareValue };
      }
      return { [listVar.name]: { $eq: compareValue } };
    case "$ne":
    case "not equals":
      return { [listVar.name]: { $ne: compareValue } };
    case "$gt":
      return { [listVar.name]: { $gt: compareValue } };
    case "$gte":
      return { [listVar.name]: { $gte: compareValue } };
    case "$lt":
      return { [listVar.name]: { $lt: compareValue } };
    case "$lte":
      return { [listVar.name]: { $lte: compareValue } };
    case "$exists":
      return { [listVar.name]: { $exists: compareValue } };
    case "$lengthGt": {
      if (compareValue < 0) {
        return { [listVar.name]: { $exists: true } };
      }
      return { [`${listVar.name}.${compareValue}`]: { $exists: true } };
    }
    case "$lengthLt": {
      if (compareValue <= 0) {
        return { [`${listVar.name}.0`]: { $exists: false } };
      }
      return { [`${listVar.name}.${compareValue - 1}`]: { $exists: false } };
    }
    default:
      return { [listVar.name]: { $eq: compareValue } };
  }
};

/**
 * Converts switch variable condition
 */
const convertSwitchVariableCondition = (switchVar, operator, value) => {
  const compareValue =
    typeof value === "string" && !isNaN(value) ? parseFloat(value) : value;

  let result;
  switch (operator) {
    case "$eq":
    case "equals":
      result = { [switchVar.name]: { $eq: compareValue } };
      break;
    case "$ne":
    case "not equals":
      result = { [switchVar.name]: { $ne: compareValue } };
      break;
    case "$gt":
      result = { [switchVar.name]: { $gt: compareValue } };
      break;
    case "$gte":
      result = { [switchVar.name]: { $gte: compareValue } };
      break;
    case "$lt":
      result = { [switchVar.name]: { $lt: compareValue } };
      break;
    case "$lte":
      result = { [switchVar.name]: { $lte: compareValue } };
      break;
    case "$exists":
      result = { [switchVar.name]: { $exists: compareValue } };
      break;
    case "$lengthGt": {
      if (compareValue < 0) {
        result = { [switchVar.name]: { $exists: true } };
      } else {
        result = { [`${switchVar.name}.${compareValue}`]: { $exists: true } };
      }
      break;
    }
    case "$lengthLt": {
      if (compareValue <= 0) {
        result = { [`${switchVar.name}.0`]: { $exists: false } };
      } else {
        result = {
          [`${switchVar.name}.${compareValue - 1}`]: { $exists: false },
        };
      }
      break;
    }
    default:
      result = { [switchVar.name]: { $eq: compareValue } };
  }

  return result;
};

/**
 * Converts arithmetic variable condition
 */
const convertArithmeticVariableCondition = (
  arithVar,
  operator,
  value,
  arrayField = null,
  customVariables = [],
) => {
  // Skip condition if value is empty or invalid
  if (
    value === null ||
    value === undefined ||
    value === "" ||
    (typeof value === "string" && value.trim() === "")
  ) {
    return {};
  }

  const compareValue = !isNaN(value) ? parseFloat(value) : value;

  // Skip condition if compareValue is NaN (invalid number)
  if (typeof compareValue === "number" && isNaN(compareValue)) {
    return {};
  }

  try {
    let expr = convertArithmeticExpression(arithVar.variable, customVariables);
    if (!expr) throw new Error("Invalid expression");

    // Validate the converted expression doesn't contain null/undefined values
    const hasInvalidValues = (obj) => {
      if (obj === null || obj === undefined) return true;
      if (typeof obj === "object") {
        if (Array.isArray(obj)) {
          return obj.some(hasInvalidValues);
        }
        return Object.values(obj).some(hasInvalidValues);
      }
      return false;
    };

    if (hasInvalidValues(expr)) {
      throw new Error("Expression contains invalid null/undefined values");
    }

    // If arrayField is provided, replace $arrayField with $$this in the expression
    if (arrayField) {
      const replaceInExpr = (obj) => {
        if (typeof obj === "string") {
          return obj.replace(new RegExp(`\\$${arrayField}`, "g"), "$$$$this");
        } else if (Array.isArray(obj)) {
          return obj.map(replaceInExpr);
        } else if (typeof obj === "object" && obj !== null) {
          const newObj = {};
          for (const key in obj) {
            newObj[key] = replaceInExpr(obj[key]);
          }
          return newObj;
        }
        return obj;
      };
      expr = replaceInExpr(expr);

      // When in array context (anyElementTrue/allElementTrue), return expression directly without $expr
      // because we're already inside an aggregation expression context
      switch (operator) {
        case "$eq":
        case "equals":
          return { $eq: [expr, compareValue] };
        case "$ne":
        case "not equals":
          return { $ne: [expr, compareValue] };
        case "$gt":
          return { $gt: [expr, compareValue] };
        case "$gte":
          return { $gte: [expr, compareValue] };
        case "$lt":
          return { $lt: [expr, compareValue] };
        case "$lte":
          return { $lte: [expr, compareValue] };
        default:
          return { $eq: [expr, compareValue] };
      }
    }

    // For $match context, wrap in $expr
    switch (operator) {
      case "$eq":
      case "equals":
        return { $expr: { $eq: [expr, compareValue] } };
      case "$ne":
      case "not equals":
        return { $expr: { $ne: [expr, compareValue] } };
      case "$gt":
        return { $expr: { $gt: [expr, compareValue] } };
      case "$gte":
        return { $expr: { $gte: [expr, compareValue] } };
      case "$lt":
        return { $expr: { $lt: [expr, compareValue] } };
      case "$lte":
        return { $expr: { $lte: [expr, compareValue] } };
      default:
        return { $expr: { $eq: [expr, compareValue] } };
    }
  } catch (error) {
    // Fall back to the old way
    switch (operator) {
      case "$eq":
      case "equals":
        return { [arithVar.name]: { $eq: compareValue } };
      case "$ne":
      case "not equals":
        return { [arithVar.name]: { $ne: compareValue } };
      case "$gt":
        return { [arithVar.name]: { $gt: compareValue } };
      case "$gte":
        return { [arithVar.name]: { $gte: compareValue } };
      case "$lt":
        return { [arithVar.name]: { $lt: compareValue } };
      case "$lte":
        return { [arithVar.name]: { $lte: compareValue } };
      default:
        return { [arithVar.name]: { $eq: compareValue } };
    }
  }
};

/**
 * Converts schema field condition
 */
const convertSchemaFieldCondition = (
  field,
  operator,
  value,
  fieldOptions,
  customVariables,
  customListVariables,
  customSwitchCases,
  arrayField = null,
  subFieldOptions = null,
  expressionContext = false,
  condition = null,
) => {
  const fieldDef = fieldOptions.find(
    (f) => f.value === field || f.label === field,
  );
  const fieldType = fieldDef?.type || "string";

  const compareValue = parseValueForComparison(
    value,
    customVariables,
    customSwitchCases,
    customListVariables,
    fieldOptions,
    arrayField,
    subFieldOptions, // Pass subFieldOptions so subfieldreferences can be resolved
  );

  // Type-aware value parsing
  let processedValue = parseNumberIfNeeded(compareValue);

  // For string fields with eq/ne operators, ensure value is a string
  if (
    fieldType === "string" &&
    (operator === "$eq" ||
      operator === "equals" ||
      operator === "$ne" ||
      operator === "not equals")
  ) {
    if (typeof processedValue === "number") {
      processedValue = String(processedValue);
    }
  }

  // Check if processedValue is a MongoDB expression (object with operators like $add, $subtract, etc.)
  // If so, we need to use $expr to compare
  const isMongoExpression =
    processedValue &&
    typeof processedValue === "object" &&
    !Array.isArray(processedValue);

  // If arrayField is provided, we're in a $filter/$map context and need expression format with $$this
  if (arrayField) {
    // Strip the arrayField prefix from the field name if present
    // e.g., if field is "prv_candidates.isdiffpos" and arrayField is "prv_candidates"
    // we want "isdiffpos", not "prv_candidates.isdiffpos"
    let fieldReference = field;
    if (field.startsWith(`${arrayField}.`)) {
      fieldReference = field.substring(arrayField.length + 1);
    }

    switch (operator) {
      case "$eq":
      case "equals":
        return { $eq: [`$$this.${fieldReference}`, processedValue] };
      case "$ne":
      case "not equals":
        return { $ne: [`$$this.${fieldReference}`, processedValue] };
      case "$gt":
        return { $gt: [`$$this.${fieldReference}`, processedValue] };
      case "$gte":
        return { $gte: [`$$this.${fieldReference}`, processedValue] };
      case "$lt":
        return { $lt: [`$$this.${fieldReference}`, processedValue] };
      case "$lte":
        return { $lte: [`$$this.${fieldReference}`, processedValue] };
      case "$exists":
        return { $ne: [`$$this.${fieldReference}`, null] };
      case "$isNumber":
        return { $isNumber: `$$this.${fieldReference}` };
      case "$lengthGt":
      case "length >":
        return { $gt: [{ $size: `$$this.${fieldReference}` }, processedValue] };
      case "$lengthLt":
      case "length <":
        return { $lt: [{ $size: `$$this.${fieldReference}` }, processedValue] };
      default:
        return { $eq: [`$$this.${fieldReference}`, processedValue] };
    }
  }

  // If expressionContext is true (e.g., inside $switch), use aggregation expression syntax
  if (expressionContext) {
    switch (operator) {
      case "$eq":
      case "equals":
        return { $eq: [`$${field}`, processedValue] };
      case "$ne":
      case "not equals":
        return { $ne: [`$${field}`, processedValue] };
      case "$gt":
        return { $gt: [`$${field}`, processedValue] };
      case "$gte":
        return { $gte: [`$${field}`, processedValue] };
      case "$lt":
        return { $lt: [`$${field}`, processedValue] };
      case "$lte":
        return { $lte: [`$${field}`, processedValue] };
      case "$exists":
        return { $ne: [`$${field}`, null] };
      case "$isNumber":
        return { $isNumber: `$${field}` };
      case "$lengthGt":
      case "length >":
        return { $gt: [{ $size: `$${field}` }, processedValue] };
      case "$lengthLt":
      case "length <":
        return { $lt: [{ $size: `$${field}` }, processedValue] };
      default:
        return { $eq: [`$${field}`, processedValue] };
    }
  }

  // Regular $match context
  // If comparing to a MongoDB expression, use $expr
  if (isMongoExpression) {
    switch (operator) {
      case "$eq":
      case "equals":
        return { $expr: { $eq: [`$${field}`, processedValue] } };
      case "$ne":
      case "not equals":
        return { $expr: { $ne: [`$${field}`, processedValue] } };
      case "$gt":
        return { $expr: { $gt: [`$${field}`, processedValue] } };
      case "$gte":
        return { $expr: { $gte: [`$${field}`, processedValue] } };
      case "$lt":
        return { $expr: { $lt: [`$${field}`, processedValue] } };
      case "$lte":
        return { $expr: { $lte: [`$${field}`, processedValue] } };
      case "$exists":
        return { [field]: { $exists: true } };
      default:
        return { $expr: { $eq: [`$${field}`, processedValue] } };
    }
  }

  // Standard comparison operators with literal values
  switch (operator) {
    case "$eq":
    case "equals":
      return { [field]: { $eq: processedValue } };
    case "$ne":
    case "not equals":
      return { [field]: { $ne: processedValue } };
    case "$gt":
      return { [field]: { $gt: processedValue } };
    case "$gte":
      return { [field]: { $gte: processedValue } };
    case "$lt":
      return { [field]: { $lt: processedValue } };
    case "$lte":
      return { [field]: { $lte: processedValue } };
    case "$lengthGt":
    case "length >":
      return { $expr: { $gt: [{ $size: `$${field}` }, processedValue] } };
    case "$lengthLt":
    case "length <":
      return { $expr: { $lt: [{ $size: `$${field}` }, processedValue] } };
    case "$exists":
      return { [field]: { $exists: processedValue } };
    case "$isNumber":
    case "isNumber":
      // $isNumber requires $expr, so we need to wrap it
      return { $expr: { $isNumber: `$${field}` } };
    case "$anyElementTrue":
    case "$allElementTrue":
      // For boolean list variables treated as schema fields, use booleanSwitch to determine comparison value
      // This happens when list variable fields are referenced without isListVariable flag
      if (
        value === undefined ||
        value === null ||
        (typeof value === "string" && value.trim() === "")
      ) {
        // Get the boolean value from booleanSwitch or default to true
        const booleanValue = condition
          ? getBooleanSwitch(condition, value)
          : true;
        return { [field]: { $eq: booleanValue } };
      }
      return { [field]: { $eq: processedValue } };
    default:
      return { [field]: processedValue };
  }
};

/**
 * Parses value for comparison, handling field references
 */
const parseValueForComparison = (
  value,
  customVariables,
  customSwitchCases,
  customListVariables,
  fieldOptions,
  arrayField = null,
  subFieldOptions = null,
) => {
  // Handle object with metadata format: {name: "...", _meta: {...}}
  if (value && typeof value === "object" && value._meta && value.name) {
    const valueName = value.name;
    const meta = value._meta;

    // Route based on metadata to avoid name collisions
    if (meta.isSwitchCase) {
      return `$${valueName}`;
    }

    if (meta.isListVariable) {
      return `$${valueName}`;
    }

    if (meta.isVariable) {
      // Arithmetic variables need to be inlined
      const arithVar = customVariables.find((v) => v.name === valueName);
      if (arithVar) {
        let expr = convertArithmeticExpression(
          arithVar.variable,
          customVariables,
        );
        if (expr) {
          if (
            typeof expr === "string" &&
            expr.startsWith("$") &&
            !isNaN(expr.slice(1))
          ) {
            expr = parseFloat(expr.slice(1));
          }
          return expr;
        }
      }
      return valueName;
    }

    if (meta.isSchemaField) {
      // In array context ($filter/$map), use $$this for field references
      if (arrayField) {
        // Strip the arrayField prefix from valueName if present
        let fieldReference = valueName;
        if (valueName.startsWith(`${arrayField}.`)) {
          fieldReference = valueName.substring(arrayField.length + 1);
        }
        return `$$this.${fieldReference}`;
      }
      return `$${valueName}`;
    }

    // If metadata doesn't match any known type, treat as literal
    return valueName;
  }

  // Handle non-string values (numbers, booleans, objects without metadata)
  if (typeof value !== "string") {
    return value;
  }

  // Fallback: Legacy string-based resolution with precedence rules
  // Check for schema field references first (prefer over variables)
  const fieldRef = fieldOptions.find(
    (f) => f.value === value || f.label === value,
  );
  if (fieldRef) {
    // In array context ($filter/$map), use $$this for field references
    if (arrayField) {
      // Strip the arrayField prefix from value if present
      let fieldReference = value;
      if (value.startsWith(`${arrayField}.`)) {
        fieldReference = value.substring(arrayField.length + 1);
      }
      return `$$this.${fieldReference}`;
    }
    return `$${value}`;
  }

  // Check subFieldOptions (array element fields) - these are specific to list variable contexts
  if (subFieldOptions && Array.isArray(subFieldOptions)) {
    const subFieldRef = subFieldOptions.find(
      (f) => f.value === value || f.label === value,
    );
    if (subFieldRef) {
      // In array context ($filter/$map), use $$this for subfield references
      if (arrayField) {
        // Strip the arrayField prefix from value if present
        let fieldReference = value;
        if (value.startsWith(`${arrayField}.`)) {
          fieldReference = value.substring(arrayField.length + 1);
        }
        return `$$this.${fieldReference}`;
      }
      return `$${value}`;
    }
  }

  // Check for arithmetic variables - these need to be inlined, not referenced as fields
  const arithVar = customVariables.find((v) => v.name === value);
  if (arithVar) {
    // Convert the arithmetic expression and return the inlined expression
    let expr = convertArithmeticExpression(arithVar.variable, customVariables);
    if (expr) {
      // If the latex converter returned a string like "$20.2", it's treating a literal number as a field reference
      // We need to convert it back to a number
      if (
        typeof expr === "string" &&
        expr.startsWith("$") &&
        !isNaN(expr.slice(1))
      ) {
        expr = parseFloat(expr.slice(1));
      }
      return expr;
    }
    // If conversion fails, return as literal (shouldn't happen but safety fallback)
    return value;
  }

  // Check for list variable or switch case references (these exist as fields in the document)
  const listOrSwitchVar = [...customListVariables, ...customSwitchCases].find(
    (v) => v.name === value,
  );
  if (listOrSwitchVar) {
    return `$${value}`;
  }

  // Check for map subfields (e.g., "listVar.subfield")
  if (typeof value === "string" && value.includes(".")) {
    const [baseVar] = value.split(".");
    const listVar = customListVariables.find((v) => v.name === baseVar);
    if (listVar?.listCondition?.operator === "$map") {
      return `$${value}`;
    }
  }

  // Return as literal value
  return !isNaN(value) && !isNaN(parseFloat(value)) ? parseFloat(value) : value;
};

/**
 * Formats pipeline as pretty-printed JSON
 */
export const formatMongoAggregation = (pipeline) => {
  return JSON.stringify(pipeline, null, 2);
};

/**
 * Validates $anyelementtrue operator structure
 */
// const isValidAnyElementTrue = (value) => {
//   if (Array.isArray(value)) return true; // Simple case: array of booleans
//   if (typeof value === 'object' && value.$map) return true; // Complex case: $map expression
//   return false;
// };

/**
 * Validates $allelementtrue operator structure
 */
// const isValidAllElementTrue = (value) => {
//   if (Array.isArray(value)) return true; // Simple case: array of booleans
//   if (typeof value === 'object' && value.$map) return true; // Complex case: $map expression
//   return false;
// };

/**
 * Validates pipeline structure
 */
export const isValidPipeline = (pipeline) => {
  if (!Array.isArray(pipeline) || pipeline.length === 0) return false;

  const validateValue = (value, path = "") => {
    if (value === null || value === undefined) return false;
    if (
      typeof value === "string" ||
      typeof value === "number" ||
      typeof value === "boolean"
    )
      return true;
    if (Array.isArray(value)) {
      return value.every((item, index) =>
        validateValue(item, `${path}[${index}]`),
      );
    }
    if (typeof value === "object") {
      for (const key in value) {
        if (!key || key.trim() === "") return false; // Invalid empty key
        if (!validateValue(value[key], `${path}.${key}`)) return false;
      }
      // Special validation for operators
      if (value.$in !== undefined && !Array.isArray(value.$in)) return false;
      // $size can be either an integer (in match context) or an expression (string/object in aggregation context)
      if (
        value.$size !== undefined &&
        typeof value.$size === "number" &&
        (!Number.isInteger(value.$size) || value.$size < 0)
      )
        return false;
      if (value.$anyElementTrue !== undefined) {
        // $anyElementTrue must be either an array or an object (for $map)
        if (
          !Array.isArray(value.$anyElementTrue) &&
          typeof value.$anyElementTrue !== "object"
        )
          return false;
      }
      if (value.$allElementTrue !== undefined) {
        // $allElementTrue must be either an array or an object (for $map)
        if (
          !Array.isArray(value.$allElementTrue) &&
          typeof value.$allElementTrue !== "object"
        )
          return false;
      }
      // $ifNull operator validation
      if (value.$ifNull !== undefined) {
        // $ifNull must be an array with exactly 2 elements
        if (!Array.isArray(value.$ifNull) || value.$ifNull.length !== 2)
          return false;
      }
      // $switch operator validation
      if (value.$switch !== undefined) {
        const switchObj = value.$switch;
        if (typeof switchObj !== "object" || switchObj === null) return false;
        // branches is required and must be an array
        if (!Array.isArray(switchObj.branches)) return false;
        // each branch must have case and then properties
        for (const branch of switchObj.branches) {
          if (
            typeof branch !== "object" ||
            branch === null ||
            !("case" in branch) ||
            !("then" in branch)
          )
            return false;
        }
        // default is optional but if present must be valid
        if (
          "default" in switchObj &&
          !validateValue(switchObj.default, `${path}.$switch.default`)
        )
          return false;
      }
      // Aggregation operators ($min, $max, $avg, $sum) accept strings (field refs), arrays, or numbers
      // No additional validation needed - they're already covered by the general validation above
      // $median requires an object with input and method, which is also covered
      return true;
    }
    return false;
  };

  for (const stage of pipeline) {
    if (!stage || typeof stage !== "object") return false;
    const keys = Object.keys(stage);
    if (keys.length !== 1) return false;

    const stageType = keys[0];
    if (!stageType.startsWith("$")) return false;

    const stageValue = stage[stageType];
    if (!stageValue || typeof stageValue !== "object") return false;

    // Allow empty stage objects (they're no-ops but valid)
    // This is important for arithmetic and list variables where stages
    // might be empty if all variables at a level are filtered out
    if (Object.keys(stageValue).length === 0) continue;

    // Validate the stage content
    if (!validateValue(stageValue, stageType)) return false;
  }

  return true;
};

// Legacy export for backward compatibility
export function convertToMongoAggregation(
  filters,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
  additionalFieldsToProject = [],
) {
  return buildMongoAggregationPipeline(
    filters,
    schema,
    fieldOptions,
    customVariables,
    customListVariables,
    customSwitchCases,
    additionalFieldsToProject,
  );
}

// Export default
export default {
  buildMongoAggregationPipeline,
  convertToMongoAggregation,
  formatMongoAggregation,
  isValidPipeline,
};
