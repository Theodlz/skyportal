import { latexToMongoConverter } from "./robustLatexConverter.js";
import { getFieldType } from "./conditionHelpers.js";

export const convertToMongoAggregation = (
  filters,
  customVariables = [],
  customListVariables = [],
) => {
  if (!filters || filters.length === 0) {
    return [];
  }

  const pipeline = [];

  // Count variable usage in filters to determine optimization strategy
  const variableUsageCounts = countVariableUsage(filters, customVariables);
  const usedFields = getUsedFields(
    filters,
    customVariables,
    customListVariables,
  );

  // Add $project stage for field optimization and variable computation
  const hasMultiUseVariables = false; // Always inline arithmetic variables instead of projecting them

  // Check if we have any list operation variables that should be projected
  // Only project filter variables if they're actually used in the conditions
  const hasListOperationVariables = customListVariables.some(
    (listVar) =>
      listVar.listCondition &&
      ["$filter", "$min", "$max", "$avg", "$sum"].includes(
        listVar.listCondition.operator,
      ) &&
      usedFields.listVariables.includes(listVar.name),
  );

  // Collect custom blocks with isTrue: false for special handling
  const customBlocksWithFalseValue = [];

  // Recursive function to find all custom blocks with isTrue: false
  const findCustomBlocksWithFalse = (block) => {
    if (!block) return;

    // Check if this block is a custom block with isTrue: false
    if (block.customBlockName && block.isTrue === false) {
      customBlocksWithFalseValue.push({
        name: block.customBlockName,
        id: block.id,
        block: block,
      });
      // Continue to recurse into children to find nested custom blocks
    }

    // Recursively check children
    if (block.children && Array.isArray(block.children)) {
      block.children.forEach((child) => {
        if (child.category === "block" || child.type === "block") {
          findCustomBlocksWithFalse(child);
        }
      });
    }
  };

  // Find all custom blocks with isTrue: false at any nesting level
  filters.forEach((block) => {
    findCustomBlocksWithFalse(block);
  });

  // Check if we need a project stage for field optimization or custom blocks
  const needsProjectStage =
    hasMultiUseVariables ||
    hasListOperationVariables ||
    usedFields.baseFields.length > 0 ||
    customBlocksWithFalseValue.length > 0;

  if (needsProjectStage) {
    const projectStage = { $project: {} };

    // Include all used base fields
    usedFields.baseFields.forEach((field) => {
      projectStage.$project[field] = 1;
    });

    // Always include objectId unless explicitly excluded
    if (!usedFields.baseFields.includes("objectId")) {
      projectStage.$project.objectId = 1;
    }

    // Add custom blocks with isTrue: false as computed variables
    customBlocksWithFalseValue.forEach((customBlock) => {
      const variableName = `${customBlock.name.replace(/[^a-zA-Z0-9]/g, "_")}`;
      // Convert the custom block conditions to a MongoDB expression
      const blockCondition = convertBlockToMongoExpr(
        customBlock.block,
        customVariables,
        customListVariables,
        customBlocksWithFalseValue,
      );
      if (blockCondition && Object.keys(blockCondition).length > 0) {
        projectStage.$project[variableName] = blockCondition;
      }
    });

    // Add computed variables - Skip arithmetic variables since we always inline them
    // (Only add list operation variables that need projection)
    // customVariables.forEach(variable => {
    //   // Arithmetic variables are now always inlined instead of projected
    // });

    // Add list operation variables (only project those that are actually used)
    customListVariables.forEach((listVar) => {
      if (
        listVar.listCondition &&
        usedFields.listVariables.includes(listVar.name)
      ) {
        const listCondition = listVar.listCondition;
        const arrayField = listCondition.field;
        const operator = listCondition.operator;
        const subField = listCondition.subField;

        if (
          operator === "$filter" &&
          listCondition.value &&
          listCondition.value.children
        ) {
          // Convert nested conditions for the filter
          const filterCondition = convertBlockToMongo(
            listCondition.value,
            customVariables,
            customListVariables,
            true,
            variableUsageCounts,
            [],
          );
          projectStage.$project[listVar.name] = {
            $filter: {
              input: `$${arrayField}`,
              cond: filterCondition,
            },
          };
        } else if (
          ["$min", "$max", "$avg", "$sum"].includes(operator) &&
          subField
        ) {
          // Handle aggregation operators
          switch (operator) {
            case "$min":
              projectStage.$project[listVar.name] = {
                $min: `$${arrayField}.${subField}`,
              };
              break;
            case "$max":
              projectStage.$project[listVar.name] = {
                $max: `$${arrayField}.${subField}`,
              };
              break;
            case "$avg":
              projectStage.$project[listVar.name] = {
                $avg: `$${arrayField}.${subField}`,
              };
              break;
            case "$sum":
              projectStage.$project[listVar.name] = {
                $sum: `$${arrayField}.${subField}`,
              };
              break;
          }
        }
      }
    });

    // Add the project stage since we have variables to compute
    pipeline.push(projectStage);
  }

  // Add main match stage
  const matchStage = { $match: {} };

  // Add conditions for custom blocks with isTrue: false
  customBlocksWithFalseValue.forEach((customBlock) => {
    const variableName = `${customBlock.name.replace(/[^a-zA-Z0-9]/g, "_")}`;
    matchStage.$match[variableName] = false;
  });

  // Convert each root filter block (skip those with isTrue: false as they're handled above)
  filters.forEach((block) => {
    const blockCondition = convertBlockToMongo(
      block,
      customVariables,
      customListVariables,
      false,
      variableUsageCounts,
      customBlocksWithFalseValue,
    );
    if (blockCondition && Object.keys(blockCondition).length > 0) {
      // Merge conditions (assuming AND logic between root blocks)
      Object.assign(matchStage.$match, blockCondition);
    }
  });

  // Only add $match stage if there are actual conditions
  if (Object.keys(matchStage.$match).length > 0) {
    pipeline.push(matchStage);
  }

  return pipeline;
};

const convertBlockToMongo = (
  block,
  customVariables = [],
  customListVariables = [],
  isInArrayFilter = false,
  variableUsageCounts = {},
  customBlocksWithFalseValue = [],
) => {
  if (!block) {
    return {};
  }

  // Skip custom blocks with isTrue: false - they're handled in the project stage
  if (block.customBlockName && block.isTrue === false) {
    return {};
  }

  // Handle direct conditions (not wrapped in blocks)
  if (block.type === "condition" || block.category === "condition") {
    let condition;
    if (isInArrayFilter) {
      // For array filters, convert to $expr format
      condition = convertConditionToMongoExpr(
        block,
        customVariables,
        customListVariables,
      );
    } else {
      // For regular conditions, use standard format
      condition = convertConditionToMongo(
        block,
        customVariables,
        customListVariables,
        isInArrayFilter,
        variableUsageCounts,
      );
    }
    return condition || {};
  }

  // Handle blocks with children
  if (!block.children || block.children.length === 0) {
    return {};
  }

  const conditions = [];

  // Process each child (condition or nested block)
  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      // Recursively handle nested blocks
      const nestedCondition = convertBlockToMongo(
        child,
        customVariables,
        customListVariables,
        isInArrayFilter,
        variableUsageCounts,
        customBlocksWithFalseValue,
      );
      if (nestedCondition && Object.keys(nestedCondition).length > 0) {
        conditions.push(nestedCondition);
      }
    } else {
      // Handle individual conditions
      let condition;
      if (isInArrayFilter) {
        // For array filters, convert to $expr format
        condition = convertConditionToMongoExpr(
          child,
          customVariables,
          customListVariables,
        );
      } else {
        // For regular conditions, use standard format
        condition = convertConditionToMongo(
          child,
          customVariables,
          customListVariables,
          isInArrayFilter,
          variableUsageCounts,
        );
      }
      if (condition && Object.keys(condition).length > 0) {
        conditions.push(condition);
      }
    }
  });

  if (conditions.length === 0) {
    return {};
  }

  if (conditions.length === 1) {
    return conditions[0];
  }

  // Combine conditions based on block logic
  const logic = block.logic || "And";
  if (logic === "Or") {
    return { $or: conditions };
  } else {
    return { $and: conditions };
  }
};

const convertConditionToMongo = (
  condition,
  customVariables = [],
  customListVariables = [],
  isInArrayFilter = false,
  variableUsageCounts = {},
) => {
  if (!condition.field || !condition.operator) {
    return {};
  }

  const operator = condition.operator;
  const value = condition.value;

  // Check if this field name references a saved list condition variable
  const listVariable = customListVariables.find(
    (lv) => lv.name === condition.field,
  );
  if (listVariable && listVariable.listCondition) {
    // This field references a saved list condition variable
    const listCondition = listVariable.listCondition;

    // For aggregation operators that are now projected, treat them as regular fields
    if (["$min", "$max", "$avg", "$sum"].includes(listCondition.operator)) {
      // The variable is now projected, so we can treat it as a regular field
      // and apply optimized operators like $lengthGt/$lengthLt if applicable
      const field = condition.field; // Use the variable name directly

      // Apply the same operator logic as regular fields
      switch (operator) {
        case "$lengthGt": {
          const gtLength = parseNumberIfNeeded(value);
          if (gtLength < 0) {
            return { [field]: { $exists: true } };
          }
          // For projected array results, we still use $exists optimization
          return { [`${field}.${gtLength}`]: { $exists: true } };
        }
        case "$lengthLt": {
          const ltLength = parseNumberIfNeeded(value);
          if (ltLength <= 0) {
            return { [`${field}.0`]: { $exists: false } };
          }
          return { [`${field}.${ltLength - 1}`]: { $exists: false } };
        }
        case "$exists":
        case "exists":
          return { [field]: { $exists: value !== false } };
        case "not exists":
          return { [field]: { $exists: false } };
        default: {
          // For comparison operators, compare against the projected value
          const compareValue = parseNumberIfNeeded(value);
          switch (operator) {
            case "$gt":
            case "greater than":
            case ">":
              return { [field]: { $gt: compareValue } };
            case "$gte":
            case "greater than or equal":
            case ">=":
              return { [field]: { $gte: compareValue } };
            case "$lt":
            case "less than":
            case "<":
              return { [field]: { $lt: compareValue } };
            case "$lte":
            case "less than or equal":
            case "<=":
              return { [field]: { $lte: compareValue } };
            case "$ne":
            case "not equals":
            case "!=":
              return { [field]: { $ne: compareValue } };
            case "$eq":
            case "equals":
            case "=":
            default:
              return { [field]: { $eq: compareValue } };
          }
        }
      }
    }

    // For $filter operations, the result is an array so we can also apply length optimizations
    if (listCondition.operator === "$filter") {
      const field = condition.field; // Use the variable name directly

      switch (operator) {
        case "$lengthGt": {
          const gtLength = parseNumberIfNeeded(value);
          if (gtLength < 0) {
            return { [field]: { $exists: true } };
          }
          return { [`${field}.${gtLength}`]: { $exists: true } };
        }
        case "$lengthLt": {
          const ltLength = parseNumberIfNeeded(value);
          if (ltLength <= 0) {
            return { [`${field}.0`]: { $exists: false } };
          }
          return { [`${field}.${ltLength - 1}`]: { $exists: false } };
        }
        case "$exists":
        case "exists":
          return { [field]: { $exists: value !== false } };
        case "not exists":
          return { [field]: { $exists: false } };
        default: {
          // For other operators on $filter results, check if any elements match
          return {
            $expr: {
              $gt: [{ $size: { $ifNull: [`$${field}`, []] } }, 0],
            },
          };
        }
      }
    }

    // For other list operations, use the original logic but pass the boolean switch from the condition
    const listConditionWithBooleanSwitch = {
      ...listVariable.listCondition,
      // Override the booleanSwitch with the one from the condition (if present)
      booleanSwitch:
        condition.booleanSwitch !== undefined
          ? condition.booleanSwitch
          : listVariable.listCondition.booleanSwitch !== undefined
            ? listVariable.listCondition.booleanSwitch
            : true,
    };
    return convertListConditionToMongo(
      listConditionWithBooleanSwitch,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  // Handle list condition structures
  if (
    condition.isListVariable ||
    (value && typeof value === "object" && value.type === "array") ||
    [
      "$anyElementTrue",
      "$allElementsTrue",
      "$filter",
      "$min",
      "$max",
      "$avg",
      "$sum",
    ].includes(operator)
  ) {
    return convertListConditionToMongo(
      condition,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  // Get field path - might be a string or a MongoDB expression for single-use variables
  const fieldPath = getFieldPath(
    condition.field,
    customVariables,
    isInArrayFilter,
  );

  // If fieldPath is a MongoDB expression (object), wrap it in $expr
  if (typeof fieldPath === "object" && fieldPath !== null) {
    // This is a single-use variable that was inlined as a MongoDB expression
    return handleInlinedVariableCondition(fieldPath, operator, value);
  }

  // Regular field path handling
  const field = fieldPath;

  // Handle different operator types
  switch (operator) {
    case "$eq":
    case "equals":
    case "=": {
      // Check if this is a boolean field or boolean value
      const fieldType = getFieldType(
        condition.field,
        customVariables,
        [],
        customListVariables,
      );
      if (fieldType === "boolean" || typeof value === "boolean") {
        // Use $in operator for boolean fields/values
        return { [field]: { $in: [parseNumberIfNeeded(value)] } };
      }
      return { [field]: { $eq: parseNumberIfNeeded(value) } };
    }

    case "$ne":
    case "not equals":
    case "!=": {
      // Check if this is a boolean field or boolean value
      const fieldType = getFieldType(
        condition.field,
        customVariables,
        [],
        customListVariables,
      );
      if (fieldType === "boolean" || typeof value === "boolean") {
        // Use $nin operator for boolean fields/values
        return { [field]: { $nin: [parseNumberIfNeeded(value)] } };
      }
      return { [field]: { $ne: parseNumberIfNeeded(value) } };
    }

    case "$gt":
    case "greater than":
    case ">":
      return { [field]: { $gt: parseNumberIfNeeded(value) } };

    case "$gte":
    case "greater than or equal":
    case ">=":
      return { [field]: { $gte: parseNumberIfNeeded(value) } };

    case "$lt":
    case "less than":
    case "<":
      return { [field]: { $lt: parseNumberIfNeeded(value) } };

    case "$lte":
    case "less than or equal":
    case "<=":
      return { [field]: { $lte: parseNumberIfNeeded(value) } };

    case "$regex":
    case "contains":
    case "like":
      return { [field]: { $regex: value, $options: "i" } };

    case "starts with":
      return { [field]: { $regex: `^${escapeRegex(value)}`, $options: "i" } };

    case "ends with":
      return { [field]: { $regex: `${escapeRegex(value)}$`, $options: "i" } };

    case "$in":
    case "in":
      return { [field]: { $in: Array.isArray(value) ? value : [value] } };

    case "$nin":
    case "not in":
      return { [field]: { $nin: Array.isArray(value) ? value : [value] } };

    case "$exists":
    case "exists":
      return { [field]: { $exists: value !== false } };

    case "not exists":
      return { [field]: { $exists: false } };

    case "$isNumber":
      // Use the literal MongoDB $isNumber operator
      return { $isNumber: `${field}` };

    case "between":
      if (Array.isArray(value) && value.length === 2) {
        return {
          [field]: {
            $gte: parseNumberIfNeeded(value[0]),
            $lte: parseNumberIfNeeded(value[1]),
          },
        };
      }
      return {};

    case "not between":
      if (Array.isArray(value) && value.length === 2) {
        return {
          $or: [
            { [field]: { $lt: parseNumberIfNeeded(value[0]) } },
            { [field]: { $gt: parseNumberIfNeeded(value[1]) } },
          ],
        };
      }
      return {};

    case "array contains":
      return { [field]: { $elemMatch: { $eq: value } } };

    case "$lengthGt": {
      // Optimize for performance: use $exists to check if array has elements at specific indices
      const gtLength = parseNumberIfNeeded(value);
      if (gtLength < 0) {
        // length > negative number is always true (arrays can't have negative length)
        // Return a condition that always matches
        return { [field]: { $exists: true } };
      }
      // Check if element exists at index gtLength (array has more than gtLength elements)
      return { [`${field}.${gtLength}`]: { $exists: true } };
    }

    case "$lengthLt": {
      // Optimize for performance: use $exists to check array length
      const ltLength = parseNumberIfNeeded(value);
      if (ltLength <= 0) {
        // length < 0 is always false (arrays can't have negative length)
        // length < 0 or length < 1 both mean array should be empty
        return { [`${field}.0`]: { $exists: false } };
      }
      // Check that element does NOT exist at index (ltLength - 1)
      return { [`${field}.${ltLength - 1}`]: { $exists: false } };
    }

    case "array length":
      return { [field]: { $size: parseNumberIfNeeded(value) } };

    case "array empty":
      return { [field]: { $size: 0 } };

    case "array not empty":
      return { [field]: { $not: { $size: 0 } } };

    // Array operators that are now handled by convertListConditionToMongo
    case "$anyElementTrue":
    case "$allElementsTrue":
    case "$filter":
    case "$min":
    case "$max":
    case "$avg":
    case "$sum":
      return convertArrayOperatorToMongo(field, operator, value);

    default:
      // Fallback for unknown operators - but now parse numbers correctly
      return { [field]: parseNumberIfNeeded(value) };
  }
};

// Helper function to convert blocks to MongoDB $expr format (for use in project stage)
const convertBlockToMongoExpr = (
  block,
  customVariables = [],
  customListVariables = [],
  customBlocksWithFalseValue = [],
) => {
  if (!block) {
    return {};
  }

  // Handle direct conditions (not wrapped in blocks)
  if (block.type === "condition" || block.category === "condition") {
    return convertConditionToProjectExpr(
      block,
      customVariables,
      customListVariables,
    );
  }

  // Handle blocks with children
  if (!block.children || block.children.length === 0) {
    return {};
  }

  const conditions = [];

  // Process each child (condition or nested blocks)
  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      // Skip custom blocks that have isTrue: false (they are handled separately)
      if (
        child.customBlockName &&
        customBlocksWithFalseValue.some((block) => block.id === child.id)
      ) {
        return; // Skip this custom block
      }

      // Recursively handle nested blocks
      const nestedCondition = convertBlockToMongoExpr(
        child,
        customVariables,
        customListVariables,
        customBlocksWithFalseValue,
      );
      if (nestedCondition && Object.keys(nestedCondition).length > 0) {
        conditions.push(nestedCondition);
      }
    } else {
      // Handle individual conditions
      const condition = convertConditionToProjectExpr(
        child,
        customVariables,
        customListVariables,
      );
      if (condition && Object.keys(condition).length > 0) {
        conditions.push(condition);
      }
    }
  });

  if (conditions.length === 0) {
    return {};
  }

  if (conditions.length === 1) {
    return conditions[0];
  }

  // Combine conditions based on block logic using $expr operators
  const logic = block.logic || "And";
  if (logic === "Or") {
    return { $or: conditions };
  } else {
    return { $and: conditions };
  }
};

// Helper function to convert conditions to MongoDB $expr format for project stage
const convertConditionToProjectExpr = (
  condition,
  customVariables = [],
  customListVariables = [],
) => {
  if (!condition.field || !condition.operator) {
    return {};
  }

  const operator = condition.operator;
  const value = condition.value;
  const field = condition.field;

  // Get the boolean switch from various possible properties
  const getBooleanSwitch = (condition, value) => {
    // For simple boolean values, use the value itself
    if (typeof value === "boolean") {
      return value;
    }

    // For complex conditions with nested children, check for boolean switch properties
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
                  : true; // Default to true (no negation)
    }

    return true; // Default to true for other cases
  };

  // Get field path for project stage (using $fieldName)
  const fieldPath = `$${field}`;

  // Convert to $expr format for project stage
  switch (operator) {
    case "$eq":
    case "equals":
    case "=":
      return { $eq: [fieldPath, parseNumberIfNeeded(value)] };

    case "$ne":
    case "not equals":
    case "!=":
      return { $ne: [fieldPath, parseNumberIfNeeded(value)] };

    case "$gt":
    case "greater than":
    case ">":
      return { $gt: [fieldPath, parseNumberIfNeeded(value)] };

    case "$gte":
    case "greater than or equal":
    case ">=":
      return { $gte: [fieldPath, parseNumberIfNeeded(value)] };

    case "$lt":
    case "less than":
    case "<":
      return { $lt: [fieldPath, parseNumberIfNeeded(value)] };

    case "$lte":
    case "less than or equal":
    case "<=":
      return { $lte: [fieldPath, parseNumberIfNeeded(value)] };

    case "$in":
    case "in":
      return { $in: [fieldPath, Array.isArray(value) ? value : [value]] };

    case "$nin":
    case "not in":
      return {
        $not: { $in: [fieldPath, Array.isArray(value) ? value : [value]] },
      };

    case "$exists":
    case "exists":
      return value !== false
        ? { $ne: [fieldPath, null] }
        : { $eq: [fieldPath, null] };

    case "not exists":
      return { $eq: [fieldPath, null] };

    case "$anyElementTrue":
      // Check if we have nested conditions (value is a block object)
      if (value && typeof value === "object" && value.children) {
        // Convert the nested conditions to a proper condition for $anyElementTrue using array context
        const conditionsForMap = convertBlockToMongo(
          value,
          customVariables,
          customListVariables,
          true,
          {},
          [],
        );
        if (conditionsForMap && Object.keys(conditionsForMap).length > 0) {
          const anyElementExpr = {
            $anyElementTrue: {
              $map: {
                input: { $ifNull: [fieldPath, []] },
                in: conditionsForMap,
              },
            },
          };

          // Apply $not if boolean switch is false
          return getBooleanSwitch(condition, value)
            ? anyElementExpr
            : { $not: anyElementExpr };
        }
      }
      // For simple boolean values, apply $not when false
      if (typeof value === "boolean") {
        if (value) {
          // Normal $anyElementTrue behavior
          return { $anyElementTrue: { $ifNull: [fieldPath, []] } };
        } else {
          // Apply $not to $anyElementTrue when boolean switch is false
          return {
            $not: {
              $anyElementTrue: { $ifNull: [fieldPath, []] },
            },
          };
        }
      }
      // For non-boolean values or undefined, use $anyElementTrue properly
      if (value !== undefined && value !== null && value !== "") {
        // Use proper $anyElementTrue for specific value matching
        const anyElementExpr = {
          $anyElementTrue: {
            $map: {
              input: { $ifNull: [fieldPath, []] },
              in: { $eq: ["$$this", value] },
            },
          },
        };

        // Apply $not based on boolean switch
        return getBooleanSwitch(condition, value)
          ? anyElementExpr
          : { $not: anyElementExpr };
      }

      return getBooleanSwitch(condition, value)
        ? { $anyElementTrue: { $ifNull: [fieldPath, []] } }
        : { $not: { $anyElementTrue: { $ifNull: [fieldPath, []] } } };

    case "$allElementsTrue":
      // Check if we have nested conditions (value is a block object)
      if (value && typeof value === "object" && value.children) {
        // Convert the nested conditions to a proper condition for $allElementsTrue using array context
        const conditionsForMap = convertBlockToMongo(
          value,
          customVariables,
          customListVariables,
          true,
          {},
          [],
        );
        if (conditionsForMap && Object.keys(conditionsForMap).length > 0) {
          const allElementExpr = {
            $allElementsTrue: {
              $map: {
                input: { $ifNull: [fieldPath, []] },
                in: conditionsForMap,
              },
            },
          };

          // Apply $not if boolean switch is false
          return getBooleanSwitch(condition, value)
            ? allElementExpr
            : { $not: allElementExpr };
        }
      }
      // For simple boolean values, apply $not when false
      if (typeof value === "boolean") {
        if (value) {
          // Normal $allElementsTrue behavior
          return { $allElementsTrue: { $ifNull: [fieldPath, []] } };
        } else {
          // Apply $not to $allElementsTrue when boolean switch is false
          return {
            $not: {
              $allElementsTrue: { $ifNull: [fieldPath, []] },
            },
          };
        }
      }
      // For non-boolean values, use proper $allElementsTrue
      if (value !== undefined && value !== null && value !== "") {
        const allElementExpr = {
          $allElementsTrue: {
            $map: {
              input: { $ifNull: [fieldPath, []] },
              in: { $eq: ["$$this", value] },
            },
          },
        };

        // Apply $not based on boolean switch
        return getBooleanSwitch(condition, value)
          ? allElementExpr
          : { $not: allElementExpr };
      }

      return getBooleanSwitch(condition, value)
        ? { $allElementsTrue: { $ifNull: [fieldPath, []] } }
        : { $not: { $allElementsTrue: { $ifNull: [fieldPath, []] } } };

    case "$filter":
      // Check if we have nested conditions (value is a block object)
      if (value && typeof value === "object" && value.children) {
        // Convert the nested conditions to a proper condition for $filter using array context
        const filterCondition = convertBlockToMongo(
          value,
          customVariables,
          customListVariables,
          true,
          {},
          [],
        );
        if (filterCondition && Object.keys(filterCondition).length > 0) {
          return {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: { $ifNull: [fieldPath, []] },
                    cond: filterCondition,
                  },
                },
              },
              0,
            ],
          };
        }
      }
      // Filter requires a condition - if we just have a simple value, check if any element equals it
      if (
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
      ) {
        return {
          $gt: [
            {
              $size: {
                $filter: {
                  input: { $ifNull: [fieldPath, []] },
                  cond: { $eq: ["$$this", value] },
                },
              },
            },
            0,
          ],
        };
      }
      // For complex filter conditions, we need more structure - fallback to existence check
      return { $ne: [fieldPath, null] };

    default:
      // Fallback to equality
      return { $eq: [fieldPath, parseNumberIfNeeded(value)] };
  }
};

// Helper function to convert conditions to MongoDB $expr format (for use in $map operations)
const convertConditionToMongoExpr = (
  condition,
  customVariables = [],
  customListVariables = [],
) => {
  if (!condition.field || !condition.operator) {
    return {};
  }

  const operator = condition.operator;
  const value = condition.value;
  const field = condition.field;

  // Check if this field is a custom variable with a LaTeX expression
  const customVar = customVariables.find((v) => v.name === field);
  let fieldPath;

  if (customVar && customVar.variable) {
    // Always inline arithmetic variables in array context
    const eqParts = customVar.variable.split("=");
    if (eqParts.length === 2) {
      const latexExpression = eqParts[1].trim();
      // Get the MongoDB expression and convert it to array context
      const mongoExpression =
        latexToMongoConverter.convertToMongo(latexExpression);
      fieldPath = convertToArrayContext(mongoExpression);
    } else {
      // Fallback to field path if expression parsing fails
      fieldPath = `$$this.${field}`;
    }
  } else {
    // Regular field path for array element context (using $$this)
    fieldPath = `$$this.${field}`;
  }

  // Check if the field is a boolean field
  const fieldType = getFieldType(
    field,
    customVariables,
    [],
    customListVariables,
  );
  const isBooleanField = fieldType === "boolean";
  const isBooleanValue = typeof value === "boolean";

  // If fieldPath is a MongoDB expression (object), we need to wrap the comparison in $expr
  if (typeof fieldPath === "object" && fieldPath !== null) {
    // This is an inlined variable expression, wrap the comparison in $expr
    switch (operator) {
      case "$eq":
      case "equals":
      case "=": {
        if (isBooleanField || isBooleanValue) {
          // Use $eq for single boolean values, $in for arrays
          if (Array.isArray(value)) {
            return { $in: [fieldPath, value] };
          }
          return { $eq: [fieldPath, value] };
        }
        return { $eq: [fieldPath, parseNumberIfNeeded(value)] };
      }

      case "$ne":
      case "not equals":
      case "!=": {
        if (isBooleanField || isBooleanValue) {
          // Use $ne for single boolean values, $nin for arrays
          if (Array.isArray(value)) {
            return { $nin: [fieldPath, value] };
          }
          return { $ne: [fieldPath, value] };
        }
        return { $ne: [fieldPath, parseNumberIfNeeded(value)] };
      }

      case "$gt":
      case "greater than":
      case ">":
        return { $gt: [fieldPath, parseNumberIfNeeded(value)] };

      case "$gte":
      case "greater than or equal":
      case ">=":
        return { $gte: [fieldPath, parseNumberIfNeeded(value)] };

      case "$lt":
      case "less than":
      case "<":
        return { $lt: [fieldPath, parseNumberIfNeeded(value)] };

      case "$lte":
      case "less than or equal":
      case "<=":
        return { $lte: [fieldPath, parseNumberIfNeeded(value)] };

      case "$in":
      case "in":
        return { $in: [fieldPath, Array.isArray(value) ? value : [value]] };

      case "$nin":
      case "not in":
        return { $nin: [fieldPath, Array.isArray(value) ? value : [value]] };

      case "between":
        if (Array.isArray(value) && value.length === 2) {
          return {
            $and: [
              { $gte: [fieldPath, parseNumberIfNeeded(value[0])] },
              { $lte: [fieldPath, parseNumberIfNeeded(value[1])] },
            ],
          };
        }
        return {};

      case "not between":
        if (Array.isArray(value) && value.length === 2) {
          return {
            $or: [
              { $lt: [fieldPath, parseNumberIfNeeded(value[0])] },
              { $gt: [fieldPath, parseNumberIfNeeded(value[1])] },
            ],
          };
        }
        return {};

      default:
        return { $eq: [fieldPath, parseNumberIfNeeded(value)] };
    }
  }

  // Regular field path (string), convert to $expr format for array context
  switch (operator) {
    case "$eq":
    case "equals":
    case "=": {
      // For boolean fields or boolean values in array context, use $eq for single values
      if (isBooleanField || isBooleanValue) {
        // Use $eq for single boolean values, $in for arrays
        if (Array.isArray(value)) {
          return { $in: [fieldPath, value] };
        }
        return { $eq: [fieldPath, value] };
      }
      return { $eq: [fieldPath, parseNumberIfNeeded(value)] };
    }

    case "$ne":
    case "not equals":
    case "!=": {
      // For boolean fields or boolean values in array context, use $ne for single values
      if (isBooleanField || isBooleanValue) {
        // Use $ne for single boolean values, $nin for arrays
        if (Array.isArray(value)) {
          return { $nin: [fieldPath, value] };
        }
        return { $ne: [fieldPath, value] };
      }
      return { $ne: [fieldPath, parseNumberIfNeeded(value)] };
    }

    case "$gt":
    case "greater than":
    case ">":
      return { $gt: [fieldPath, parseNumberIfNeeded(value)] };

    case "$gte":
    case "greater than or equal":
    case ">=":
      return { $gte: [fieldPath, parseNumberIfNeeded(value)] };

    case "$lt":
    case "less than":
    case "<":
      return { $lt: [fieldPath, parseNumberIfNeeded(value)] };

    case "$lte":
    case "less than or equal":
    case "<=":
      return { $lte: [fieldPath, parseNumberIfNeeded(value)] };

    case "$in":
    case "in": {
      // For boolean fields in $expr context, use $in directly
      if (isBooleanField || isBooleanValue) {
        return { $in: [fieldPath, Array.isArray(value) ? value : [value]] };
      }
      return { $in: [fieldPath, Array.isArray(value) ? value : [value]] };
    }

    case "$nin":
    case "not in": {
      // For boolean fields in $expr context, use $nin directly
      if (isBooleanField || isBooleanValue) {
        return { $nin: [fieldPath, Array.isArray(value) ? value : [value]] };
      }
      return {
        $not: { $in: [fieldPath, Array.isArray(value) ? value : [value]] },
      };
    }

    case "$exists":
    case "$isNumber":
      // Use the literal MongoDB $isNumber operator
      return { $isNumber: `${fieldPath}` };
    case "exists":
      if (value !== false) {
        return { $ne: [fieldPath, null] };
      } else {
        return { $eq: [fieldPath, null] };
      }

    case "not exists":
      return { $eq: [fieldPath, null] };

    case "$regex":
    case "contains":
    case "like":
      return { $regexMatch: { input: fieldPath, regex: value, options: "i" } };

    case "starts with":
      return {
        $regexMatch: {
          input: fieldPath,
          regex: `^${escapeRegex(value)}`,
          options: "i",
        },
      };

    case "ends with":
      return {
        $regexMatch: {
          input: fieldPath,
          regex: `${escapeRegex(value)}$`,
          options: "i",
        },
      };

    case "between":
      if (Array.isArray(value) && value.length === 2) {
        return {
          $and: [
            { $gte: [fieldPath, parseNumberIfNeeded(value[0])] },
            { $lte: [fieldPath, parseNumberIfNeeded(value[1])] },
          ],
        };
      }
      return {};

    case "not between":
      if (Array.isArray(value) && value.length === 2) {
        return {
          $or: [
            { $lt: [fieldPath, parseNumberIfNeeded(value[0])] },
            { $gt: [fieldPath, parseNumberIfNeeded(value[1])] },
          ],
        };
      }
      return {};

    default:
      // Fallback to equality
      return { $eq: [fieldPath, parseNumberIfNeeded(value)] };
  }
};

const getFieldPath = (field, customVariables = [], isInArrayFilter = false) => {
  // Handle different field formats
  if (typeof field === "string") {
    // Check if this field is a custom variable with a LaTeX expression
    const customVar = customVariables.find((v) => v.name === field);
    if (customVar && customVar.variable) {
      // Always inline arithmetic variables instead of referencing projected variables
      const eqParts = customVar.variable.split("=");
      if (eqParts.length === 2) {
        const latexExpression = eqParts[1].trim();
        // Convert to MongoDB expression with array context awareness
        return latexToMongoConverter.convertToMongo(
          latexExpression,
          isInArrayFilter,
        );
      }
      // Fallback to field name if expression parsing fails
      return isInArrayFilter ? `$$this.${field}` : field;
    }
    return isInArrayFilter ? `$$this.${field}` : field;
  }

  if (field && typeof field === "object") {
    const fieldPath = field.value || field.name || field.field || String(field);
    // Check if this is a custom variable
    const customVar = customVariables.find((v) => v.name === fieldPath);
    if (customVar && customVar.variable) {
      // Always inline arithmetic variables
      const eqParts = customVar.variable.split("=");
      if (eqParts.length === 2) {
        const latexExpression = eqParts[1].trim();
        return latexToMongoConverter.convertToMongo(
          latexExpression,
          isInArrayFilter,
        );
      }
      return isInArrayFilter ? `$$this.${fieldPath}` : fieldPath;
    }
    return isInArrayFilter ? `$$this.${fieldPath}` : fieldPath;
  }

  const fieldPath = String(field);
  // Check if this is a custom variable
  const customVar = customVariables.find((v) => v.name === fieldPath);
  if (customVar && customVar.variable) {
    // Always inline arithmetic variables
    const eqParts = customVar.variable.split("=");
    if (eqParts.length === 2) {
      const latexExpression = eqParts[1].trim();
      return latexToMongoConverter.convertToMongo(
        latexExpression,
        isInArrayFilter,
      );
    }
    return isInArrayFilter ? `$$this.${fieldPath}` : fieldPath;
  }
  return isInArrayFilter ? `$$this.${fieldPath}` : fieldPath;
};

const parseNumberIfNeeded = (value) => {
  // Try to parse as number if it looks like one
  if (typeof value === "string" && !isNaN(value) && !isNaN(parseFloat(value))) {
    return parseFloat(value);
  }
  return value;
};

const escapeRegex = (string) => {
  return string.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
};

// Export formatted JSON for display
export const formatMongoAggregation = (pipeline) => {
  return JSON.stringify(pipeline, null, 2);
};

// Validate if pipeline has meaningful content
// Helper function to validate MongoDB query expressions
const isValidMongoExpression = (expression) => {
  if (expression === null || expression === undefined) {
    return false;
  }

  // // Primitive values are valid
  if (typeof expression !== "object") {
    return true;
  }

  // Arrays are valid if all elements are valid
  if (Array.isArray(expression)) {
    return expression.every((item) => isValidMongoExpression(item));
  }

  // Objects should not be empty unless they're specific operators
  const keys = Object.keys(expression);
  if (keys.length === 0) {
    return false;
  }

  // Check for common invalid patterns
  for (const [key, value] of Object.entries(expression)) {
    // Field names should not be empty strings
    if (key === "") {
      return false;
    }

    // Recursively validate nested expressions
    if (!isValidMongoExpression(value)) {
      return false;
    }

    // Check for operators that require specific value types
    if (key.startsWith("$")) {
      switch (key) {
        case "$in":
        case "$nin":
          if (!Array.isArray(value)) {
            return false;
          }
          break;
        case "$size":
          if (
            typeof value !== "number" ||
            value < 0 ||
            !Number.isInteger(value)
          ) {
            return false;
          }
          break;
        case "$gt":
        case "$gte":
        case "$lt":
        case "$lte":
          // if none of the elements are numbers, it's invalid
          if (Array.isArray(value)) {
            if (
              !value.some(
                (v) =>
                  typeof v === "number" ||
                  typeof v === "string" ||
                  v instanceof Date,
              )
            ) {
              return false;
            }
          }
          break;
      }
    }
  }

  return true;
};

export const isValidPipeline = (pipeline) => {
  // Basic array validation
  if (!Array.isArray(pipeline)) {
    return false;
  }

  // Empty pipeline is valid (represents no filters)
  if (pipeline.length === 0) {
    return false;
  }

  // Validate each stage in the pipeline
  for (const stage of pipeline) {
    // Each stage must be an object
    if (!stage || typeof stage !== "object") {
      return false;
    }

    // Each stage must have exactly one top-level key (the stage operator)
    const stageKeys = Object.keys(stage);
    if (stageKeys.length !== 1) {
      return false;
    }

    const stageType = stageKeys[0];
    const stageContent = stage[stageType];

    // Validate common stage types
    switch (stageType) {
      case "$match":
        // $match stage content must be an object
        if (!stageContent || typeof stageContent !== "object") {
          return false;
        }
        // $match cannot be empty object (would match everything)
        if (Object.keys(stageContent).length === 0) {
          return false;
        }
        // Validate each condition in the $match stage
        if (!isValidMongoExpression(stageContent)) {
          return false;
        }
        break;

      case "$project":
        // $project stage content must be an object
        if (!stageContent || typeof stageContent !== "object") {
          return false;
        }
        // $project cannot be empty object
        if (Object.keys(stageContent).length === 0) {
          return false;
        }
        // Validate projection expressions
        if (!isValidMongoExpression(stageContent)) {
          return false;
        }
        break;

      case "$group":
      case "$sort":
      case "$limit":
      case "$skip":
      case "$lookup":
      case "$unwind":
      case "$addFields":
        // These stages must have content
        if (
          !stageContent ||
          (typeof stageContent === "object" &&
            Object.keys(stageContent).length === 0)
        ) {
          return false;
        }
        break;

      // Allow other stages but ensure they have content
      default:
        if (stageContent === undefined || stageContent === null) {
          return false;
        }
        break;
    }
  }

  return true;
};

// Helper function to convert list conditions to MongoDB
const convertListConditionToMongo = (
  condition,
  customVariables = [],
  customListVariables = [],
  variableUsageCounts = {},
) => {
  // If this is actually a listCondition object (passed from convertConditionToMongo)
  // then process it directly
  if (condition.type === "array" && condition.field && condition.operator) {
    return convertListConditionDefinitionToMongo(
      condition,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  const field = getFieldPath(condition.field, customVariables, false);
  const operator = condition.operator;
  const value = condition.value;

  // Handle different types of list conditions
  if (condition.isListVariable) {
    // This shouldn't happen if convertConditionToMongo handles it correctly
    // But keep this as fallback
    return {};
  }

  // Handle array value objects (complex list conditions)
  if (value && typeof value === "object" && value.type === "array") {
    return convertArrayValueToMongo(
      value,
      operator,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  // Handle direct array operators with nested conditions
  if (
    ["$anyElementTrue", "$allElementsTrue", "$filter"].includes(operator) &&
    value &&
    typeof value === "object" &&
    value.children
  ) {
    return convertListConditionDefinitionToMongo(
      {
        field: field,
        operator: operator,
        value: value,
        subField: null,
        // Pass the boolean switch information from the original condition
        booleanSwitch:
          condition.booleanSwitch !== undefined
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
                      : true, // Default to true (no negation)
      },
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  // Handle direct array operators
  return convertArrayOperatorToMongo(field, operator, value, customVariables);
};

// Helper function to convert a list condition definition to MongoDB
const convertListConditionDefinitionToMongo = (
  listCondition,
  customVariables = [],
  customListVariables = [],
  variableUsageCounts = {},
) => {
  const arrayField = listCondition.field;
  const operator = listCondition.operator;
  const nestedConditions = listCondition.value;
  const subField = listCondition.subField;
  const booleanSwitch =
    listCondition.booleanSwitch !== undefined
      ? listCondition.booleanSwitch
      : true;

  switch (operator) {
    case "$anyElementTrue":
      if (nestedConditions && nestedConditions.children) {
        // Convert the nested conditions to a proper condition for $anyElementTrue
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
        );
        const anyElementTrueExpr = {
          $anyElementTrue: {
            $map: {
              input: { $ifNull: [`$${arrayField}`, []] },
              in: conditionsForMap,
            },
          },
        };

        // Apply $not if boolean switch is false
        return {
          $expr: booleanSwitch
            ? anyElementTrueExpr
            : { $not: anyElementTrueExpr },
        };
      }

      return {
        $expr: booleanSwitch
          ? { $anyElementTrue: { $ifNull: [`$${arrayField}`, []] } }
          : { $not: { $anyElementTrue: { $ifNull: [`$${arrayField}`, []] } } },
      };

    case "$allElementsTrue":
      if (nestedConditions && nestedConditions.children) {
        // Convert the nested conditions to a proper condition for $allElementsTrue
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
        );
        const allElementsTrueExpr = {
          $allElementsTrue: {
            $map: {
              input: { $ifNull: [`$${arrayField}`, []] },
              in: conditionsForMap,
            },
          },
        };

        // Apply $not if boolean switch is false
        return {
          $expr: booleanSwitch
            ? allElementsTrueExpr
            : { $not: allElementsTrueExpr },
        };
      }

      return {
        $expr: booleanSwitch
          ? { $allElementsTrue: { $ifNull: [`$${arrayField}`, []] } }
          : { $not: { $allElementsTrue: { $ifNull: [`$${arrayField}`, []] } } },
      };

    case "$filter":
      if (nestedConditions && nestedConditions.children) {
        const filterCondition = convertBlockToMongo(
          nestedConditions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
        );
        return {
          $expr: {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: { $ifNull: [`$${arrayField}`, []] },
                    cond: filterCondition,
                  },
                },
              },
              0,
            ],
          },
        };
      }
      return { [arrayField]: { $exists: true, $type: "array" } };

    case "$min":
      if (subField) {
        const aggregatedValue = { $min: `$${arrayField}.${subField}` };
        // Use comparison operator and value if provided
        if (
          listCondition.comparisonOperator &&
          listCondition.comparisonValue !== undefined
        ) {
          const compareValue = parseNumberIfNeeded(
            listCondition.comparisonValue,
          );
          switch (listCondition.comparisonOperator) {
            case "$gt":
            case "greater than":
            case ">":
              return { $expr: { $gt: [aggregatedValue, compareValue] } };
            case "$gte":
            case "greater than or equal":
            case ">=":
              return { $expr: { $gte: [aggregatedValue, compareValue] } };
            case "$lt":
            case "less than":
            case "<":
              return { $expr: { $lt: [aggregatedValue, compareValue] } };
            case "$lte":
            case "less than or equal":
            case "<=":
              return { $expr: { $lte: [aggregatedValue, compareValue] } };
            case "$ne":
            case "not equals":
            case "!=":
              return { $expr: { $ne: [aggregatedValue, compareValue] } };
            case "$eq":
            case "equals":
            case "=":
            default:
              return { $expr: { $eq: [aggregatedValue, compareValue] } };
          }
        }
        // Default to existence check if no comparison specified
        return { $expr: { $gt: [aggregatedValue, 0] } };
      }
      return { [arrayField]: { $exists: true, $type: "array" } };

    case "$max":
      if (subField) {
        const aggregatedValue = { $max: `$${arrayField}.${subField}` };
        if (
          listCondition.comparisonOperator &&
          listCondition.comparisonValue !== undefined
        ) {
          const compareValue = parseNumberIfNeeded(
            listCondition.comparisonValue,
          );
          switch (listCondition.comparisonOperator) {
            case "$gt":
            case "greater than":
            case ">":
              return { $expr: { $gt: [aggregatedValue, compareValue] } };
            case "$gte":
            case "greater than or equal":
            case ">=":
              return { $expr: { $gte: [aggregatedValue, compareValue] } };
            case "$lt":
            case "less than":
            case "<":
              return { $expr: { $lt: [aggregatedValue, compareValue] } };
            case "$lte":
            case "less than or equal":
            case "<=":
              return { $expr: { $lte: [aggregatedValue, compareValue] } };
            case "$ne":
            case "not equals":
            case "!=":
              return { $expr: { $ne: [aggregatedValue, compareValue] } };
            case "$eq":
            case "equals":
            case "=":
            default:
              return { $expr: { $eq: [aggregatedValue, compareValue] } };
          }
        }
        return { $expr: { $gt: [aggregatedValue, 0] } };
      }
      return { [arrayField]: { $exists: true, $type: "array" } };

    case "$avg":
      if (subField) {
        const aggregatedValue = { $avg: `$${arrayField}.${subField}` };
        if (
          listCondition.comparisonOperator &&
          listCondition.comparisonValue !== undefined
        ) {
          const compareValue = parseNumberIfNeeded(
            listCondition.comparisonValue,
          );
          switch (listCondition.comparisonOperator) {
            case "$gt":
            case "greater than":
            case ">":
              return { $expr: { $gt: [aggregatedValue, compareValue] } };
            case "$gte":
            case "greater than or equal":
            case ">=":
              return { $expr: { $gte: [aggregatedValue, compareValue] } };
            case "$lt":
            case "less than":
            case "<":
              return { $expr: { $lt: [aggregatedValue, compareValue] } };
            case "$lte":
            case "less than or equal":
            case "<=":
              return { $expr: { $lte: [aggregatedValue, compareValue] } };
            case "$ne":
            case "not equals":
            case "!=":
              return { $expr: { $ne: [aggregatedValue, compareValue] } };
            case "$eq":
            case "equals":
            case "=":
            default:
              return { $expr: { $eq: [aggregatedValue, compareValue] } };
          }
        }
        return { $expr: { $gt: [aggregatedValue, 0] } };
      }
      return { [arrayField]: { $exists: true, $type: "array" } };

    case "$sum":
      if (subField) {
        const aggregatedValue = { $sum: `$${arrayField}.${subField}` };
        if (
          listCondition.comparisonOperator &&
          listCondition.comparisonValue !== undefined
        ) {
          const compareValue = parseNumberIfNeeded(
            listCondition.comparisonValue,
          );
          switch (listCondition.comparisonOperator) {
            case "$gt":
            case "greater than":
            case ">":
              return { $expr: { $gt: [aggregatedValue, compareValue] } };
            case "$gte":
            case "greater than or equal":
            case ">=":
              return { $expr: { $gte: [aggregatedValue, compareValue] } };
            case "$lt":
            case "less than":
            case "<":
              return { $expr: { $lt: [aggregatedValue, compareValue] } };
            case "$lte":
            case "less than or equal":
            case "<=":
              return { $expr: { $lte: [aggregatedValue, compareValue] } };
            case "$ne":
            case "not equals":
            case "!=":
              return { $expr: { $ne: [aggregatedValue, compareValue] } };
            case "$eq":
            case "equals":
            case "=":
            default:
              return { $expr: { $eq: [aggregatedValue, compareValue] } };
          }
        }
        return { $expr: { $gt: [aggregatedValue, 0] } };
      }
      return { [arrayField]: { $exists: true, $type: "array" } };

    default:
      return { [arrayField]: { $exists: true, $type: "array" } };
  }
};

// Helper function to convert array value objects to MongoDB
const convertArrayValueToMongo = (
  arrayValue,
  operator,
  customVariables = [],
  customListVariables = [],
  variableUsageCounts = {},
) => {
  const arrayField = arrayValue.field;
  const subField = arrayValue.subField;
  const nestedConditions = arrayValue.value;
  const comparisonValue = arrayValue.comparisonValue || arrayValue.value;
  const comparisonOperator = arrayValue.comparison || "$eq";

  switch (operator) {
    case "$anyElementTrue":
      if (nestedConditions && nestedConditions.children) {
        // Use proper MongoDB $anyElementTrue with $map
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
        );
        return {
          $expr: {
            $anyElementTrue: {
              $map: {
                input: { $ifNull: [`$${arrayField}`, []] },
                in: conditionsForMap,
              },
            },
          },
        };
      }
      return {};

    case "$allElementsTrue":
      if (nestedConditions && nestedConditions.children) {
        // Use proper MongoDB $allElementsTrue with $map
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
        );
        return {
          $expr: {
            $allElementsTrue: {
              $map: {
                input: { $ifNull: [`$${arrayField}`, []] },
                in: conditionsForMap,
              },
            },
          },
        };
      }
      return {};

    case "$filter":
      if (nestedConditions && nestedConditions.children) {
        const filterCondition = convertBlockToMongo(
          nestedConditions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
        );
        return {
          $expr: {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: { $ifNull: [`$${arrayField}`, []] },
                    cond: filterCondition,
                  },
                },
              },
              0,
            ],
          },
        };
      }
      return {};

    case "$min":
      if (subField) {
        // Support different comparison operators for aggregated values
        const aggregatedValue = { $min: `$${arrayField}.${subField}` };
        const targetValue =
          typeof comparisonValue === "number"
            ? comparisonValue
            : parseNumberIfNeeded(comparisonValue);

        switch (comparisonOperator) {
          case "$gt":
            return { $expr: { $gt: [aggregatedValue, targetValue] } };
          case "$gte":
            return { $expr: { $gte: [aggregatedValue, targetValue] } };
          case "$lt":
            return { $expr: { $lt: [aggregatedValue, targetValue] } };
          case "$lte":
            return { $expr: { $lte: [aggregatedValue, targetValue] } };
          case "$ne":
            return { $expr: { $ne: [aggregatedValue, targetValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [aggregatedValue, targetValue] } };
        }
      }
      return {};

    case "$max":
      if (subField) {
        const aggregatedValue = { $max: `$${arrayField}.${subField}` };
        const targetValue =
          typeof comparisonValue === "number"
            ? comparisonValue
            : parseNumberIfNeeded(comparisonValue);

        switch (comparisonOperator) {
          case "$gt":
            return { $expr: { $gt: [aggregatedValue, targetValue] } };
          case "$gte":
            return { $expr: { $gte: [aggregatedValue, targetValue] } };
          case "$lt":
            return { $expr: { $lt: [aggregatedValue, targetValue] } };
          case "$lte":
            return { $expr: { $lte: [aggregatedValue, targetValue] } };
          case "$ne":
            return { $expr: { $ne: [aggregatedValue, targetValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [aggregatedValue, targetValue] } };
        }
      }
      return {};

    case "$avg":
      if (subField) {
        const aggregatedValue = { $avg: `$${arrayField}.${subField}` };
        const targetValue =
          typeof comparisonValue === "number"
            ? comparisonValue
            : parseNumberIfNeeded(comparisonValue);

        switch (comparisonOperator) {
          case "$gt":
            return { $expr: { $gt: [aggregatedValue, targetValue] } };
          case "$gte":
            return { $expr: { $gte: [aggregatedValue, targetValue] } };
          case "$lt":
            return { $expr: { $lt: [aggregatedValue, targetValue] } };
          case "$lte":
            return { $expr: { $lte: [aggregatedValue, targetValue] } };
          case "$ne":
            return { $expr: { $ne: [aggregatedValue, targetValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [aggregatedValue, targetValue] } };
        }
      }
      return {};

    case "$sum":
      if (subField) {
        const aggregatedValue = { $sum: `$${arrayField}.${subField}` };
        const targetValue =
          typeof comparisonValue === "number"
            ? comparisonValue
            : parseNumberIfNeeded(comparisonValue);

        switch (comparisonOperator) {
          case "$gt":
            return { $expr: { $gt: [aggregatedValue, targetValue] } };
          case "$gte":
            return { $expr: { $gte: [aggregatedValue, targetValue] } };
          case "$lt":
            return { $expr: { $lt: [aggregatedValue, targetValue] } };
          case "$lte":
            return { $expr: { $lte: [aggregatedValue, targetValue] } };
          case "$ne":
            return { $expr: { $ne: [aggregatedValue, targetValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [aggregatedValue, targetValue] } };
        }
      }
      return {};

    default:
      return {};
  }
};

// Helper function to convert MongoDB expressions to array context ($$this)
const convertToArrayContext = (mongoExpression) => {
  if (!mongoExpression || typeof mongoExpression !== "object") {
    return mongoExpression;
  }

  // Clone the expression to avoid modifying the original
  const clonedExpression = JSON.parse(JSON.stringify(mongoExpression));
  // Recursively convert field references from $field to $$this.field
  const convertFields = (obj) => {
    if (typeof obj === "string" && !obj.startsWith("$$")) {
      if (obj.startsWith("$this")) {
        // If the field is $this, convert it to $$this
        return `$$${obj.substring(1)}`;
      } else if (obj.startsWith("$")) {
        // If the field is $field, convert it to $$this.field
        return `$${obj.substring(1)}`;
      }
      // Convert $field to $$this.field
      return `$$this.${obj.substring(1)}`;
    }

    if (Array.isArray(obj)) {
      return obj.map(convertFields);
    }

    if (typeof obj === "object" && obj !== null) {
      const result = {};
      for (const [key, value] of Object.entries(obj)) {
        result[key] = convertFields(value);
      }
      return result;
    }

    return obj;
  };

  return convertFields(clonedExpression);
};

// Helper function to handle conditions where variables are inlined as MongoDB expressions
const handleInlinedVariableCondition = (mongoExpression, operator, value) => {
  // For inlined variables, we need to use $expr to compare the expression result
  switch (operator) {
    case "$eq":
    case "equals":
    case "=":
      return { $expr: { $eq: [mongoExpression, parseNumberIfNeeded(value)] } };

    case "$ne":
    case "not equals":
    case "!=":
      return { $expr: { $ne: [mongoExpression, parseNumberIfNeeded(value)] } };

    case "$gt":
    case "greater than":
    case ">":
      return { $expr: { $gt: [mongoExpression, parseNumberIfNeeded(value)] } };

    case "$gte":
    case "greater than or equal":
    case ">=":
      return { $expr: { $gte: [mongoExpression, parseNumberIfNeeded(value)] } };

    case "$lt":
    case "less than":
    case "<":
      return { $expr: { $lt: [mongoExpression, parseNumberIfNeeded(value)] } };

    case "$lte":
    case "less than or equal":
    case "<=":
      return { $expr: { $lte: [mongoExpression, parseNumberIfNeeded(value)] } };

    case "$in":
    case "in":
      return {
        $expr: {
          $in: [mongoExpression, Array.isArray(value) ? value : [value]],
        },
      };

    case "$nin":
    case "not in":
      return {
        $expr: {
          $not: {
            $in: [mongoExpression, Array.isArray(value) ? value : [value]],
          },
        },
      };

    case "between":
      if (Array.isArray(value) && value.length === 2) {
        return {
          $expr: {
            $and: [
              { $gte: [mongoExpression, parseNumberIfNeeded(value[0])] },
              { $lte: [mongoExpression, parseNumberIfNeeded(value[1])] },
            ],
          },
        };
      }
      return {};

    case "not between":
      if (Array.isArray(value) && value.length === 2) {
        return {
          $expr: {
            $or: [
              { $lt: [mongoExpression, parseNumberIfNeeded(value[0])] },
              { $gt: [mongoExpression, parseNumberIfNeeded(value[1])] },
            ],
          },
        };
      }
      return {};

    default:
      // For other operators, default to equality comparison
      return { $expr: { $eq: [mongoExpression, parseNumberIfNeeded(value)] } };
  }
};

// Helper function to count how many times each variable is used in the filter conditions
const countVariableUsage = (filters, customVariables = []) => {
  const usageCounts = {};

  // Initialize counts for all custom variables
  customVariables.forEach((variable) => {
    if (variable.name) {
      usageCounts[variable.name] = 0;
    }
  });
  // Count usage in each filter block
  filters.forEach((block) => {
    countVariableUsageInBlock(block, usageCounts, customVariables);
  });

  return usageCounts;
};

// Helper function to collect all fields used in the filter conditions
const getUsedFields = (
  filters,
  customVariables = [],
  customListVariables = [],
) => {
  const usedFields = {
    baseFields: new Set(),
    customVariables: new Set(),
    listVariables: new Set(),
  };

  // Collect fields from each filter block
  filters.forEach((block) => {
    collectUsedFieldsInBlock(
      block,
      usedFields,
      customVariables,
      customListVariables,
    );
  });

  return {
    baseFields: Array.from(usedFields.baseFields),
    customVariables: Array.from(usedFields.customVariables),
    listVariables: Array.from(usedFields.listVariables),
  };
};

// Helper function to recursively collect used fields in a block
const collectUsedFieldsInBlock = (
  block,
  usedFields,
  customVariables = [],
  customListVariables = [],
) => {
  if (!block) {
    return;
  }

  // Handle direct conditions (not wrapped in blocks)
  if (block.type === "condition" || block.category === "condition") {
    collectUsedFieldsInCondition(
      block,
      usedFields,
      customVariables,
      customListVariables,
    );
    return;
  }

  // Handle blocks with children
  if (!block.children || block.children.length === 0) {
    return;
  }

  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      // Recursively collect in nested blocks
      collectUsedFieldsInBlock(
        child,
        usedFields,
        customVariables,
        customListVariables,
      );
    } else if (child.category === "condition" || child.type === "condition") {
      // Collect fields used in this condition
      collectUsedFieldsInCondition(
        child,
        usedFields,
        customVariables,
        customListVariables,
      );
    }
  });
};

// Helper function to collect fields used in a single condition
const collectUsedFieldsInCondition = (
  condition,
  usedFields,
  customVariables = [],
  customListVariables = [],
) => {
  if (!condition.field) {
    return;
  }
  const fieldName = getFieldName(condition.field);
  // Check if it's a custom variable
  const isCustomVariable = customVariables.some((v) => v.name === fieldName);
  const isListVariable = customListVariables.some((v) => v.name === fieldName);

  if (isCustomVariable) {
    usedFields.customVariables.add(fieldName);
    // Extract field dependencies from the custom variable's LaTeX expression
    const customVar = customVariables.find((v) => v.name === fieldName);
    if (customVar && customVar.variable) {
      const eqParts = customVar.variable.split("=");
      if (eqParts.length === 2) {
        const latexExpression = eqParts[1].trim();
        const fieldDependencies =
          latexToMongoConverter.extractFieldDependencies(latexExpression);
        fieldDependencies.forEach((depField) => {
          usedFields.baseFields.add(depField);
        });
      }
    }
  } else if (isListVariable) {
    usedFields.listVariables.add(fieldName);

    // Check if the list variable contains custom arithmetic variables that need field dependencies extracted
    const listVariable = customListVariables.find(
      (lv) => lv.name === fieldName,
    );
    if (
      listVariable &&
      listVariable.listCondition &&
      listVariable.listCondition.value
    ) {
      // Get the array field that this list variable operates on
      const arrayField = listVariable.listCondition.field;

      // Convert the nested conditions to MongoDB to extract only absolute field references
      try {
        const mongoCondition = convertBlockToMongo(
          listVariable.listCondition.value,
          customVariables,
          customListVariables,
          true,
          {},
          [],
        );
        const absoluteFields = extractAbsoluteFieldReferences(mongoCondition);

        // Add only the absolute field references (these are fields that need projection)
        absoluteFields.forEach((field) => {
          // Check if it's a custom variable or list variable
          const isCustomVariable = customVariables.some(
            (v) => v.name === field,
          );
          const isListVariable = customListVariables.some(
            (v) => v.name === field,
          );

          if (isCustomVariable) {
            usedFields.customVariables.add(field);
            // Also extract dependencies from custom variables
            const customVar = customVariables.find((v) => v.name === field);
            if (customVar && customVar.variable) {
              const eqParts = customVar.variable.split("=");
              if (eqParts.length === 2) {
                const latexExpression = eqParts[1].trim();
                const fieldDependencies =
                  latexToMongoConverter.extractFieldDependencies(
                    latexExpression,
                  );
                fieldDependencies.forEach((depField) => {
                  usedFields.baseFields.add(depField);
                });
              }
            }
          } else if (isListVariable) {
            usedFields.listVariables.add(field);
          } else {
            usedFields.baseFields.add(field);
          }
        });
      } catch (error) {
        console.warn(
          "Failed to extract absolute field references from list variable:",
          error,
        );
      }

      // Add the array field itself to ensure it's projected
      if (arrayField) {
        usedFields.baseFields.add(arrayField);
      }
    }
  } else {
    usedFields.baseFields.add(fieldName);
  }

  // For list conditions, collect nested fields as well
  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.type === "array"
  ) {
    const arrayValue = condition.value;

    // Collect nested fields in list conditions
    if (arrayValue.value) {
      if (arrayValue.value.children) {
        // It's a block with children
        collectUsedFieldsInBlock(
          arrayValue.value,
          usedFields,
          customVariables,
          customListVariables,
        );
      } else if (
        arrayValue.value.type === "condition" ||
        arrayValue.value.category === "condition"
      ) {
        // It's a direct condition, not wrapped in a block
        collectUsedFieldsInCondition(
          arrayValue.value,
          usedFields,
          customVariables,
          customListVariables,
        );
      }
    }

    // Collect the array field itself
    if (arrayValue.field) {
      const arrayFieldName = getFieldName(arrayValue.field);
      const isArrayCustomVariable = customVariables.some(
        (v) => v.name === arrayFieldName,
      );
      const isArrayListVariable = customListVariables.some(
        (v) => v.name === arrayFieldName,
      );

      if (isArrayCustomVariable) {
        usedFields.customVariables.add(arrayFieldName);
      } else if (isArrayListVariable) {
        usedFields.listVariables.add(arrayFieldName);
      } else {
        usedFields.baseFields.add(arrayFieldName);
      }
    }
  }

  // Handle nested conditions in array operations (like $anyElementTrue, $allElementsTrue)
  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.children
  ) {
    // For array operations, we need to collect both array-relative fields ($$this.field)
    // and absolute fields ($field) that might be referenced
    if (
      ["$anyElementTrue", "$allElementsTrue", "$filter"].includes(
        condition.operator,
      )
    ) {
      // Convert the nested conditions to MongoDB to extract absolute field references
      try {
        const mongoCondition = convertBlockToMongo(
          condition.value,
          customVariables,
          customListVariables,
          true,
          {},
          [],
        );
        const absoluteFields = extractAbsoluteFieldReferences(mongoCondition);
        absoluteFields.forEach((field) => {
          // Check if it's a custom variable or list variable
          const isCustomVariable = customVariables.some(
            (v) => v.name === field,
          );
          const isListVariable = customListVariables.some(
            (v) => v.name === field,
          );

          if (isCustomVariable) {
            usedFields.customVariables.add(field);
            // Also extract dependencies from custom variables
            const customVar = customVariables.find((v) => v.name === field);
            if (customVar && customVar.variable) {
              const eqParts = customVar.variable.split("=");
              if (eqParts.length === 2) {
                const latexExpression = eqParts[1].trim();
                const fieldDependencies =
                  latexToMongoConverter.extractFieldDependencies(
                    latexExpression,
                  );
                fieldDependencies.forEach((depField) => {
                  usedFields.baseFields.add(depField);
                });
              }
            }
          } else if (isListVariable) {
            usedFields.listVariables.add(field);
          } else {
            usedFields.baseFields.add(field);
          }
        });
      } catch (error) {
        // If conversion fails, fallback to regular collection
        console.warn("Failed to extract absolute field references:", error);
      }
    }

    // Also collect array-relative fields normally
    collectUsedFieldsInBlock(
      condition.value,
      usedFields,
      customVariables,
      customListVariables,
    );
  }
};

// Helper function to recursively count variable usage in a block
const countVariableUsageInBlock = (
  block,
  usageCounts,
  customVariables = [],
) => {
  if (!block || !block.children || block.children.length === 0) {
    return;
  }

  block.children.forEach((child) => {
    if (child.category === "block") {
      // Recursively count in nested blocks
      countVariableUsageInBlock(child, usageCounts, customVariables);
    } else if (child.category === "condition") {
      // Count variable usage in this condition
      countVariableUsageInCondition(child, usageCounts, customVariables);
    }
  });
};

// Helper function to count variable usage in a single condition
const countVariableUsageInCondition = (
  condition,
  usageCounts,
  customVariables = [],
) => {
  if (!condition.field) {
    return;
  }

  // Check if the field is a custom variable
  const fieldName = getFieldName(condition.field);
  if (Object.prototype.hasOwnProperty.call(usageCounts, fieldName)) {
    usageCounts[fieldName]++;
  }

  // For list conditions, check nested conditions as well
  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.type === "array"
  ) {
    const arrayValue = condition.value;

    // Check nested conditions in list conditions
    if (arrayValue.value && arrayValue.value.children) {
      countVariableUsageInBlock(arrayValue.value, usageCounts, customVariables);
    }

    // Check if the array field itself is a custom variable
    if (arrayValue.field) {
      const arrayFieldName = getFieldName(arrayValue.field);
      if (Object.prototype.hasOwnProperty.call(usageCounts, arrayFieldName)) {
        usageCounts[arrayFieldName]++;
      }
    }
  }

  // Handle nested conditions in array operations (like $anyElementTrue, $allElementsTrue)
  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.children
  ) {
    countVariableUsageInBlock(condition.value, usageCounts, customVariables);
  }
};

// Helper function to extract field name from various field formats
const getFieldName = (field) => {
  if (typeof field === "string") {
    return field;
  }

  if (field && typeof field === "object") {
    return field.value || field.name || field.field || String(field);
  }

  return String(field);
};

// Helper function to convert array operators to MongoDB
const convertArrayOperatorToMongo = (field, operator, value) => {
  switch (operator) {
    case "$anyElementTrue":
      // For simple boolean values, apply $not when false
      if (typeof value === "boolean") {
        if (value) {
          // Normal $anyElementTrue behavior
          return {
            $expr: {
              $anyElementTrue: { $ifNull: [`$${field}`, []] },
            },
          };
        } else {
          // Apply $not to $anyElementTrue when boolean switch is false
          return {
            $expr: {
              $not: {
                $anyElementTrue: { $ifNull: [`$${field}`, []] },
              },
            },
          };
        }
      }
      // For non-boolean values or undefined, use $anyElementTrue properly
      if (value !== undefined && value !== null && value !== "") {
        // Use proper $anyElementTrue for specific value matching
        return {
          $expr: {
            $anyElementTrue: {
              $map: {
                input: { $ifNull: [`$${field}`, []] },
                in: { $eq: ["$$this", value] },
              },
            },
          },
        };
      }
      // For undefined/null/empty values, check if array has any truthy elements
      return {
        $expr: {
          $anyElementTrue: { $ifNull: [`$${field}`, []] },
        },
      };

    case "$allElementsTrue":
      // For simple boolean values, apply $not when false
      if (typeof value === "boolean") {
        if (value) {
          // Normal $allElementsTrue behavior
          return {
            $expr: {
              $allElementsTrue: { $ifNull: [`$${field}`, []] },
            },
          };
        } else {
          // Apply $not to $allElementsTrue when boolean switch is false
          return {
            $expr: {
              $not: {
                $allElementsTrue: { $ifNull: [`$${field}`, []] },
              },
            },
          };
        }
      }
      // For non-boolean values, use proper $allElementsTrue
      if (value !== undefined && value !== null && value !== "") {
        return {
          $expr: {
            $allElementsTrue: {
              $map: {
                input: { $ifNull: [`$${field}`, []] },
                in: { $eq: ["$$this", value] },
              },
            },
          },
        };
      }
      // For undefined/null/empty values, check if all elements are truthy
      return {
        $expr: {
          $allElementsTrue: { $ifNull: [`$${field}`, []] },
        },
      };

    case "$filter":
      // Filter requires a condition - if we just have a simple value, check if any element equals it
      if (
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
      ) {
        return {
          $expr: {
            $gt: [
              {
                $size: {
                  $filter: {
                    input: { $ifNull: [`$${field}`, []] },
                    cond: { $eq: ["$$this", value] },
                  },
                },
              },
              0,
            ],
          },
        };
      }
      // For complex filter conditions, we need more structure - fallback to existence check
      return { [field]: { $exists: true, $type: "array" } };

    case "$min":
      // Find documents where the minimum value in the array matches the condition
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $min: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
      // For objects with comparison operators
      if (
        typeof value === "object" &&
        value.comparison &&
        value.value !== undefined
      ) {
        const comparison = value.comparison || "$eq";
        const compareValue = parseNumberIfNeeded(value.value);
        const minExpression = { $min: { $ifNull: [`$${field}`, []] } };

        switch (comparison) {
          case "$gt":
            return { $expr: { $gt: [minExpression, compareValue] } };
          case "$gte":
            return { $expr: { $gte: [minExpression, compareValue] } };
          case "$lt":
            return { $expr: { $lt: [minExpression, compareValue] } };
          case "$lte":
            return { $expr: { $lte: [minExpression, compareValue] } };
          case "$ne":
            return { $expr: { $ne: [minExpression, compareValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [minExpression, compareValue] } };
        }
      }
      return { [field]: { $exists: true, $type: "array" } };

    case "$max":
      // Find documents where the maximum value in the array matches the condition
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $max: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
      // For objects with comparison operators
      if (
        typeof value === "object" &&
        value.comparison &&
        value.value !== undefined
      ) {
        const comparison = value.comparison || "$eq";
        const compareValue = parseNumberIfNeeded(value.value);
        const maxExpression = { $max: { $ifNull: [`$${field}`, []] } };

        switch (comparison) {
          case "$gt":
            return { $expr: { $gt: [maxExpression, compareValue] } };
          case "$gte":
            return { $expr: { $gte: [maxExpression, compareValue] } };
          case "$lt":
            return { $expr: { $lt: [maxExpression, compareValue] } };
          case "$lte":
            return { $expr: { $lte: [maxExpression, compareValue] } };
          case "$ne":
            return { $expr: { $ne: [maxExpression, compareValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [maxExpression, compareValue] } };
        }
      }
      return { [field]: { $exists: true, $type: "array" } };

    case "$avg":
      // Find documents where the average value in the array matches the condition
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $avg: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
      // For objects with comparison operators
      if (
        typeof value === "object" &&
        value.comparison &&
        value.value !== undefined
      ) {
        const comparison = value.comparison || "$eq";
        const compareValue = parseNumberIfNeeded(value.value);
        const avgExpression = { $avg: { $ifNull: [`$${field}`, []] } };

        switch (comparison) {
          case "$gt":
            return { $expr: { $gt: [avgExpression, compareValue] } };
          case "$gte":
            return { $expr: { $gte: [avgExpression, compareValue] } };
          case "$lt":
            return { $expr: { $lt: [avgExpression, compareValue] } };
          case "$lte":
            return { $expr: { $lte: [avgExpression, compareValue] } };
          case "$ne":
            return { $expr: { $ne: [avgExpression, compareValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [avgExpression, compareValue] } };
        }
      }
      return { [field]: { $exists: true, $type: "array" } };

    case "$sum":
      // Find documents where the sum of values in the array matches the condition
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $sum: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
      // For objects with comparison operators
      if (
        typeof value === "object" &&
        value.comparison &&
        value.value !== undefined
      ) {
        const comparison = value.comparison || "$eq";
        const compareValue = parseNumberIfNeeded(value.value);
        const sumExpression = { $sum: { $ifNull: [`$${field}`, []] } };

        switch (comparison) {
          case "$gt":
            return { $expr: { $gt: [sumExpression, compareValue] } };
          case "$gte":
            return { $expr: { $gte: [sumExpression, compareValue] } };
          case "$lt":
            return { $expr: { $lt: [sumExpression, compareValue] } };
          case "$lte":
            return { $expr: { $lte: [sumExpression, compareValue] } };
          case "$ne":
            return { $expr: { $ne: [sumExpression, compareValue] } };
          case "$eq":
          default:
            return { $expr: { $eq: [sumExpression, compareValue] } };
        }
      }
      return { [field]: { $exists: true, $type: "array" } };

    default:
      return {};
  }
};

// Helper function to extract absolute field references from MongoDB expressions
// This finds fields like "$candidate.jd" that are not array-relative ($$this.field)
const extractAbsoluteFieldReferences = (mongoExpression) => {
  const fields = new Set();

  const extractFromValue = (value) => {
    if (typeof value === "string") {
      // Look for absolute field references that start with $ but not $$
      if (value.startsWith("$") && !value.startsWith("$$")) {
        // Remove the $ prefix to get the field name
        fields.add(value.substring(1));
      }
    } else if (Array.isArray(value)) {
      value.forEach(extractFromValue);
    } else if (typeof value === "object" && value !== null) {
      Object.values(value).forEach(extractFromValue);
    }
  };

  extractFromValue(mongoExpression);
  return Array.from(fields);
};
