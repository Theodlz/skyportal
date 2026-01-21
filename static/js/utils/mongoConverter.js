import { latexToMongoConverter } from "./robustLatexConverter.js";
import { getFieldType } from "./conditionHelpers.js";

const removePathCollisions = (fields) => {
  const fieldArray = Array.isArray(fields) ? fields : Object.keys(fields);
  const sortedFields = [...fieldArray].sort();
  const filteredFields = [];

  for (const field of sortedFields) {
    const hasParent = filteredFields.some((existingField) =>
      field.startsWith(`${existingField}.`),
    );

    if (!hasParent) {
      const childFields = filteredFields.filter((existingField) =>
        existingField.startsWith(`${field}.`),
      );

      childFields.forEach((child) => {
        const index = filteredFields.indexOf(child);
        if (index > -1) {
          filteredFields.splice(index, 1);
        }
      });

      filteredFields.push(field);
    }
  }

  return filteredFields;
};

const getNumericComparison = (operator) => {
  switch (operator) {
    case "$gt":
    case "greater than":
    case ">":
      return "$gt";
    case "$gte":
    case "greater than or equal":
    case ">=":
      return "$gte";
    case "$lt":
    case "less than":
    case "<":
      return "$lt";
    case "$lte":
    case "less than or equal":
    case "<=":
      return "$lte";
    default:
      return null;
  }
};

export const convertToMongoAggregation = (
  filters,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
  additionalFieldsToProject = [],
) => {
  if (
    !filters ||
    (filters.length === 0 && additionalFieldsToProject.length === 0)
  ) {
    return [];
  }

  const pipeline = [];

  const { simpleConditions, complexFilters } =
    separateSimpleAndComplexConditions(
      filters,
      customVariables,
      customListVariables,
      customSwitchCases,
      schema,
      fieldOptions,
    );

  if (Object.keys(simpleConditions).length > 0) {
    pipeline.push({ $match: simpleConditions });
  }

  const dependencyGraph = buildVariableDependencyGraph(
    customVariables,
    customListVariables,
    customSwitchCases,
  );

  const variableUsageCounts = countVariableUsage(
    complexFilters,
    customVariables,
  );
  const usedFields = getUsedFields(
    complexFilters,
    customVariables,
    customListVariables,
  );

  const hasListOperationVariables = customListVariables.some(
    (listVar) =>
      listVar.listCondition &&
      ["$filter", "$min", "$max", "$avg", "$sum"].includes(
        listVar.listCondition.operator,
      ) &&
      usedFields.listVariables.includes(listVar.name),
  );

  const customBlocksWithFalseValue = [];

  const findCustomBlocksWithFalse = (block) => {
    if (!block) return;

    if (block.customBlockName && block.isTrue === false) {
      customBlocksWithFalseValue.push({
        name: block.customBlockName,
        id: block.id,
        block: block,
      });
    }

    if (block.children && Array.isArray(block.children)) {
      block.children.forEach((child) => {
        if (child.category === "block" || child.type === "block") {
          findCustomBlocksWithFalse(child);
        }
      });
    }
  };

  complexFilters.forEach((block) => {
    findCustomBlocksWithFalse(block);
  });

  const needsProjectStage =
    hasListOperationVariables ||
    usedFields.baseFields.length > 0 ||
    customBlocksWithFalseValue.length > 0;

  const projectedVariables = new Set();

  let initialProjectStage = null;
  if (needsProjectStage) {
    initialProjectStage = { $project: {} };

    const filteredBaseFields = removePathCollisions(usedFields.baseFields);

    filteredBaseFields.forEach((field) => {
      initialProjectStage.$project[field] = 1;
    });

    if (!filteredBaseFields.includes("objectId")) {
      initialProjectStage.$project.objectId = 1;
    }

    customBlocksWithFalseValue.forEach((customBlock) => {
      const variableName = `${customBlock.name.replace(/[^a-zA-Z0-9]/g, "_")}`;
      const blockCondition = convertBlockToMongoExpr(
        customBlock.block,
        schema,
        fieldOptions,
        customVariables,
        customListVariables,
        customBlocksWithFalseValue,
      );
      if (blockCondition && Object.keys(blockCondition).length > 0) {
        initialProjectStage.$project[variableName] = blockCondition;
      }
    });

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
          const filterCondition = convertBlockToMongo(
            listCondition.value,
            schema,
            fieldOptions,
            customVariables,
            customListVariables,
            true,
            variableUsageCounts,
            [],
            listCondition.field,
          );
          initialProjectStage.$project[listVar.name] = {
            $filter: {
              input: `$${arrayField}`,
              cond: filterCondition,
            },
          };
        } else if (
          ["$min", "$max", "$avg", "$sum"].includes(operator) &&
          subField
        ) {
          switch (operator) {
            case "$min":
              initialProjectStage.$project[listVar.name] = {
                $min: `$${arrayField}.${subField}`,
              };
              break;
            case "$max":
              initialProjectStage.$project[listVar.name] = {
                $max: `$${arrayField}.${subField}`,
              };
              break;
            case "$avg":
              initialProjectStage.$project[listVar.name] = {
                $avg: `$${arrayField}.${subField}`,
              };
              break;
            case "$sum":
              initialProjectStage.$project[listVar.name] = {
                $sum: `$${arrayField}.${subField}`,
              };
              break;
          }
        }
      }
    });

    pipeline.push(initialProjectStage);
  }

  const additionalSwitchCases = new Set();
  additionalFieldsToProject.forEach((fieldName) => {
    const isSwitchCase = customSwitchCases.some((sc) => sc.name === fieldName);
    if (isSwitchCase && !projectedVariables.has(fieldName)) {
      additionalSwitchCases.add(fieldName);
    }
  });

  const blockGroups = [];
  let currentGroup = {
    blocks: [],
    requiredVariables: new Set(),
  };

  complexFilters.forEach((block) => {
    const variablesInBlock = getVariablesUsedInBlock(
      block,
      customVariables,
      customListVariables,
      customSwitchCases,
    );

    const arithmeticVarsInBlock = variablesInBlock.filter(
      (varName) => !customListVariables.some((lv) => lv.name === varName),
    );

    const newVariablesNeeded = arithmeticVarsInBlock.filter(
      (varName) => !projectedVariables.has(varName),
    );

    if (newVariablesNeeded.length > 0) {
      if (currentGroup.blocks.length > 0) {
        blockGroups.push(currentGroup);
        currentGroup = {
          blocks: [],
          requiredVariables: new Set(),
        };
      }

      newVariablesNeeded.forEach((varName) => {
        currentGroup.requiredVariables.add(varName);
        const deps = getAllVariableDependencies(varName, dependencyGraph);
        deps.variables.forEach((depVar) => {
          if (!projectedVariables.has(depVar)) {
            currentGroup.requiredVariables.add(depVar);
          }
        });
        if (deps.listVariables) {
          deps.listVariables.forEach((listVarName) => {
            usedFields.listVariables.push(listVarName);
          });
        }
      });
    }

    currentGroup.blocks.push(block);
  });

  if (currentGroup.blocks.length > 0) {
    blockGroups.push(currentGroup);
  }

  if (additionalSwitchCases.size > 0 && blockGroups.length === 0) {
    blockGroups.push({
      blocks: [],
      requiredVariables: additionalSwitchCases,
    });
  } else if (additionalSwitchCases.size > 0 && blockGroups.length > 0) {
    additionalSwitchCases.forEach((switchCase) => {
      blockGroups[0].requiredVariables.add(switchCase);
    });
  }

  blockGroups.forEach((group, groupIndex) => {
    if (group.requiredVariables.size > 0) {
      const sortedVars = topologicalSortVariables(
        Array.from(group.requiredVariables),
        dependencyGraph,
      );

      const variableLayers = [];
      const processedVars = new Set();

      sortedVars.forEach((varName) => {
        const deps = getAllVariableDependencies(varName, dependencyGraph);

        let requiredLayer = 0;
        deps.variables.forEach((depVar) => {
          if (processedVars.has(depVar)) {
            for (let i = 0; i < variableLayers.length; i++) {
              if (variableLayers[i].has(depVar)) {
                requiredLayer = Math.max(requiredLayer, i + 1);
                break;
              }
            }
          }
        });

        while (variableLayers.length <= requiredLayer) {
          variableLayers.push(new Set());
        }
        variableLayers[requiredLayer].add(varName);
        processedVars.add(varName);
      });

      let firstLayerDependsOnListVariables = false;
      if (variableLayers.length > 0) {
        Array.from(variableLayers[0]).forEach((varName) => {
          const deps = getAllVariableDependencies(varName, dependencyGraph);
          if (deps.listVariables && deps.listVariables.length > 0) {
            firstLayerDependsOnListVariables = true;
          }
        });
      }

      variableLayers.forEach((layerVars, layerIndex) => {
        let projectStage;
        if (
          groupIndex === 0 &&
          layerIndex === 0 &&
          initialProjectStage &&
          !firstLayerDependsOnListVariables
        ) {
          projectStage = initialProjectStage;
          pipeline.pop();
        } else {
          projectStage = { $project: {} };

          projectStage.$project.objectId = 1;

          const fieldsToProject = [
            ...usedFields.baseFields,
            ...usedFields.listVariables,
            ...Array.from(projectedVariables),
          ];

          const filteredFields = removePathCollisions(fieldsToProject);

          filteredFields.forEach((field) => {
            projectStage.$project[field] = 1;
          });
        }

        Array.from(layerVars).forEach((varName) => {
          const customVar = customVariables.find((v) => v.name === varName);
          const switchCase = customSwitchCases.find((v) => v.name === varName);

          if (customVar && customVar.variable) {
            const eqParts = customVar.variable.split("=");
            if (eqParts.length === 2) {
              const latexExpression = eqParts[1].trim();
              try {
                const mongoExpression =
                  latexToMongoConverter.convertToMongo(latexExpression);
                projectStage.$project[varName] = mongoExpression;
                projectedVariables.add(varName);
              } catch (error) {
                console.warn(
                  `Failed to convert LaTeX expression for variable ${varName}:`,
                  error,
                );
              }
            }
          } else if (switchCase && switchCase.switchCondition) {
            const switchExpr = convertSwitchCaseToMongo(
              switchCase.switchCondition,
              schema,
              fieldOptions,
              customVariables,
              customListVariables,
              customSwitchCases,
            );
            if (switchExpr) {
              projectStage.$project[varName] = switchExpr;
              projectedVariables.add(varName);
            }
          }
        });

        pipeline.push(projectStage);
      });
    }

    const matchStage = { $match: {} };

    group.blocks.forEach((block) => {
      const blockCondition = convertBlockToMongo(
        block,
        schema,
        fieldOptions,
        customVariables,
        customListVariables,
        false,
        variableUsageCounts,
        customBlocksWithFalseValue,
      );

      if (blockCondition && Object.keys(blockCondition).length > 0) {
        Object.assign(matchStage.$match, blockCondition);
      }
    });

    if (blockGroups.indexOf(group) === 0) {
      customBlocksWithFalseValue.forEach((customBlock) => {
        const variableName = `${customBlock.name.replace(
          /[^a-zA-Z0-9]/g,
          "_",
        )}`;
        matchStage.$match[variableName] = false;
      });
    }

    if (Object.keys(matchStage.$match).length > 0) {
      pipeline.push(matchStage);
    }
  });

  const finalProjectStage = { $project: { objectId: 1 } };

  const allFieldsToProject = [
    ...usedFields.baseFields,
    ...usedFields.customVariables,
    ...usedFields.listVariables,
  ];

  const filteredFields = removePathCollisions(allFieldsToProject);

  filteredFields.forEach((field) => {
    finalProjectStage.$project[field] = 1;
  });

  pipeline.push(finalProjectStage);

  return pipeline;
};

const convertBlockToMongo = (
  block,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  isInArrayFilter = false,
  variableUsageCounts = {},
  customBlocksWithFalseValue = [],
  arrayFieldName = null,
) => {
  if (!block) {
    return {};
  }

  if (block.type === "condition" || block.category === "condition") {
    let condition;
    if (isInArrayFilter) {
      condition = convertConditionToMongoExpr(
        block,
        customVariables,
        customListVariables,
        schema,
        fieldOptions,
        arrayFieldName,
      );
    } else {
      condition = convertConditionToMongo(
        block,
        schema,
        fieldOptions,
        customVariables,
        customListVariables,
        isInArrayFilter,
        variableUsageCounts,
        arrayFieldName,
      );
    }
    return condition || {};
  }

  if (!block.children || block.children.length === 0) {
    return {};
  }

  const conditions = [];

  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      const nestedCondition = convertBlockToMongo(
        child,
        schema,
        fieldOptions,
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
      let condition;
      if (isInArrayFilter) {
        condition = convertConditionToMongoExpr(
          child,
          customVariables,
          customListVariables,
          schema,
          fieldOptions,
          arrayFieldName,
        );
      } else {
        condition = convertConditionToMongo(
          child,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          isInArrayFilter,
          variableUsageCounts,
          arrayFieldName,
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
    const result = conditions[0];
    // Apply NOT if isTrue is false
    if (block.isTrue === false) {
      return { $nor: [result] };
    }
    return result;
  }

  const logic = (block.logic || "and").toLowerCase();
  let result;
  if (logic === "or") {
    result = { $or: conditions };
  } else {
    result = { $and: conditions };
  }

  // Apply NOT if isTrue is false (for nested blocks or custom blocks)
  if (block.isTrue === false) {
    return { $nor: [result] };
  }

  return result;
};

const convertConditionToMongo = (
  condition,
  schema,
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  isInArrayFilter = false,
  variableUsageCounts = {},
  arrayFieldName = null,
) => {
  const operator = condition.operator;

  if (operator !== "$switch" && (!condition.field || !condition.operator)) {
    return {};
  }

  if (!condition.operator) {
    return {};
  }
  const value = condition.value;

  const listVariable = customListVariables.find(
    (lv) => lv.name === condition.field,
  );
  if (listVariable && listVariable.listCondition) {
    const listCondition = listVariable.listCondition;

    if (["$min", "$max", "$avg", "$sum"].includes(listCondition.operator)) {
      const field = condition.field;

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

    if (listCondition.operator === "$filter") {
      const field = condition.field;

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
          return {
            $expr: {
              $gt: [{ $size: { $ifNull: [`$${field}`, []] } }, 0],
            },
          };
        }
      }
    }

    const listConditionWithBooleanSwitch = {
      ...listVariable.listCondition,
      booleanSwitch:
        condition.booleanSwitch !== undefined
          ? condition.booleanSwitch
          : listVariable.listCondition.booleanSwitch !== undefined
            ? listVariable.listCondition.booleanSwitch
            : true,
    };
    return convertListConditionToMongo(
      listConditionWithBooleanSwitch,
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

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
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  let fieldPath;
  if (isInArrayFilter && arrayFieldName) {
    if (condition.field.startsWith(`${arrayFieldName}.`)) {
      const fieldForArray = condition.field.substring(
        arrayFieldName.length + 1,
      );
      fieldPath = `$$this.${fieldForArray}`;
    } else if (!condition.field.includes(".")) {
      fieldPath = `$$this.${condition.field}`;
    } else {
      fieldPath = `$${condition.field}`;
    }
  } else if (
    isInArrayFilter &&
    !arrayFieldName &&
    condition.field.includes(".")
  ) {
    const commonArrayPatterns = ["prv_candidates", "candidates", "detections"];
    let patternMatched = false;
    for (const pattern of commonArrayPatterns) {
      if (condition.field.startsWith(`${pattern}.`)) {
        const fieldForArray = condition.field.substring(pattern.length + 1);
        fieldPath = `$$this.${fieldForArray}`;
        console.warn(
          `Detected array field pattern in convertConditionToMongo for ${condition.field}, using $$this.${fieldForArray}. Consider ensuring arrayFieldName is set properly.`,
        );
        patternMatched = true;
        break;
      }
    }
    if (!patternMatched) {
      fieldPath = getFieldPath(condition.field, customVariables, false);
    }
  } else {
    fieldPath = getFieldPath(condition.field, customVariables, false);
  }

  if (typeof fieldPath === "object" && fieldPath !== null) {
    return handleInlinedVariableCondition(fieldPath, operator, value);
  }

  const field = fieldPath;

  switch (operator) {
    case "$eq":
    case "equals":
    case "=": {
      const processedValue = convertNullLikeStringsToNull(value);
      const fieldType = getFieldType(
        condition.field,
        customVariables,
        schema,
        fieldOptions,
        [],
        customListVariables,
      );
      if (fieldType === "boolean" || typeof processedValue === "boolean") {
        return { [field]: { $in: [parseNumberIfNeeded(processedValue)] } };
      }
      return { [field]: { $eq: parseNumberIfNeeded(processedValue) } };
    }

    case "$ne":
    case "not equals":
    case "!=": {
      const processedValue = convertNullLikeStringsToNull(value);
      const fieldType = getFieldType(
        condition.field,
        customVariables,
        schema,
        fieldOptions,
        [],
        customListVariables,
      );
      if (fieldType === "boolean" || typeof processedValue === "boolean") {
        return { [field]: { $nin: [parseNumberIfNeeded(processedValue)] } };
      }
      return { [field]: { $ne: parseNumberIfNeeded(processedValue) } };
    }

    case "$gt":
    case "greater than":
    case ">":
    case "$gte":
    case "greater than or equal":
    case ">=":
    case "$lt":
    case "less than":
    case "<":
    case "$lte":
    case "less than or equal":
    case "<=": {
      const mongoOp = getNumericComparison(operator);
      return { [field]: { [mongoOp]: parseNumberIfNeeded(value) } };
    }

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

    case "array length":
      return { [field]: { $size: parseNumberIfNeeded(value) } };

    case "array empty":
      return { [field]: { $size: 0 } };

    case "array not empty":
      return { [field]: { $not: { $size: 0 } } };

    case "$anyElementTrue":
    case "$allElementsTrue":
    case "$filter":
    case "$min":
    case "$max":
    case "$avg":
    case "$sum":
      return convertArrayOperatorToMongo(field, operator, value);

    case "$switch": {
      if (!value || !value.cases || !Array.isArray(value.cases)) {
        return {};
      }

      const branches = [];

      for (const caseItem of value.cases) {
        if (!caseItem.block || !caseItem.then) {
          continue;
        }

        const caseCondition = convertBlockToMongoExpr(
          caseItem.block,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          [],
        );

        if (caseCondition && Object.keys(caseCondition).length > 0) {
          let thenValue;

          if (caseItem.then && typeof caseItem.then === "string") {
            const customVar = customVariables.find(
              (v) => v.name === caseItem.then,
            );
            if (customVar && customVar.variable) {
              const eqParts = customVar.variable.split("=");
              if (eqParts.length === 2) {
                const latexExpression = eqParts[1].trim();
                try {
                  const mongoExpression =
                    latexToMongoConverter.convertToMongo(latexExpression);
                  if (mongoExpression) {
                    thenValue = mongoExpression;
                  } else {
                    thenValue = `$${caseItem.then}`;
                  }
                } catch (error) {
                  console.warn(
                    `Failed to convert LaTeX expression for then value ${caseItem.then}:`,
                    error,
                  );
                  thenValue = `$${caseItem.then}`;
                }
              } else {
                thenValue = `$${caseItem.then}`;
              }
            } else {
              const listVar = customListVariables.find(
                (v) => v.name === caseItem.then,
              );
              const isField = fieldOptions.some(
                (f) =>
                  (typeof f === "string" && f === caseItem.then) ||
                  (f &&
                    (f.value === caseItem.then ||
                      f.name === caseItem.then ||
                      f.label === caseItem.then ||
                      f.field === caseItem.then)),
              );

              if (listVar || isField) {
                thenValue = `$${caseItem.then}`;
              } else {
                thenValue = caseItem.then;
              }
            }
          } else {
            thenValue = parseNumberIfNeeded(caseItem.then);
          }

          branches.push({
            case: caseCondition,
            then: thenValue,
          });
        }
      }

      const switchExpr = {
        $switch: {
          branches,
        },
      };

      if (
        value.default !== undefined &&
        value.default !== null &&
        value.default !== ""
      ) {
        if (typeof value.default === "string") {
          const customVar = customVariables.find(
            (v) => v.name === value.default,
          );
          if (customVar && customVar.variable) {
            const eqParts = customVar.variable.split("=");
            if (eqParts.length === 2) {
              const latexExpression = eqParts[1].trim();
              try {
                const mongoExpression =
                  latexToMongoConverter.convertToMongo(latexExpression);
                if (mongoExpression) {
                  switchExpr.$switch.default = mongoExpression;
                } else {
                  switchExpr.$switch.default = `$${value.default}`;
                }
              } catch (error) {
                console.warn(
                  `Failed to convert LaTeX expression for default value ${value.default}:`,
                  error,
                );
                switchExpr.$switch.default = `$${value.default}`;
              }
            } else {
              switchExpr.$switch.default = `$${value.default}`;
            }
          } else {
            const listVar = customListVariables.find(
              (v) => v.name === value.default,
            );
            const isField = fieldOptions.some(
              (f) =>
                (typeof f === "string" && f === value.default) ||
                (f &&
                  (f.value === value.default ||
                    f.name === value.default ||
                    f.label === value.default ||
                    f.field === value.default)),
            );

            if (listVar || isField) {
              switchExpr.$switch.default = `$${value.default}`;
            } else {
              switchExpr.$switch.default = value.default;
            }
          }
        } else {
          switchExpr.$switch.default = parseNumberIfNeeded(value.default);
        }
      }

      return { $expr: switchExpr };
    }

    default:
      return { [field]: parseNumberIfNeeded(value) };
  }
};

const convertBlockToMongoExpr = (
  block,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  customBlocksWithFalseValue = [],
) => {
  if (!block) {
    return {};
  }

  if (block.type === "condition" || block.category === "condition") {
    return convertConditionToProjectExpr(
      block,
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
    );
  }

  if (!block.children || block.children.length === 0) {
    return {};
  }

  const conditions = [];

  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      if (
        child.customBlockName &&
        customBlocksWithFalseValue.some(
          (custom_block) => custom_block.id === child.id,
        )
      ) {
        return;
      }

      const nestedCondition = convertBlockToMongoExpr(
        child,
        schema,
        fieldOptions,
        customVariables,
        customListVariables,
        customBlocksWithFalseValue,
      );
      if (nestedCondition && Object.keys(nestedCondition).length > 0) {
        conditions.push(nestedCondition);
      }
    } else {
      const condition = convertConditionToProjectExpr(
        child,
        schema,
        fieldOptions,
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

  const logic = (block.logic || "and").toLowerCase();
  if (logic === "or") {
    return { $or: conditions };
  } else {
    return { $and: conditions };
  }
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
                : true;
  }

  return true;
};

const convertConditionToProjectExpr = (
  condition,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
) => {
  const operator = condition.operator;

  if (operator !== "$switch" && (!condition.field || !condition.operator)) {
    return {};
  }

  if (!condition.operator) {
    return {};
  }
  const value = condition.value;
  const field = condition.field;

  const fieldPath = `$${field}`;

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
      if (value && typeof value === "object" && value.children) {
        const conditionsForMap = convertBlockToMongo(
          value,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          true,
          {},
          [],
          field,
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

          return getBooleanSwitch(condition, value)
            ? anyElementExpr
            : { $not: anyElementExpr };
        }
      }
      if (typeof value === "boolean") {
        if (value) {
          return { $anyElementTrue: { $ifNull: [fieldPath, []] } };
        } else {
          return {
            $not: {
              $anyElementTrue: { $ifNull: [fieldPath, []] },
            },
          };
        }
      }
      if (value !== undefined && value !== null && value !== "") {
        const anyElementExpr = {
          $anyElementTrue: {
            $map: {
              input: { $ifNull: [fieldPath, []] },
              in: { $eq: ["$$this", value] },
            },
          },
        };

        return getBooleanSwitch(condition, value)
          ? anyElementExpr
          : { $not: anyElementExpr };
      }

      return getBooleanSwitch(condition, value)
        ? { $anyElementTrue: { $ifNull: [fieldPath, []] } }
        : { $not: { $anyElementTrue: { $ifNull: [fieldPath, []] } } };

    case "$allElementsTrue":
      if (value && typeof value === "object" && value.children) {
        const conditionsForMap = convertBlockToMongo(
          value,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          true,
          {},
          [],
          field,
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

          return getBooleanSwitch(condition, value)
            ? allElementExpr
            : { $not: allElementExpr };
        }
      }
      if (typeof value === "boolean") {
        if (value) {
          return { $allElementsTrue: { $ifNull: [fieldPath, []] } };
        } else {
          return {
            $not: {
              $allElementsTrue: { $ifNull: [fieldPath, []] },
            },
          };
        }
      }
      if (value !== undefined && value !== null && value !== "") {
        const allElementExpr = {
          $allElementsTrue: {
            $map: {
              input: { $ifNull: [fieldPath, []] },
              in: { $eq: ["$$this", value] },
            },
          },
        };

        return getBooleanSwitch(condition, value)
          ? allElementExpr
          : { $not: allElementExpr };
      }

      return getBooleanSwitch(condition, value)
        ? { $allElementsTrue: { $ifNull: [fieldPath, []] } }
        : { $not: { $allElementsTrue: { $ifNull: [fieldPath, []] } } };

    case "$filter":
      if (value && typeof value === "object" && value.children) {
        const filterCondition = convertBlockToMongo(
          value,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          true,
          {},
          [],
          field,
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
      return { $ne: [fieldPath, null] };

    case "$switch": {
      if (!value || !value.cases || !Array.isArray(value.cases)) {
        return {};
      }

      const branches = [];

      for (const caseItem of value.cases) {
        if (!caseItem.block || !caseItem.then) {
          continue;
        }

        const caseCondition = convertBlockToMongoExpr(
          caseItem.block,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          [],
        );

        if (caseCondition && Object.keys(caseCondition).length > 0) {
          let thenValue;

          if (caseItem.then && typeof caseItem.then === "string") {
            const customVar = customVariables.find(
              (v) => v.name === caseItem.then,
            );
            if (customVar && customVar.variable) {
              const eqParts = customVar.variable.split("=");
              if (eqParts.length === 2) {
                const latexExpression = eqParts[1].trim();
                try {
                  const mongoExpression =
                    latexToMongoConverter.convertToMongo(latexExpression);
                  if (mongoExpression) {
                    thenValue = mongoExpression;
                  } else {
                    thenValue = `$${caseItem.then}`;
                  }
                } catch (error) {
                  console.warn(
                    `Failed to convert LaTeX expression for then value ${caseItem.then}:`,
                    error,
                  );
                  thenValue = `$${caseItem.then}`;
                }
              } else {
                thenValue = `$${caseItem.then}`;
              }
            } else {
              const listVar = customListVariables.find(
                (v) => v.name === caseItem.then,
              );
              const isField = fieldOptions.some(
                (f) =>
                  (typeof f === "string" && f === caseItem.then) ||
                  (f &&
                    (f.value === caseItem.then ||
                      f.name === caseItem.then ||
                      f.label === caseItem.then ||
                      f.field === caseItem.then)),
              );

              if (listVar || isField) {
                thenValue = `$${caseItem.then}`;
              } else {
                thenValue = caseItem.then;
              }
            }
          } else {
            thenValue = parseNumberIfNeeded(caseItem.then);
          }

          branches.push({
            case: caseCondition,
            then: thenValue,
          });
        }
      }

      const switchExpr = {
        $switch: {
          branches,
        },
      };

      if (
        value.default !== undefined &&
        value.default !== null &&
        value.default !== ""
      ) {
        if (typeof value.default === "string") {
          const customVar = customVariables.find(
            (v) => v.name === value.default,
          );
          if (customVar && customVar.variable) {
            const eqParts = customVar.variable.split("=");
            if (eqParts.length === 2) {
              const latexExpression = eqParts[1].trim();
              try {
                const mongoExpression =
                  latexToMongoConverter.convertToMongo(latexExpression);
                if (mongoExpression) {
                  switchExpr.$switch.default = mongoExpression;
                } else {
                  switchExpr.$switch.default = `$${value.default}`;
                }
              } catch (error) {
                console.warn(
                  `Failed to convert LaTeX expression for default value ${value.default}:`,
                  error,
                );
                switchExpr.$switch.default = `$${value.default}`;
              }
            } else {
              switchExpr.$switch.default = `$${value.default}`;
            }
          } else {
            const listVar = customListVariables.find(
              (v) => v.name === value.default,
            );
            const isField = fieldOptions.some(
              (f) =>
                (typeof f === "string" && f === value.default) ||
                (f &&
                  (f.value === value.default ||
                    f.name === value.default ||
                    f.label === value.default ||
                    f.field === value.default)),
            );

            if (listVar || isField) {
              switchExpr.$switch.default = `$${value.default}`;
            } else {
              switchExpr.$switch.default = value.default;
            }
          }
        } else {
          switchExpr.$switch.default = parseNumberIfNeeded(value.default);
        }
      }

      return switchExpr;
    }

    default:
      return { $eq: [fieldPath, parseNumberIfNeeded(value)] };
  }
};

const convertConditionToMongoExpr = (
  condition,
  customVariables = [],
  customListVariables = [],
  schema = {},
  fieldOptions = [],
  arrayFieldName = null,
) => {
  const operator = condition.operator;

  if (operator !== "$switch" && (!condition.field || !condition.operator)) {
    return {};
  }

  if (!condition.operator) {
    return {};
  }
  const value = condition.value;
  const field = condition.field;

  const safeCustomVariables = Array.isArray(customVariables)
    ? customVariables
    : [];

  const customVar = safeCustomVariables.find((v) => v.name === field);
  let fieldPath;

  if (customVar && customVar.variable) {
    const eqParts = customVar.variable.split("=");
    if (eqParts.length === 2) {
      const latexExpression = eqParts[1].trim();
      try {
        const mongoExpression =
          latexToMongoConverter.convertToMongo(latexExpression);
        if (mongoExpression) {
          fieldPath = convertToArrayContext(mongoExpression, arrayFieldName);
        } else {
          if (arrayFieldName && field.startsWith(`${arrayFieldName}.`)) {
            const fieldForArray = field.substring(arrayFieldName.length + 1);
            fieldPath = `$$this.${fieldForArray}`;
          } else {
            fieldPath = `$${field}`;
          }
        }
      } catch (error) {
        console.warn(
          `Failed to convert LaTeX expression for variable ${field}:`,
          error,
        );
        if (arrayFieldName && field.startsWith(`${arrayFieldName}.`)) {
          const fieldForArray = field.substring(arrayFieldName.length + 1);
          fieldPath = `$$this.${fieldForArray}`;
        } else {
          fieldPath = `$${field}`;
        }
      }
    } else {
      if (arrayFieldName && field.startsWith(`${arrayFieldName}.`)) {
        const fieldForArray = field.substring(arrayFieldName.length + 1);
        fieldPath = `$$this.${fieldForArray}`;
      } else {
        fieldPath = `$${field}`;
      }
    }
  } else {
    let fieldForArray = field;

    if (arrayFieldName && field.startsWith(`${arrayFieldName}.`)) {
      fieldForArray = field.substring(arrayFieldName.length + 1);
      fieldPath = `$$this.${fieldForArray}`;
    } else if (arrayFieldName && !field.includes(".")) {
      fieldPath = `$$this.${field}`;
    } else {
      if (!arrayFieldName && field.includes(".")) {
        const commonArrayPatterns = [
          "prv_candidates",
          "candidates",
          "detections",
        ];
        for (const pattern of commonArrayPatterns) {
          if (field.startsWith(`${pattern}.`)) {
            fieldForArray = field.substring(pattern.length + 1);
            fieldPath = `$$this.${fieldForArray}`;
            break;
          }
        }
      }

      if (!fieldPath) {
        fieldPath = `$${field}`;
      }
    }
  }

  const fieldType = getFieldType(
    field,
    customVariables,
    schema,
    fieldOptions,
    [],
    customListVariables,
  );
  const isBooleanField = fieldType === "boolean";
  const isBooleanValue = typeof value === "boolean";

  if (typeof fieldPath === "object" && fieldPath !== null) {
    switch (operator) {
      case "$eq":
      case "equals":
      case "=":
        return createComparison(
          "$eq",
          fieldPath,
          value,
          isBooleanField,
          isBooleanValue,
        );

      case "$ne":
      case "not equals":
      case "!=":
        return createComparison(
          "$ne",
          fieldPath,
          value,
          isBooleanField,
          isBooleanValue,
        );

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
        return createComparison(
          "$in",
          fieldPath,
          value,
          isBooleanField,
          isBooleanValue,
        );

      case "$nin":
      case "not in":
        return createComparison(
          "$nin",
          fieldPath,
          value,
          isBooleanField,
          isBooleanValue,
        );

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

  switch (operator) {
    case "$eq":
    case "equals":
    case "=":
      return createComparison(
        "$eq",
        fieldPath,
        value,
        isBooleanField,
        isBooleanValue,
      );

    case "$ne":
    case "not equals":
    case "!=":
      return createComparison(
        "$ne",
        fieldPath,
        value,
        isBooleanField,
        isBooleanValue,
      );

    case "$gt":
    case "greater than":
    case ">":
    case "$gte":
    case "greater than or equal":
    case ">=":
    case "$lt":
    case "less than":
    case "<":
    case "$lte":
    case "less than or equal":
    case "<=":
      return createComparison(
        operator,
        fieldPath,
        value,
        isBooleanField,
        isBooleanValue,
      );

    case "$in":
    case "in":
      return createComparison(
        "$in",
        fieldPath,
        value,
        isBooleanField,
        isBooleanValue,
      );

    case "$nin":
    case "not in":
      return createComparison(
        "$nin",
        fieldPath,
        value,
        isBooleanField,
        isBooleanValue,
      );

    case "$exists":
    case "$isNumber":
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

    case "$switch": {
      if (!value || !value.cases || !Array.isArray(value.cases)) {
        return {};
      }

      const branches = [];

      for (const caseItem of value.cases) {
        if (!caseItem.block || !caseItem.then) {
          continue;
        }

        const caseCondition = convertBlockToMongoExpr(
          caseItem.block,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          [],
        );

        if (caseCondition && Object.keys(caseCondition).length > 0) {
          let thenValue;

          if (caseItem.then && typeof caseItem.then === "string") {
            const customVar = customVariables.find(
              (v) => v.name === caseItem.then,
            );
            if (customVar && customVar.variable) {
              const eqParts = customVar.variable.split("=");
              if (eqParts.length === 2) {
                const latexExpression = eqParts[1].trim();
                try {
                  const mongoExpression =
                    latexToMongoConverter.convertToMongo(latexExpression);
                  if (mongoExpression) {
                    thenValue = mongoExpression;
                  } else {
                    thenValue = arrayFieldName
                      ? `$$this.${caseItem.then}`
                      : `$${caseItem.then}`;
                  }
                } catch (error) {
                  console.warn(
                    `Failed to convert LaTeX expression for then value ${caseItem.then}:`,
                    error,
                  );
                  thenValue = arrayFieldName
                    ? `$$this.${caseItem.then}`
                    : `$${caseItem.then}`;
                }
              } else {
                thenValue = arrayFieldName
                  ? `$$this.${caseItem.then}`
                  : `$${caseItem.then}`;
              }
            } else {
              const listVar = customListVariables.find(
                (v) => v.name === caseItem.then,
              );
              const isField = fieldOptions.some(
                (f) =>
                  (typeof f === "string" && f === caseItem.then) ||
                  (f &&
                    (f.value === caseItem.then ||
                      f.name === caseItem.then ||
                      f.label === caseItem.then ||
                      f.field === caseItem.then)),
              );

              if (listVar || isField) {
                if (
                  arrayFieldName &&
                  caseItem.then.startsWith(`${arrayFieldName}.`)
                ) {
                  const fieldForArray = caseItem.then.substring(
                    arrayFieldName.length + 1,
                  );
                  thenValue = `$$this.${fieldForArray}`;
                } else {
                  thenValue = `$${caseItem.then}`;
                }
              } else {
                thenValue = caseItem.then;
              }
            }
          } else {
            thenValue = parseNumberIfNeeded(caseItem.then);
          }

          branches.push({
            case: caseCondition,
            then: thenValue,
          });
        }
      }

      const switchExpr = {
        $switch: {
          branches,
        },
      };

      if (
        value.default !== undefined &&
        value.default !== null &&
        value.default !== ""
      ) {
        if (typeof value.default === "string") {
          const customVar = customVariables.find(
            (v) => v.name === value.default,
          );
          if (customVar && customVar.variable) {
            const eqParts = customVar.variable.split("=");
            if (eqParts.length === 2) {
              const latexExpression = eqParts[1].trim();
              try {
                const mongoExpression =
                  latexToMongoConverter.convertToMongo(latexExpression);
                if (mongoExpression) {
                  switchExpr.$switch.default = mongoExpression;
                } else {
                  switchExpr.$switch.default = arrayFieldName
                    ? `$$this.${value.default}`
                    : `$${value.default}`;
                }
              } catch (error) {
                console.warn(
                  `Failed to convert LaTeX expression for default value ${value.default}:`,
                  error,
                );
                switchExpr.$switch.default = arrayFieldName
                  ? `$$this.${value.default}`
                  : `$${value.default}`;
              }
            } else {
              switchExpr.$switch.default = arrayFieldName
                ? `$$this.${value.default}`
                : `$${value.default}`;
            }
          } else {
            const listVar = customListVariables.find(
              (v) => v.name === value.default,
            );
            const isField = fieldOptions.some(
              (f) =>
                (typeof f === "string" && f === value.default) ||
                (f &&
                  (f.value === value.default ||
                    f.name === value.default ||
                    f.label === value.default ||
                    f.field === value.default)),
            );

            if (listVar || isField) {
              if (
                arrayFieldName &&
                value.default.startsWith(`${arrayFieldName}.`)
              ) {
                const fieldForArray = value.default.substring(
                  arrayFieldName.length + 1,
                );
                switchExpr.$switch.default = `$$this.${fieldForArray}`;
              } else {
                switchExpr.$switch.default = `$${value.default}`;
              }
            } else {
              switchExpr.$switch.default = value.default;
            }
          }
        } else {
          switchExpr.$switch.default = parseNumberIfNeeded(value.default);
        }
      }

      return switchExpr;
    }

    default:
      return { $eq: [fieldPath, parseNumberIfNeeded(value)] };
  }
};

const createComparison = (
  operator,
  fieldPath,
  value,
  isBooleanField,
  isBooleanValue,
) => {
  const isBoolean = isBooleanField || isBooleanValue;
  const mongoOp = getNumericComparison(operator);

  if (mongoOp) {
    return { [mongoOp]: [fieldPath, parseNumberIfNeeded(value)] };
  }

  switch (operator) {
    case "$eq":
      if (isBoolean && Array.isArray(value)) {
        return { $in: [fieldPath, value] };
      }
      return {
        $eq: [fieldPath, isBoolean ? value : parseNumberIfNeeded(value)],
      };

    case "$ne":
      if (isBoolean && Array.isArray(value)) {
        return { $nin: [fieldPath, value] };
      }
      return {
        $ne: [fieldPath, isBoolean ? value : parseNumberIfNeeded(value)],
      };

    case "$in":
      return { $in: [fieldPath, Array.isArray(value) ? value : [value]] };

    case "$nin":
      return isBoolean
        ? { $nin: [fieldPath, Array.isArray(value) ? value : [value]] }
        : {
            $not: {
              $in: [fieldPath, Array.isArray(value) ? value : [value]],
            },
          };

    default:
      return { [operator]: [fieldPath, parseNumberIfNeeded(value)] };
  }
};

const getFieldPath = (field, customVariables = [], isInArrayFilter = false) => {
  const safeCustomVariables = Array.isArray(customVariables)
    ? customVariables
    : [];

  const formatFieldPath = (fieldName) =>
    isInArrayFilter ? `$$this.${fieldName}` : fieldName;

  const safeConvertLatex = (latexExpression, fieldName) => {
    try {
      const mongoExpression =
        latexToMongoConverter.convertToMongo(latexExpression);
      return mongoExpression || formatFieldPath(fieldName);
    } catch (error) {
      console.warn(
        `Failed to convert LaTeX expression for field ${fieldName}:`,
        error,
      );
      return formatFieldPath(fieldName);
    }
  };

  const convertCustomVariable = (fieldName, customVar) => {
    const eqParts = customVar.variable.split("=");
    if (eqParts.length === 2) {
      const latexExpression = eqParts[1].trim();
      return safeConvertLatex(latexExpression, fieldName);
    }
    return formatFieldPath(fieldName);
  };

  let fieldName;
  if (typeof field === "string") {
    fieldName = field;
  } else if (field && typeof field === "object") {
    fieldName = field.value || field.name || field.field || String(field);
  } else {
    fieldName = String(field);
  }

  const customVar = safeCustomVariables.find((v) => v.name === fieldName);
  if (customVar && customVar.variable) {
    if (isInArrayFilter) {
      return convertCustomVariable(fieldName, customVar);
    }
    return formatFieldPath(fieldName);
  }

  return formatFieldPath(fieldName);
};

const parseNumberIfNeeded = (value) => {
  if (typeof value === "string" && !isNaN(value) && !isNaN(parseFloat(value))) {
    return parseFloat(value);
  }
  return value;
};

const convertNullLikeStringsToNull = (value) => {
  if (typeof value === "string") {
    const lowerValue = value.toLowerCase();
    if (
      lowerValue === "null" ||
      lowerValue === "none" ||
      lowerValue === "nan"
    ) {
      return null;
    }
  }
  return value;
};

const escapeRegex = (string) => {
  return string.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
};

export const formatMongoAggregation = (pipeline) => {
  return JSON.stringify(pipeline, null, 2);
};

const isValidMongoExpression = (expression, allowNull = true) => {
  if (expression === null || expression === undefined) {
    return allowNull;
  }

  if (typeof expression !== "object") {
    return true;
  }

  if (Array.isArray(expression)) {
    return expression.every((item) => isValidMongoExpression(item, allowNull));
  }

  const keys = Object.keys(expression);
  if (keys.length === 0) {
    return false;
  }

  for (const [key, value] of Object.entries(expression)) {
    if (key === "") {
      return false;
    }

    if (!isValidMongoExpression(value, true)) {
      return false;
    }

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
  if (!Array.isArray(pipeline)) {
    return false;
  }

  if (pipeline.length === 0) {
    return false;
  }

  for (const stage of pipeline) {
    if (!stage || typeof stage !== "object") {
      return false;
    }

    const stageKeys = Object.keys(stage);
    if (stageKeys.length !== 1) {
      return false;
    }

    const stageType = stageKeys[0];
    const stageContent = stage[stageType];

    switch (stageType) {
      case "$match":
        if (!stageContent || typeof stageContent !== "object") {
          return false;
        }
        if (Object.keys(stageContent).length === 0) {
          return false;
        }
        if (!isValidMongoExpression(stageContent)) {
          return false;
        }
        break;

      case "$project":
        if (!stageContent || typeof stageContent !== "object") {
          return false;
        }
        if (Object.keys(stageContent).length === 0) {
          return false;
        }
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
        if (
          !stageContent ||
          (typeof stageContent === "object" &&
            Object.keys(stageContent).length === 0)
        ) {
          return false;
        }
        break;

      default:
        if (stageContent === undefined || stageContent === null) {
          return false;
        }
        break;
    }
  }

  return true;
};

const convertListConditionToMongo = (
  condition,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  variableUsageCounts = {},
) => {
  if (condition.type === "array" && condition.field && condition.operator) {
    return convertListConditionDefinitionToMongo(
      condition,
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  const field = getFieldPath(condition.field, customVariables, false);
  const operator = condition.operator;
  const value = condition.value;

  if (condition.isListVariable) {
    return {};
  }

  if (value && typeof value === "object" && value.type === "array") {
    return convertArrayValueToMongo(
      value,
      operator,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

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
                      : true,
      },
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
      variableUsageCounts,
    );
  }

  return convertArrayOperatorToMongo(field, operator, value, customVariables);
};

const convertListConditionDefinitionToMongo = (
  listCondition,
  schema = {},
  fieldOptions = [],
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
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
          arrayField,
        );
        const anyElementTrueExpr = {
          $anyElementTrue: {
            $map: {
              input: { $ifNull: [`$${arrayField}`, []] },
              in: conditionsForMap,
            },
          },
        };

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
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
          arrayField,
        );
        const allElementsTrueExpr = {
          $allElementsTrue: {
            $map: {
              input: { $ifNull: [`$${arrayField}`, []] },
              in: conditionsForMap,
            },
          },
        };

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
          schema,
          fieldOptions,
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
          arrayField,
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
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          {},
          [],
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
          arrayField,
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
        const conditionsForMap = convertBlockToMongo(
          nestedConditions,
          {},
          [],
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
          arrayField,
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
          {},
          [],
          customVariables,
          customListVariables,
          true,
          variableUsageCounts,
          [],
          arrayField,
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

const convertToArrayContext = (mongoExpression, arrayFieldName = null) => {
  if (!mongoExpression || typeof mongoExpression !== "object") {
    return mongoExpression;
  }

  try {
    const clonedExpression = JSON.parse(JSON.stringify(mongoExpression));

    const convertFields = (obj) => {
      if (typeof obj === "string" && !obj.startsWith("$$")) {
        if (obj.startsWith("$")) {
          const fieldName = obj.substring(1);

          if (arrayFieldName && fieldName.includes(".")) {
            const parts = fieldName.split(".");
            const firstPart = parts[0];

            if (firstPart === arrayFieldName) {
              const subPath = parts.slice(1).join(".");
              return `$$this.${subPath}`;
            }
          }

          return obj;
        }
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
  } catch (error) {
    console.warn("Failed to convert expression to array context:", error);
    return mongoExpression;
  }
};

const handleInlinedVariableCondition = (mongoExpression, operator, value) => {
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
      return { $expr: { $eq: [mongoExpression, parseNumberIfNeeded(value)] } };
  }
};

const convertSwitchCaseToMongo = (
  switchCondition,
  schema = {},
  fieldOptions = [],
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
) => {
  if (
    !switchCondition ||
    !switchCondition.value ||
    !switchCondition.value.cases
  ) {
    return null;
  }

  const branches = [];

  for (const caseItem of switchCondition.value.cases) {
    if (!caseItem.block || !caseItem.then) {
      continue;
    }

    const caseCondition = convertBlockToMongoExpr(
      caseItem.block,
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
      [],
    );

    if (caseCondition && Object.keys(caseCondition).length > 0) {
      let thenValue;

      if (caseItem.then && typeof caseItem.then === "string") {
        const customVar = customVariables.find((v) => v.name === caseItem.then);
        if (customVar && customVar.variable) {
          const eqParts = customVar.variable.split("=");
          if (eqParts.length === 2) {
            const latexExpression = eqParts[1].trim();
            try {
              const mongoExpression =
                latexToMongoConverter.convertToMongo(latexExpression);
              if (mongoExpression) {
                thenValue = mongoExpression;
              } else {
                thenValue = `$${caseItem.then}`;
              }
            } catch (error) {
              console.warn(
                `Failed to convert LaTeX expression for then value ${caseItem.then}:`,
                error,
              );
              thenValue = `$${caseItem.then}`;
            }
          } else {
            thenValue = `$${caseItem.then}`;
          }
        } else {
          const listVar = customListVariables.find(
            (v) => v.name === caseItem.then,
          );
          const isField = fieldOptions.some(
            (f) =>
              (typeof f === "string" && f === caseItem.then) ||
              (f &&
                (f.value === caseItem.then ||
                  f.name === caseItem.then ||
                  f.label === caseItem.then ||
                  f.field === caseItem.then)),
          );

          if (listVar || isField) {
            thenValue = `$${caseItem.then}`;
          } else {
            thenValue = caseItem.then;
          }
        }
      } else {
        thenValue = parseNumberIfNeeded(caseItem.then);
      }

      branches.push({
        case: caseCondition,
        then: thenValue,
      });
    }
  }

  const switchExpr = {
    $switch: {
      branches,
    },
  };

  if (
    switchCondition.value.default !== undefined &&
    switchCondition.value.default !== null &&
    switchCondition.value.default !== ""
  ) {
    if (typeof switchCondition.value.default === "string") {
      const customVar = customVariables.find(
        (v) => v.name === switchCondition.value.default,
      );
      if (customVar && customVar.variable) {
        const eqParts = customVar.variable.split("=");
        if (eqParts.length === 2) {
          const latexExpression = eqParts[1].trim();
          try {
            const mongoExpression =
              latexToMongoConverter.convertToMongo(latexExpression);
            if (mongoExpression) {
              switchExpr.$switch.default = mongoExpression;
            } else {
              switchExpr.$switch.default = `$${switchCondition.value.default}`;
            }
          } catch (error) {
            console.warn(
              `Failed to convert LaTeX expression for default value ${switchCondition.value.default}:`,
              error,
            );
            switchExpr.$switch.default = `$${switchCondition.value.default}`;
          }
        } else {
          switchExpr.$switch.default = `$${switchCondition.value.default}`;
        }
      } else {
        const listVar = customListVariables.find(
          (v) => v.name === switchCondition.value.default,
        );
        const isField = fieldOptions.some(
          (f) =>
            (typeof f === "string" && f === switchCondition.value.default) ||
            (f &&
              (f.value === switchCondition.value.default ||
                f.name === switchCondition.value.default ||
                f.field === switchCondition.value.default)),
        );

        if (listVar || isField) {
          switchExpr.$switch.default = `$${switchCondition.value.default}`;
        } else {
          switchExpr.$switch.default = switchCondition.value.default;
        }
      }
    } else {
      switchExpr.$switch.default = parseNumberIfNeeded(
        switchCondition.value.default,
      );
    }
  }

  return switchExpr;
};

const buildVariableDependencyGraph = (
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
) => {
  const dependencyGraph = new Map();

  customVariables.forEach((variable) => {
    if (!variable.name || !variable.variable) {
      return;
    }

    const eqParts = variable.variable.split("=");
    if (eqParts.length !== 2) {
      return;
    }

    const latexExpression = eqParts[1].trim();
    const fieldDependencies =
      latexToMongoConverter.extractFieldDependencies(latexExpression);

    const baseDeps = [];
    const varDeps = [];
    const listVarDeps = [];

    fieldDependencies.forEach((dep) => {
      const isVariable = customVariables.some((v) => v.name === dep);
      const isListVariable = customListVariables.some((lv) => lv.name === dep);
      if (isVariable) {
        varDeps.push(dep);
      } else if (isListVariable) {
        listVarDeps.push(dep);
      } else {
        baseDeps.push(dep);
      }
    });

    dependencyGraph.set(variable.name, {
      baseFields: baseDeps,
      variables: varDeps,
      listVariables: listVarDeps,
      latexExpression,
    });
  });

  customSwitchCases.forEach((switchCase) => {
    if (!switchCase.name || !switchCase.switchCondition) {
      return;
    }

    const baseDeps = new Set();
    const varDeps = new Set();
    const listVarDeps = new Set();

    const extractDepsFromBlock = (block) => {
      if (!block) return;

      if (block.field) {
        const fieldName = getFieldName(block.field);
        const isVariable = customVariables.some((v) => v.name === fieldName);
        const isListVariable = customListVariables.some(
          (lv) => lv.name === fieldName,
        );

        if (isVariable) {
          varDeps.add(fieldName);
        } else if (isListVariable) {
          listVarDeps.add(fieldName);
        } else if (fieldName) {
          baseDeps.add(fieldName);
        }
      }

      if (block.children && Array.isArray(block.children)) {
        block.children.forEach((child) => extractDepsFromBlock(child));
      }
    };

    if (
      switchCase.switchCondition.value &&
      switchCase.switchCondition.value.cases
    ) {
      switchCase.switchCondition.value.cases.forEach((caseItem) => {
        if (caseItem.block) {
          extractDepsFromBlock(caseItem.block);
        }
        if (caseItem.then && typeof caseItem.then === "string") {
          const isVariable = customVariables.some(
            (v) => v.name === caseItem.then,
          );
          const isListVariable = customListVariables.some(
            (lv) => lv.name === caseItem.then,
          );
          if (isVariable) {
            varDeps.add(caseItem.then);
          } else if (isListVariable) {
            listVarDeps.add(caseItem.then);
          }
        }
      });
    }

    if (
      switchCase.switchCondition.value &&
      switchCase.switchCondition.value.default
    ) {
      const defaultVal = switchCase.switchCondition.value.default;
      if (typeof defaultVal === "string") {
        const isVariable = customVariables.some((v) => v.name === defaultVal);
        const isListVariable = customListVariables.some(
          (lv) => lv.name === defaultVal,
        );
        if (isVariable) {
          varDeps.add(defaultVal);
        } else if (isListVariable) {
          listVarDeps.add(defaultVal);
        }
      }
    }

    dependencyGraph.set(switchCase.name, {
      baseFields: Array.from(baseDeps),
      variables: Array.from(varDeps),
      listVariables: Array.from(listVarDeps),
      switchCondition: switchCase.switchCondition,
    });
  });

  return dependencyGraph;
};

const topologicalSortVariables = (variables, dependencyGraph) => {
  const sorted = [];
  const visited = new Set();
  const visiting = new Set();

  const visit = (varName) => {
    if (visited.has(varName)) {
      return true;
    }

    if (visiting.has(varName)) {
      console.warn(`Circular dependency detected for variable: ${varName}`);
      return false;
    }

    visiting.add(varName);

    const deps = dependencyGraph.get(varName);
    if (deps && deps.variables) {
      for (const depVar of deps.variables) {
        if (!visit(depVar)) {
          return false;
        }
      }
    }

    visiting.delete(varName);
    visited.add(varName);
    sorted.push(varName);

    return true;
  };

  for (const varName of variables) {
    if (!visited.has(varName)) {
      visit(varName);
    }
  }

  return sorted;
};

const getVariablesUsedInBlock = (
  block,
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
) => {
  const usedVariables = new Set();

  const processBlock = (b) => {
    if (!b) return;

    if ((b.type === "condition" || b.category === "condition") && b.field) {
      const fieldName = getFieldName(b.field);
      const isCustomVariable = customVariables.some(
        (v) => v.name === fieldName,
      );
      const isSwitchCase = customSwitchCases.some((v) => v.name === fieldName);

      if (isCustomVariable || isSwitchCase) {
        usedVariables.add(fieldName);
      }
    }

    if (b.children && Array.isArray(b.children)) {
      b.children.forEach((child) => {
        processBlock(child);
      });
    }
  };

  processBlock(block);
  return Array.from(usedVariables);
};

const getAllVariableDependencies = (
  varName,
  dependencyGraph,
  visited = new Set(),
) => {
  if (visited.has(varName)) {
    return { baseFields: [], variables: [] };
  }

  visited.add(varName);

  const deps = dependencyGraph.get(varName);
  if (!deps) {
    return { baseFields: [], variables: [], listVariables: [] };
  }

  const allBaseFields = new Set(deps.baseFields);
  const allVariables = new Set(deps.variables);
  const allListVariables = new Set(deps.listVariables || []);

  deps.variables.forEach((depVar) => {
    const transitiveDeps = getAllVariableDependencies(
      depVar,
      dependencyGraph,
      visited,
    );
    transitiveDeps.baseFields.forEach((f) => allBaseFields.add(f));
    transitiveDeps.variables.forEach((v) => allVariables.add(v));
    transitiveDeps.listVariables.forEach((lv) => allListVariables.add(lv));
  });

  return {
    baseFields: Array.from(allBaseFields),
    variables: Array.from(allVariables),
    listVariables: Array.from(allListVariables),
  };
};

const countVariableUsage = (filters, customVariables = []) => {
  const usageCounts = {};

  customVariables.forEach((variable) => {
    if (variable.name) {
      usageCounts[variable.name] = 0;
    }
  });
  filters.forEach((block) => {
    countVariableUsageInBlock(block, usageCounts, customVariables);
  });

  return usageCounts;
};

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

const collectUsedFieldsInBlock = (
  block,
  usedFields,
  customVariables = [],
  customListVariables = [],
) => {
  if (!block) {
    return;
  }

  if (block.type === "condition" || block.category === "condition") {
    collectUsedFieldsInCondition(
      block,
      usedFields,
      customVariables,
      customListVariables,
    );
    return;
  }

  if (!block.children || block.children.length === 0) {
    return;
  }

  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      collectUsedFieldsInBlock(
        child,
        usedFields,
        customVariables,
        customListVariables,
      );
    } else if (child.category === "condition" || child.type === "condition") {
      collectUsedFieldsInCondition(
        child,
        usedFields,
        customVariables,
        customListVariables,
      );
    }
  });
};

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
  const isCustomVariable = customVariables.some((v) => v.name === fieldName);
  const isListVariable = customListVariables.some((v) => v.name === fieldName);

  if (isCustomVariable) {
    usedFields.customVariables.add(fieldName);
    const customVar = customVariables.find((v) => v.name === fieldName);
    if (customVar && customVar.variable) {
      const eqParts = customVar.variable.split("=");
      if (eqParts.length === 2) {
        const latexExpression = eqParts[1].trim();
        const fieldDependencies =
          latexToMongoConverter.extractFieldDependencies(latexExpression);
        fieldDependencies.forEach((depField) => {
          const isDepListVariable = customListVariables.some(
            (lv) => lv.name === depField,
          );
          const isDepCustomVariable = customVariables.some(
            (v) => v.name === depField,
          );
          if (isDepListVariable) {
            usedFields.listVariables.add(depField);
          } else if (isDepCustomVariable) {
            usedFields.customVariables.add(depField);
          } else {
            usedFields.baseFields.add(depField);
          }
        });
      }
    }
  } else if (isListVariable) {
    usedFields.listVariables.add(fieldName);

    const listVariable = customListVariables.find(
      (lv) => lv.name === fieldName,
    );
    if (
      listVariable &&
      listVariable.listCondition &&
      listVariable.listCondition.value
    ) {
      const arrayField = listVariable.listCondition.field;

      try {
        const mongoCondition = convertBlockToMongo(
          listVariable.listCondition.value,
          {},
          [],
          customVariables,
          customListVariables,
          true,
          {},
          [],
        );
        const absoluteFields = extractAbsoluteFieldReferences(mongoCondition);

        absoluteFields.forEach((field) => {
          const isVariableCustom = customVariables.some(
            (v) => v.name === field,
          );
          const isVariableList = customListVariables.some(
            (v) => v.name === field,
          );

          if (isVariableCustom) {
            usedFields.customVariables.add(field);
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
          } else if (isVariableList) {
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

      if (arrayField) {
        usedFields.baseFields.add(arrayField);
      }
    }
  } else {
    usedFields.baseFields.add(fieldName);
  }

  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.type === "array"
  ) {
    const arrayValue = condition.value;

    if (arrayValue.value) {
      if (arrayValue.value.children) {
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
        collectUsedFieldsInCondition(
          arrayValue.value,
          usedFields,
          customVariables,
          customListVariables,
        );
      }
    }

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

  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.children
  ) {
    if (
      ["$anyElementTrue", "$allElementsTrue", "$filter"].includes(
        condition.operator,
      )
    ) {
      try {
        const mongoCondition = convertBlockToMongo(
          condition.value,
          {},
          [],
          customVariables,
          customListVariables,
          true,
          {},
          [],
        );
        const absoluteFields = extractAbsoluteFieldReferences(mongoCondition);
        absoluteFields.forEach((field) => {
          const isVariableCustom = customVariables.some(
            (v) => v.name === field,
          );
          const isVariableList = customListVariables.some(
            (v) => v.name === field,
          );

          if (isVariableCustom) {
            usedFields.customVariables.add(field);
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
          } else if (isVariableList) {
            usedFields.listVariables.add(field);
          } else {
            usedFields.baseFields.add(field);
          }
        });
      } catch (error) {
        console.warn("Failed to extract absolute field references:", error);
      }
    }

    collectUsedFieldsInBlock(
      condition.value,
      usedFields,
      customVariables,
      customListVariables,
    );
  }
};

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
      countVariableUsageInBlock(child, usageCounts, customVariables);
    } else if (child.category === "condition") {
      countVariableUsageInCondition(child, usageCounts, customVariables);
    }
  });
};

const countVariableUsageInCondition = (
  condition,
  usageCounts,
  customVariables = [],
) => {
  if (!condition.field) {
    return;
  }

  const fieldName = getFieldName(condition.field);
  if (Object.prototype.hasOwnProperty.call(usageCounts, fieldName)) {
    usageCounts[fieldName]++;
  }

  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.type === "array"
  ) {
    const arrayValue = condition.value;

    if (arrayValue.value && arrayValue.value.children) {
      countVariableUsageInBlock(arrayValue.value, usageCounts, customVariables);
    }

    if (arrayValue.field) {
      const arrayFieldName = getFieldName(arrayValue.field);
      if (Object.prototype.hasOwnProperty.call(usageCounts, arrayFieldName)) {
        usageCounts[arrayFieldName]++;
      }
    }
  }

  if (
    condition.value &&
    typeof condition.value === "object" &&
    condition.value.children
  ) {
    countVariableUsageInBlock(condition.value, usageCounts, customVariables);
  }
};

const getFieldName = (field) => {
  if (typeof field === "string") {
    return field;
  }

  if (field && typeof field === "object") {
    return field.value || field.name || field.field || String(field);
  }

  return String(field);
};

const convertArrayOperatorToMongo = (field, operator, value) => {
  switch (operator) {
    case "$anyElementTrue":
      if (typeof value === "boolean") {
        if (value) {
          return {
            $expr: {
              $anyElementTrue: { $ifNull: [`$${field}`, []] },
            },
          };
        } else {
          return {
            $expr: {
              $not: {
                $anyElementTrue: { $ifNull: [`$${field}`, []] },
              },
            },
          };
        }
      }
      if (value !== undefined && value !== null && value !== "") {
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
      return {
        $expr: {
          $anyElementTrue: { $ifNull: [`$${field}`, []] },
        },
      };

    case "$allElementsTrue":
      if (typeof value === "boolean") {
        if (value) {
          return {
            $expr: {
              $allElementsTrue: { $ifNull: [`$${field}`, []] },
            },
          };
        } else {
          return {
            $expr: {
              $not: {
                $allElementsTrue: { $ifNull: [`$${field}`, []] },
              },
            },
          };
        }
      }
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
      return {
        $expr: {
          $allElementsTrue: { $ifNull: [`$${field}`, []] },
        },
      };

    case "$filter":
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
      return { [field]: { $exists: true, $type: "array" } };

    case "$min":
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $min: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
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
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $max: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
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
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $avg: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
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
      if (typeof value === "number") {
        return {
          $expr: {
            $eq: [{ $sum: { $ifNull: [`$${field}`, []] } }, value],
          },
        };
      }
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

const extractAbsoluteFieldReferences = (mongoExpression) => {
  const fields = new Set();

  const extractFromValue = (value) => {
    if (typeof value === "string") {
      if (value.startsWith("$") && !value.startsWith("$$")) {
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

const isSimpleCondition = (
  condition,
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
) => {
  if (!condition || typeof condition !== "object") {
    return false;
  }

  if (condition.field) {
    const fieldName = getFieldName(condition.field);
    if (customVariables.some((v) => v.name === fieldName)) {
      return false;
    }
    if (customListVariables.some((v) => v.name === fieldName)) {
      return false;
    }
    if (customSwitchCases.some((v) => v.name === fieldName)) {
      return false;
    }
  }

  if (condition.field) {
    if (
      condition.field.startsWith("prv_candidates") ||
      condition.field.startsWith("fp_hists")
    ) {
      return false;
    }
  }

  if (
    condition.operator &&
    ["$filter", "$map", "$anyElementTrue", "$allElementsTrue"].includes(
      condition.operator,
    )
  ) {
    return false;
  }

  if (condition.field && condition.field.startsWith("candidate.")) {
    return true;
  }

  if (
    condition.field &&
    !condition.field.includes(".") &&
    [
      "$eq",
      "$ne",
      "$gt",
      "$gte",
      "$lt",
      "$lte",
      "$in",
      "$nin",
      "$exists",
      "$regex",
    ].includes(condition.operator)
  ) {
    return true;
  }

  return false;
};

const isBlockEntirelySimple = (
  block,
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
) => {
  if (!block) {
    return false;
  }

  if (block.customBlockName && block.isTrue === false) {
    return false;
  }

  if (block.type === "condition" || block.category === "condition") {
    return isSimpleCondition(
      block,
      customVariables,
      customListVariables,
      customSwitchCases,
    );
  }

  if (block.children && Array.isArray(block.children)) {
    return block.children.every((child) => {
      if (child.category === "block" || child.type === "block") {
        return isBlockEntirelySimple(
          child,
          customVariables,
          customListVariables,
          customSwitchCases,
        );
      } else if (child.type === "condition" || child.category === "condition") {
        return isSimpleCondition(
          child,
          customVariables,
          customListVariables,
          customSwitchCases,
        );
      }
      return false;
    });
  }

  return false;
};

const convertSimpleBlockToMongo = (block, schema = {}, fieldOptions = []) => {
  if (!block) {
    return {};
  }

  if (block.type === "condition" || block.category === "condition") {
    return (
      convertConditionToMongo(
        block,
        schema,
        fieldOptions,
        [],
        [],
        false,
        {},
        null,
      ) || {}
    );
  }

  if (!block.children || block.children.length === 0) {
    return {};
  }

  const conditions = [];

  block.children.forEach((child) => {
    if (child.category === "block" || child.type === "block") {
      const nestedCondition = convertSimpleBlockToMongo(
        child,
        schema,
        fieldOptions,
      );
      if (nestedCondition && Object.keys(nestedCondition).length > 0) {
        conditions.push(nestedCondition);
      }
    } else if (child.category === "condition" || child.type === "condition") {
      const condition = convertConditionToMongo(
        child,
        schema,
        fieldOptions,
        [],
        [],
        {},
        null,
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

  const logic = (block.logic || "and").toLowerCase();
  if (logic === "or") {
    return { $or: conditions };
  } else {
    return { $and: conditions };
  }
};

const separateSimpleAndComplexConditions = (
  filters,
  customVariables = [],
  customListVariables = [],
  customSwitchCases = [],
  schema = {},
  fieldOptions = [],
) => {
  const simpleConditions = {};
  const complexFilters = [];

  filters.forEach((block) => {
    if (
      isBlockEntirelySimple(
        block,
        customVariables,
        customListVariables,
        customSwitchCases,
      )
    ) {
      const mongoCondition = convertSimpleBlockToMongo(
        block,
        schema,
        fieldOptions,
      );
      if (mongoCondition && Object.keys(mongoCondition).length > 0) {
        Object.assign(simpleConditions, mongoCondition);
      }
    } else {
      complexFilters.push(block);
    }
  });

  return {
    simpleConditions,
    complexFilters,
  };
};
