export const flattenFieldOptions = (avroSchema) => {
  const flattenedOptions = [];
  const defaultGroupName = "Other Fields";

  // Helper function to capitalize first letter of group names
  const capitalizeGroup = (groupName) => {
    if (!groupName || typeof groupName !== "string") return groupName;
    return groupName.charAt(0).toUpperCase() + groupName.slice(1);
  };

  // Helper to resolve named types
  const resolveNamedType = (typeName, schema) => {
    if (typeof typeName !== "string") return null;

    const findNamedType = (fields) => {
      for (const field of fields) {
        const fieldType = Array.isArray(field.type)
          ? field.type.find((t) => t !== "null")
          : field.type;

        if (typeof fieldType === "object" && fieldType.name === typeName) {
          return fieldType;
        }

        // Recursively search in nested records
        if (
          typeof fieldType === "object" &&
          fieldType.type === "record" &&
          fieldType.fields
        ) {
          const found = findNamedType(fieldType.fields);
          if (found) return found;
        }

        if (
          typeof fieldType === "object" &&
          fieldType.type === "array" &&
          typeof fieldType.items === "object" &&
          fieldType.items.fields
        ) {
          const found = findNamedType(fieldType.items.fields);
          if (found) return found;
        }
      }
      return null;
    };

    return findNamedType(schema.fields || []);
  };

  const getSimpleType = (avroType) => {
    if (typeof avroType === "string") {
      // Map Avro primitive types to our system types
      switch (avroType) {
        case "double":
        case "float":
        case "int":
        case "long":
          return "number";
        case "string":
          return "string";
        case "boolean":
          return "boolean";
        default:
          return "string";
      }
    }

    if (Array.isArray(avroType)) {
      // Handle union types - get the non-null type
      const nonNullType = avroType.find((t) => t !== "null");
      return getSimpleType(nonNullType);
    }

    if (typeof avroType === "object") {
      if (avroType.type === "array") return "array";
      if (avroType.type === "record") return "object";
      return getSimpleType(avroType.type);
    }

    return "string";
  };

  const processField = (field, parentPath = "") => {
    const currentPath = parentPath ? `${parentPath}.${field.name}` : field.name;
    const fieldType = field.type;

    // Handle union types (e.g., ["null", {...}])
    let actualType = fieldType;
    if (Array.isArray(fieldType)) {
      actualType = fieldType.find((t) => t !== "null") || fieldType[0];
    }

    if (typeof actualType === "object") {
      if (actualType.type === "record" && actualType.fields) {
        // For record fields, recursively process nested fields
        actualType.fields.forEach((nestedField) => {
          processField(nestedField, currentPath);
        });
      } else if (actualType.type === "array" && actualType.items) {
        // Handle array items - check if it's a named type reference
        let itemsType = actualType.items;

        // If items is a string, it's a named type reference
        if (typeof itemsType === "string") {
          const resolvedType = resolveNamedType(itemsType, avroSchema);
          if (resolvedType) {
            itemsType = resolvedType;
          }
        }

        // Automatically detect the array type based on structure:
        // - Cross_matches-style: arrays with record items that have union type fields (catalog fields)
        // - Expandable arrays: arrays with simple record items (no union types)
        const isCrossMatchStyle =
          typeof itemsType === "object" &&
          itemsType.type === "record" &&
          itemsType.fields &&
          itemsType.fields.some((catalogField) =>
            Array.isArray(catalogField.type),
          );

        if (isCrossMatchStyle) {
          // For cross_matches-style arrays, create entries for each catalog/database
          // These appear as "arrayName.catalogName" in the main autocomplete
          itemsType.fields.forEach((catalogField) => {
            flattenedOptions.push({
              label: `${currentPath}.${catalogField.name}`,
              type: "array", // Mark as array type since it represents an array element
              group: capitalizeGroup(field.name), // Use the actual field name as group
              parentArray: currentPath,
              arrayObject: catalogField.name,
              catalogName: catalogField.name,
            });
          });
        } else if (
          typeof itemsType === "object" &&
          itemsType.type === "record"
        ) {
          // For expandable arrays (simple record arrays), only show the array itself as selectable
          // The nested fields will be available in the list condition dialog
          flattenedOptions.push({
            label: currentPath,
            type: "array",
            group: parentPath
              ? capitalizeGroup(parentPath.split(".")[0])
              : defaultGroupName,
            arrayItems: itemsType,
            isExpandableArray: true, // Mark as expandable for UI behavior
          });

          // Do NOT process the record fields for the main autocomplete
          // They will be handled separately in the list condition dialog
        } else {
          // Array of primitives
          flattenedOptions.push({
            label: currentPath,
            type: "array",
            group: parentPath
              ? capitalizeGroup(parentPath.split(".")[0])
              : defaultGroupName,
            itemType: getSimpleType(itemsType),
          });
        }
      } else {
        // Other complex types, treat as objects
        flattenedOptions.push({
          label: currentPath,
          type: getSimpleType(actualType),
          group: parentPath
            ? capitalizeGroup(parentPath.split(".")[0])
            : defaultGroupName,
        });
      }
    } else {
      // Simple field types
      flattenedOptions.push({
        label: currentPath,
        type: getSimpleType(actualType),
        group: parentPath
          ? capitalizeGroup(parentPath.split(".")[0])
          : defaultGroupName,
      });
    }
  };

  if (avroSchema && avroSchema.fields) {
    avroSchema.fields.forEach((field) => processField(field));
  }

  return flattenedOptions;
};

const getExpandableArrayFields = (avroSchema, arrayFieldName) => {
  if (!avroSchema || !avroSchema.fields) return [];

  const arrayField = avroSchema.fields.find((f) => f.name === arrayFieldName);
  if (!arrayField) return [];

  let fieldType = arrayField.type;

  // Handle union types
  if (Array.isArray(fieldType)) {
    fieldType = fieldType.find((t) => t !== "null") || fieldType[0];
  }

  if (fieldType.type !== "array" || !fieldType.items) return [];

  let itemsType = fieldType.items;

  // Helper to resolve named types
  const resolveNamedType = (typeName, schema) => {
    if (typeof typeName !== "string") return null;

    const findNamedType = (fields) => {
      for (const field of fields) {
        const fieldTypeName = Array.isArray(field.type)
          ? field.type.find((t) => t !== "null")
          : field.type;

        if (
          typeof fieldTypeName === "object" &&
          fieldTypeName.name === typeName
        ) {
          return fieldTypeName;
        }

        if (
          typeof fieldTypeName === "object" &&
          fieldTypeName.type === "record" &&
          fieldTypeName.fields
        ) {
          const found = findNamedType(fieldTypeName.fields);
          if (found) return found;
        }

        if (
          typeof fieldTypeName === "object" &&
          fieldTypeName.type === "array" &&
          typeof fieldTypeName.items === "object" &&
          fieldTypeName.items.fields
        ) {
          const found = findNamedType(fieldTypeName.items.fields);
          if (found) return found;
        }
      }
      return null;
    };

    return findNamedType(schema.fields || []);
  };

  // If items is a string, it's a named type reference
  if (typeof itemsType === "string") {
    const resolvedType = resolveNamedType(itemsType, avroSchema);
    if (resolvedType) {
      itemsType = resolvedType;
    }
  }

  if (
    typeof itemsType !== "object" ||
    itemsType.type !== "record" ||
    !itemsType.fields
  ) {
    return [];
  }

  const getSimpleType = (avroType) => {
    if (typeof avroType === "string") {
      switch (avroType) {
        case "double":
        case "float":
        case "int":
        case "long":
          return "number";
        case "string":
          return "string";
        case "boolean":
          return "boolean";
        default:
          return "string";
      }
    }

    if (Array.isArray(avroType)) {
      const nonNullType = avroType.find((t) => t !== "null");
      return getSimpleType(nonNullType);
    }

    if (typeof avroType === "object") {
      if (avroType.type === "array") return "array";
      if (avroType.type === "record") return "object";
      return getSimpleType(avroType.type);
    }

    return "string";
  };

  // Convert fields to flattened options
  const nestedFields = [];

  const processNestedField = (field, parentPath = "") => {
    const currentPath = parentPath ? `${parentPath}.${field.name}` : field.name;
    const fieldItemsType = field.type;

    let actualType = fieldItemsType;
    if (Array.isArray(fieldItemsType)) {
      actualType =
        fieldItemsType.find((t) => t !== "null") || fieldItemsType[0];
    }

    if (typeof actualType === "object") {
      if (actualType.type === "record" && actualType.fields) {
        // For nested records, recursively process fields
        actualType.fields.forEach((nestedField) => {
          processNestedField(nestedField, currentPath);
        });
      } else if (actualType.type === "array") {
        // Handle nested arrays
        nestedFields.push({
          label: currentPath,
          type: "array",
          itemType: getSimpleType(actualType.items),
        });
      } else {
        nestedFields.push({
          label: currentPath,
          type: getSimpleType(actualType),
        });
      }
    } else {
      nestedFields.push({
        label: currentPath,
        type: getSimpleType(actualType),
      });
    }
  };

  itemsType.fields.forEach((field) => processNestedField(field));

  return nestedFields.sort((a, b) => a.label.localeCompare(b.label));
};

// export const nestedFieldOptions = defaultFieldOptions;

// export const fieldOptions = flattenFieldOptions(defaultFieldOptions);

export const mongoOperatorLabels = {
  $eq: "=",
  $ne: "≠",
  $gt: ">",
  $gte: "≥",
  $lt: "<",
  $lte: "≤",
  $in: "In",
  $nin: "Not In",
  $anyElementTrue: "Any Element True",
  $allElementsTrue: "All Elements True",
  $filter: "Filter",
  $map: "Map",
  $exists: "Exists",
  $isNumber: "Is Number",
  $min: "Minimum",
  $max: "Maximum",
  $avg: "Average",
  $sum: "Sum",
  $round: "Round",
  $lengthGt: "Length >",
  $lengthLt: "Length <",
  $regex: "Regex Match",
  $type: "Type Check",
};

export const mongoOperatorTypes = {
  $eq: "comparison",
  $ne: "comparison",
  $gt: "comparison",
  $gte: "comparison",
  $lt: "comparison",
  $lte: "comparison",
  $in: "array_boolean",
  $nin: "array_boolean",
  $anyElementTrue: "array",
  $allElementsTrue: "array",
  $filter: "array",
  $map: "array",
  $exists: "exists",
  $isNumber: "exists",
  $min: "aggregation",
  $max: "aggregation",
  $avg: "aggregation",
  $sum: "aggregation",
  $round: "aggregation",
  $lengthGt: "array_single",
  $lengthLt: "array_single",
  $regex: "string",
  $type: "string",
};

// Helper functions for handling nested objects in arrays
export function flattenObject(obj, prefix = "", separator = ".") {
  const flattened = {};

  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      const newKey = prefix ? `${prefix}${separator}${key}` : key;
      const value = obj[key];

      if (
        value !== null &&
        typeof value === "object" &&
        !Array.isArray(value)
      ) {
        // Recursively flatten nested objects
        Object.assign(flattened, flattenObject(value, newKey, separator));
      } else {
        flattened[newKey] = value;
      }
    }
  }

  return flattened;
}

export function unflattenObject(flatObj, separator = ".") {
  const result = {};

  for (const key in flatObj) {
    if (Object.prototype.hasOwnProperty.call(flatObj, key)) {
      const keys = key.split(separator);
      let current = result;

      for (let i = 0; i < keys.length - 1; i++) {
        const k = keys[i];
        if (!(k in current)) {
          current[k] = {};
        }
        current = current[k];
      }

      current[keys[keys.length - 1]] = flatObj[key];
    }
  }

  return result;
}

// Helper to get all array and aggregation operators
export function getArrayOperators() {
  return Object.keys(mongoOperatorTypes).filter(
    (op) =>
      mongoOperatorTypes[op] === "array" ||
      mongoOperatorTypes[op] === "array_boolean" ||
      mongoOperatorTypes[op] === "array_single" ||
      mongoOperatorTypes[op] === "array_number" ||
      mongoOperatorTypes[op] === "aggregation",
  );
}

// Helper to get comparison operators
export function getComparisonOperators() {
  return Object.keys(mongoOperatorTypes).filter(
    (op) => mongoOperatorTypes[op] === "comparison",
  );
}

// Helper to extract array field subkeys for list condition dialog
export function getArrayFieldSubOptions(arrayFieldLabel, schema) {
  // Handle null/undefined input
  if (!arrayFieldLabel || typeof arrayFieldLabel !== "string") {
    return [];
  }

  // Handle different array field formats
  if (arrayFieldLabel.startsWith("cross_matches.")) {
    // Extract the catalog name (e.g., 'AllWISE' from 'cross_matches.AllWISE')
    const catalogName = arrayFieldLabel.replace("cross_matches.", "");

    // Find the cross_matches field in the Avro schema
    const crossMatchesField = schema?.fields?.find(
      (field) => field.name === "cross_matches",
    );
    if (!crossMatchesField) return [];

    let crossMatchType = crossMatchesField.type;
    if (Array.isArray(crossMatchType)) {
      crossMatchType =
        crossMatchType.find((t) => t !== "null") || crossMatchType[0];
    }

    if (
      crossMatchType.type === "array" &&
      crossMatchType.items &&
      crossMatchType.items.type === "record" &&
      crossMatchType.items.fields
    ) {
      // Find the specific catalog field
      const catalogField = crossMatchType.items.fields.find(
        (f) => f.name === catalogName,
      );
      if (!catalogField) return [];

      let catalogType = catalogField.type;
      if (Array.isArray(catalogType)) {
        catalogType = catalogType.find((t) => t !== "null") || catalogType[0];
      }

      if (
        typeof catalogType === "object" &&
        catalogType.type === "record" &&
        catalogType.fields
      ) {
        // Convert Avro fields to our field options format
        const convertAvroField = (field, prefix = "") => {
          const fieldPath = prefix ? `${prefix}.${field.name}` : field.name;
          let fieldType = field.type;

          if (Array.isArray(fieldType)) {
            fieldType = fieldType.find((t) => t !== "null") || fieldType[0];
          }

          const result = [];

          if (typeof fieldType === "object") {
            if (fieldType.type === "record" && fieldType.fields) {
              // Recursively process nested records
              fieldType.fields.forEach((nestedField) => {
                result.push(...convertAvroField(nestedField, fieldPath));
              });
            } else if (fieldType.type === "array") {
              result.push({
                label: fieldPath,
                type: "array",
              });
            } else {
              result.push({
                label: fieldPath,
                type: getAvroFieldType(fieldType.type),
              });
            }
          } else {
            result.push({
              label: fieldPath,
              type: getAvroFieldType(fieldType),
            });
          }

          return result;
        };

        const getAvroFieldType = (avroType) => {
          switch (avroType) {
            case "double":
            case "float":
            case "int":
            case "long":
              return "number";
            case "string":
              return "string";
            case "boolean":
              return "boolean";
            default:
              return "string";
          }
        };

        const allFields = [];
        catalogType.fields.forEach((field) => {
          allFields.push(...convertAvroField(field));
        });

        return allFields.sort((a, b) => a.label.localeCompare(b.label));
      }
    }

    return [];
  }

  // Check if this is an expandable array (direct array field name without dots)
  if (!arrayFieldLabel.includes(".")) {
    // Try to get expandable array fields for any single field name
    const expandableFields = getExpandableArrayFields(schema, arrayFieldLabel);
    if (expandableFields.length > 0) {
      return expandableFields;
    }
  }

  // Legacy format handling (kept for backward compatibility if needed)
  if (arrayFieldLabel.includes(".")) {
    // These are likely cross_matches-style or legacy formats
    return [];
  }

  return [];
}
