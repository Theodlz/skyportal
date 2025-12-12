import React, { useState, useRef, useEffect, useMemo } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  TextField,
  Paper,
  Radio,
  RadioGroup,
  FormControlLabel,
  FormControl,
  Select,
  MenuItem,
  InputLabel,
  Autocomplete,
  IconButton,
} from "@mui/material";
import { Info, Close as CloseIcon, ContentCopy } from "@mui/icons-material";
import { v4 as uuidv4 } from "uuid";
import { useCurrentBuilder } from "../../../hooks/useContexts";
import { postElement } from "../../../ducks/boom_filter_modules";
import { useDispatch } from "react-redux";
import EquationEditor from "equation-editor-react";

const AddVariableDialog = () => {
  const {
    specialConditionDialog,
    setSpecialConditionDialog,
    setCustomVariables,
    setFilters,
    schema,
    customVariables,
    customListVariables,
  } = useCurrentBuilder() || {};

  const dispatch = useDispatch();

  const [variableName, setVariableName] = useState("");
  const [expression, setExpression] = useState("");
  const [context, setContext] = useState("simple");
  const [arrayCollection, setArrayCollection] = useState("");
  const [cursorPos, setCursorPos] = useState(0);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [selectedSuggestion, setSelectedSuggestion] = useState(0);
  const [justSelected, setJustSelected] = useState(false);
  const [preventNextSuggestions, setPreventNextSuggestions] = useState(false);
  const [lastInsertedValue, setLastInsertedValue] = useState("");
  const [copySuccess, setCopySuccess] = useState(false);
  const inputRef = useRef(null);
  const inputElementRef = useRef(null);
  const suggestionsRef = useRef(null);
  const isKeyboardNavigation = useRef(false);

  // Use actual schema from context, no defaults
  const activeSchema = schema || {};

  // Helper to resolve named types in the schema
  const resolveNamedType = (typeName, schema) => {
    if (typeof typeName !== "string" || !schema || !schema.fields) return null;

    for (const field of schema.fields) {
      const fieldType = Array.isArray(field.type)
        ? field.type.find((t) => t !== "null")
        : field.type;

      // Direct match in field type
      if (typeof fieldType === "object" && fieldType.name === typeName) {
        return fieldType;
      }

      // Search in nested records
      if (
        typeof fieldType === "object" &&
        fieldType.type === "record" &&
        fieldType.fields &&
        fieldType.name === typeName
      ) {
        return fieldType;
      }

      // Search in array items
      if (
        typeof fieldType === "object" &&
        fieldType.type === "array" &&
        typeof fieldType.items === "object" &&
        fieldType.items.name === typeName
      ) {
        return fieldType.items;
      }
    }

    return null;
  };

  // Get all available array collections from Avro schema and list conditions
  const getAvailableArrayCollections = () => {
    const collections = [];

    // Helper to get array type from potential union
    const getArrayTypeFromField = (field) => {
      if (typeof field.type === "object" && field.type.type === "array") {
        return field.type;
      }
      if (Array.isArray(field.type)) {
        return field.type.find(
          (t) => typeof t === "object" && t.type === "array",
        );
      }
      return null;
    };

    // Handle Avro schema format
    if (
      activeSchema &&
      activeSchema.fields &&
      Array.isArray(activeSchema.fields)
    ) {
      activeSchema.fields.forEach((field) => {
        const arrayType = getArrayTypeFromField(field);

        if (arrayType) {
          // Check if this array contains records with nested catalogs/unions (like cross_matches)
          let itemsType = arrayType.items;

          // Resolve string references
          if (typeof itemsType === "string") {
            const resolvedType = resolveNamedType(itemsType, activeSchema);
            if (resolvedType) {
              itemsType = resolvedType;
            }
          }

          // Check if items are records with union-type fields (catalog structure)
          const hasCatalogStructure =
            itemsType &&
            typeof itemsType === "object" &&
            itemsType.type === "record" &&
            itemsType.fields &&
            itemsType.fields.some(
              (f) =>
                Array.isArray(f.type) &&
                f.type.some(
                  (t) =>
                    t !== "null" &&
                    typeof t === "object" &&
                    t.type === "record" &&
                    t.fields,
                ),
            );

          if (hasCatalogStructure) {
            // Add each catalog as a separate collection
            itemsType.fields.forEach((catalogField) => {
              if (Array.isArray(catalogField.type)) {
                const recordType = catalogField.type.find(
                  (t) =>
                    t !== "null" &&
                    typeof t === "object" &&
                    t.type === "record" &&
                    t.fields,
                );
                if (recordType) {
                  collections.push({
                    name: `${field.name}.${catalogField.name}`,
                    source: "schema",
                    description: `${field.name} → ${catalogField.name}`,
                    parentArray: field.name,
                    catalogField: catalogField.name,
                  });
                }
              }
            });
          } else {
            // Regular array - add as single collection
            collections.push({
              name: field.name,
              source: "schema",
              description: `Schema collection: ${field.name}`,
            });
          }
        }
      });
    }

    // TODO: Add arrays from list conditions when filter structure is available

    return collections;
  };

  const availableArrayCollections = getAvailableArrayCollections();

  // Set default arrayCollection to first available collection
  useEffect(() => {
    if (availableArrayCollections.length > 0 && !arrayCollection) {
      setArrayCollection(availableArrayCollections[0].name);
    }
  }, [availableArrayCollections, arrayCollection]);

  const operators = [
    { symbol: "+", name: "Add", type: "arithmetic" },
    { symbol: "-", name: "Subtract", type: "arithmetic" },
    { symbol: "*", name: "Multiply", type: "arithmetic" },
    { symbol: "\\frac{}{}", name: "Fraction", type: "arithmetic" },
    { symbol: "/", name: "Divide", type: "arithmetic" },
    { symbol: "||", name: "Absolute Value", type: "math" },
    { symbol: "sin(", name: "Sine", type: "math" },
    { symbol: "cos(", name: "Cosine", type: "math" },
    { symbol: "tan(", name: "Tangent", type: "math" },
    { symbol: "sqrt(", name: "Square Root", type: "math" },
    { symbol: "pow(", name: "Power", type: "math" },
    { symbol: "abs(", name: "Absolute", type: "math" },
    { symbol: "round(", name: "Round", type: "math" },
    { symbol: "floor(", name: "Floor", type: "math" },
    { symbol: "ceil(", name: "Ceiling", type: "math" },
  ];

  const getSuggestions = () => {
    const beforeCursor = expression.slice(0, cursorPos);
    const afterCursor = expression.slice(cursorPos);
    const lastWord = beforeCursor.match(/[a-zA-Z._]*$/)?.[0] || "";

    // Don't show suggestions if there's no partial word being typed
    // EXCEPT when in arrayElement context where we want to show "this.*" suggestions
    if ((!lastWord || lastWord.length === 0) && context !== "arrayElement") {
      return [];
    }

    // Don't show suggestions if the cursor is right after a complete field name
    // and there's an operator or space following, OR if we're at the very end
    const isAtEndOfCompleteField =
      lastWord.length > 0 &&
      (afterCursor.length === 0 || /^[\s+\-*/()=<>!&|,]/.test(afterCursor));

    // Only skip suggestions if we're at the end AND it looks like a complete field path
    if (isAtEndOfCompleteField && afterCursor.length === 0) {
      // Check if this looks like a complete field path (contains dots or matches known complete field pattern)
      if (
        lastWord.includes(".") ||
        lastWord.match(/^(this|candidate|[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_])/)
      ) {
        return [];
      }
    }

    const suggestions = [];

    // Helper function to check if adding a suggestion would be meaningful
    const wouldChangeMeaningfully = (suggestionValue) => {
      const lastWordStart = beforeCursor.search(/[a-zA-Z._]*$/);
      const simulatedExpression =
        expression.slice(0, lastWordStart) + suggestionValue + afterCursor;

      // If the simulated expression is the same as current, don't suggest it
      // Also don't suggest if this is the value we just inserted
      return (
        simulatedExpression !== expression &&
        suggestionValue !== lastInsertedValue
      );
    };

    // Arithmetic variable suggestions (all are numerical) - ADD FIRST for priority
    if (customVariables && Array.isArray(customVariables)) {
      customVariables.forEach((variable) => {
        if (
          variable.name &&
          variable.name.toLowerCase().includes(lastWord.toLowerCase()) &&
          wouldChangeMeaningfully(variable.name)
        ) {
          suggestions.push({
            type: "variable",
            display: variable.name,
            value: variable.name,
            fullPath: variable.name,
            description: "Arithmetic Variable",
          });
        }
      });
    }

    // List variable suggestions (only numerical aggregation operators) - ADD SECOND for priority
    if (customListVariables && Array.isArray(customListVariables)) {
      const numericalOperators = ["$min", "$max", "$avg", "$sum"];
      customListVariables.forEach((listVar) => {
        if (
          listVar.name &&
          listVar.listCondition &&
          numericalOperators.includes(listVar.listCondition.operator) &&
          listVar.name.toLowerCase().includes(lastWord.toLowerCase()) &&
          wouldChangeMeaningfully(listVar.name)
        ) {
          const operatorName = listVar.listCondition.operator
            .replace("$", "")
            .toUpperCase();
          suggestions.push({
            type: "listVariable",
            display: listVar.name,
            value: listVar.name,
            fullPath: listVar.name,
            description: `List Variable: ${operatorName}`,
          });
        }
      });
    }

    // Field suggestions for Avro schema
    if (
      activeSchema &&
      activeSchema.fields &&
      Array.isArray(activeSchema.fields)
    ) {
      activeSchema.fields.forEach((field) => {
        if (field.name && field.type) {
          // Get the field type
          const fieldType =
            typeof field.type === "string" ? field.type : field.type?.type;

          // Helper function to extract array type from potential union types
          const getArrayType = () => {
            // Direct array type
            if (fieldType === "array" && typeof field.type === "object") {
              return field.type;
            }
            // Union type (like ["null", {type: "array", ...}])
            if (Array.isArray(field.type)) {
              const arrayTypeInUnion = field.type.find(
                (t) => typeof t === "object" && t.type === "array",
              );
              return arrayTypeInUnion;
            }
            return null;
          };

          // Special handling for arrays in arrayElement context
          if (fieldType === "array" && context === "arrayElement") {
            // Check if arrayCollection is a nested path (like "cross_matches.NED_BetaV3")
            const isNestedPath = arrayCollection.includes(".");

            if (isNestedPath) {
              const [parentArray, catalogName] = arrayCollection.split(".");

              if (field.name === parentArray) {
                const arrayType = getArrayType();
                if (!arrayType) return;

                let itemsType = arrayType.items;

                // Resolve string references
                if (typeof itemsType === "string") {
                  const resolvedType = resolveNamedType(
                    itemsType,
                    activeSchema,
                  );
                  if (resolvedType) {
                    itemsType = resolvedType;
                  }
                }

                // Find the catalog field within the items
                if (
                  itemsType &&
                  typeof itemsType === "object" &&
                  itemsType.type === "record" &&
                  itemsType.fields
                ) {
                  const catalogField = itemsType.fields.find(
                    (f) => f.name === catalogName,
                  );

                  if (catalogField && Array.isArray(catalogField.type)) {
                    const catalogRecordType = catalogField.type.find(
                      (t) =>
                        t !== "null" &&
                        typeof t === "object" &&
                        t.type === "record" &&
                        t.fields,
                    );

                    if (catalogRecordType && catalogRecordType.fields) {
                      catalogRecordType.fields.forEach((catalogSubField) => {
                        const catalogSubFieldType =
                          typeof catalogSubField.type === "string"
                            ? catalogSubField.type
                            : catalogSubField.type?.type;

                        const isExcluded = ["boolean", "array"].includes(
                          catalogSubFieldType,
                        );

                        if (!isExcluded) {
                          const itemPath = `this.${catalogSubField.name}`;

                          if (
                            itemPath
                              .toLowerCase()
                              .includes(lastWord.toLowerCase()) &&
                            (wouldChangeMeaningfully(itemPath) ||
                              lastWord.length < 2)
                          ) {
                            suggestions.push({
                              type: "field",
                              display: catalogSubField.name,
                              fullPath: itemPath,
                              collection: arrayCollection,
                              description: `${catalogName} → ${catalogSubField.name}`,
                            });
                          }
                        }
                      });
                    }
                  }
                }
                return; // Skip further processing
              }
            } else if (field.name === arrayCollection) {
              // Standard array handling (non-nested path)
              const arrayType = getArrayType();
              if (!arrayType) {
                return; // Skip if we couldn't find the array type
              }

              let itemsType = arrayType.items;

              // If items is a string reference, resolve it
              if (typeof itemsType === "string") {
                const resolvedType = resolveNamedType(itemsType, activeSchema);
                if (resolvedType) {
                  itemsType = resolvedType;
                }
              }

              // If the array contains records with fields, suggest them with "this." prefix
              if (
                itemsType &&
                typeof itemsType === "object" &&
                itemsType.type === "record" &&
                itemsType.fields
              ) {
                itemsType.fields.forEach((itemField) => {
                  // Exclude booleans and arrays from suggestions
                  const itemFieldType =
                    typeof itemField.type === "string"
                      ? itemField.type
                      : itemField.type?.type;
                  const isExcluded = ["boolean", "array"].includes(
                    itemFieldType,
                  );

                  if (!isExcluded) {
                    const itemPath = `this.${itemField.name}`;

                    // Show suggestions if it matches the search or if lastWord is empty/very short
                    if (
                      itemPath.toLowerCase().includes(lastWord.toLowerCase()) &&
                      (wouldChangeMeaningfully(itemPath) || lastWord.length < 2)
                    ) {
                      suggestions.push({
                        type: "field",
                        display: itemField.name,
                        fullPath: itemPath,
                        collection: field.name,
                        description: `Array element → ${itemField.name}`,
                      });
                    }
                  }
                });
              }
              return; // Skip further processing for this array field
            }
          }

          // Skip array types in normal context - they can't be used in arithmetic operations
          if (fieldType === "array") {
            return;
          }

          // Exclude booleans from suggestions
          const isExcluded = fieldType === "boolean";

          // Include all fields except booleans for simple types
          if (typeof field.type === "string" || !field.type.type) {
            if (!isExcluded) {
              const prefix =
                context === "arrayElement" && field.name === arrayCollection
                  ? "this"
                  : field.name;
              const fullPath = `${prefix}`;

              if (
                fullPath.toLowerCase().includes(lastWord.toLowerCase()) &&
                wouldChangeMeaningfully(fullPath)
              ) {
                suggestions.push({
                  type: "field",
                  display: field.name,
                  fullPath: fullPath,
                  collection: field.name,
                  description: `Field: ${field.name}`,
                });
              }
            }
            return;
          }

          // If it's a record type, suggest its nested fields (except booleans and arrays)
          // This function recursively processes nested records
          const processNestedRecord = (recordField, parentPath, depth = 0) => {
            if (depth > 5) return; // Prevent infinite recursion

            if (
              recordField.type &&
              typeof recordField.type === "object" &&
              recordField.type.type === "record" &&
              recordField.type.fields
            ) {
              recordField.type.fields.forEach((nestedField) => {
                const nestedFieldType =
                  typeof nestedField.type === "string"
                    ? nestedField.type
                    : nestedField.type?.type;

                const nestedPath = `${parentPath}.${nestedField.name}`;

                // If it's a simple numeric field, suggest it
                if (
                  typeof nestedField.type === "string" &&
                  !["boolean", "array"].includes(nestedField.type)
                ) {
                  if (
                    nestedPath.toLowerCase().includes(lastWord.toLowerCase()) &&
                    wouldChangeMeaningfully(nestedPath)
                  ) {
                    suggestions.push({
                      type: "field",
                      display: nestedField.name,
                      fullPath: nestedPath,
                      collection: field.name,
                      description: `${parentPath.replace(/^[^.]+\.?/, "")} → ${
                        nestedField.name
                      }`.replace(/^→ /, ""),
                    });
                  }
                }
                // If it's a nested record, recurse
                else if (
                  typeof nestedField.type === "object" &&
                  nestedField.type.type === "record"
                ) {
                  processNestedRecord(nestedField, nestedPath, depth + 1);
                }
              });
            }
          };

          if (
            field.type &&
            typeof field.type === "object" &&
            field.type.type === "record" &&
            field.type.fields
          ) {
            const prefix =
              context === "arrayElement" && field.name === arrayCollection
                ? "this"
                : field.name;
            processNestedRecord(field, prefix);
          }
        }
      });
    }

    // Add suggestions for array condition fields if arrayCollection is from a condition
    const selectedCollection = availableArrayCollections.find(
      (col) => col.name === arrayCollection,
    );
    if (
      context === "arrayElement" &&
      selectedCollection &&
      selectedCollection.source === "condition"
    ) {
      // For array conditions, suggest common array element properties
      const commonArrayFields = ["id", "value", "name", "type", "status"];
      commonArrayFields.forEach((field) => {
        const fullPath = `this.${field}`;
        if (
          fullPath.toLowerCase().includes(lastWord.toLowerCase()) &&
          wouldChangeMeaningfully(fullPath)
        ) {
          suggestions.push({
            type: "field",
            display: field,
            fullPath: fullPath,
            collection: arrayCollection,
            description: `Array element → ${field}`,
          });
        }
      });
    }

    // Operator suggestions
    operators.forEach((op) => {
      if (
        (op.name.toLowerCase().includes(lastWord.toLowerCase()) ||
          op.symbol.includes(lastWord)) &&
        wouldChangeMeaningfully(op.symbol)
      ) {
        suggestions.push({
          type: "operator",
          display: op.name,
          value: op.symbol,
          description: op.type,
        });
      }
    });

    return suggestions.slice(0, 20);
  };

  const suggestions = getSuggestions();

  // Update suggestions visibility
  useEffect(() => {
    // Don't show suggestions if we just selected one or are preventing suggestions
    if (justSelected || preventNextSuggestions) return;

    const shouldShow = suggestions.length > 0 && expression.trim().length > 0;
    setShowSuggestions(shouldShow);
    if (shouldShow) {
      setSelectedSuggestion(0);
    }
  }, [suggestions.length, expression, justSelected, preventNextSuggestions]);

  const handleExpressionChange = (e) => {
    setExpression(e.target.value);
    setCursorPos(e.target.selectionStart || 0);
    // Only show suggestions if we didn't just select one and aren't preventing suggestions
    if (!justSelected && !preventNextSuggestions) {
      setShowSuggestions(true);
    }
  };

  const insertSuggestion = (suggestion) => {
    if (!inputElementRef.current) return;

    // Set flag first to prevent suggestions from showing
    setJustSelected(true);
    setShowSuggestions(false);

    const beforeCursor = expression.slice(0, cursorPos);
    const afterCursor = expression.slice(cursorPos);
    const lastWordStart = beforeCursor.search(/[a-zA-Z._]*$/);

    const value =
      suggestion.type === "field" ? suggestion.fullPath : suggestion.value;
    const newExpression =
      expression.slice(0, lastWordStart) + value + afterCursor;

    const newPos = lastWordStart + value.length;

    // Track what was inserted to prevent re-suggesting it
    setLastInsertedValue(value);

    setExpression(newExpression);
    setCursorPos(newPos);
    setTimeout(() => {
      setJustSelected(false);
      setLastInsertedValue(""); // Clear after delay
    }, 1000); // Longer delay to prevent premature re-showing

    // Focus and set cursor position
    setTimeout(() => {
      if (inputElementRef.current) {
        inputElementRef.current.focus();
        inputElementRef.current.setSelectionRange(newPos, newPos);
      }
    }, 0);
  };

  const handleKeyDown = (e) => {
    if (!showSuggestions) return;

    if (e.key === "ArrowDown") {
      e.preventDefault();
      isKeyboardNavigation.current = true;
      const newIndex = (selectedSuggestion + 1) % suggestions.length;
      setSelectedSuggestion(newIndex);
      scrollToSuggestion(newIndex);
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      isKeyboardNavigation.current = true;
      const newIndex =
        (selectedSuggestion - 1 + suggestions.length) % suggestions.length;
      setSelectedSuggestion(newIndex);
      scrollToSuggestion(newIndex);
    } else if (e.key === "Enter" || e.key === "Tab") {
      e.preventDefault();
      e.stopPropagation();
      if (suggestions[selectedSuggestion]) {
        setPreventNextSuggestions(true);
        insertSuggestion(suggestions[selectedSuggestion]);
        // Prevent any further processing of this Enter key
        setTimeout(() => setPreventNextSuggestions(false), 1000);
      }
    } else if (e.key === "Escape") {
      setShowSuggestions(false);
    }
  };

  const scrollToSuggestion = (index) => {
    if (suggestionsRef.current) {
      const suggestionElement = suggestionsRef.current.children[index];
      if (suggestionElement) {
        suggestionElement.scrollIntoView({
          behavior: "smooth",
          block: "nearest",
        });

        // Reset keyboard navigation flag after scrolling completes
        setTimeout(() => {
          isKeyboardNavigation.current = false;
        }, 300);
      }
    }
  };
  console.log("suggestions", suggestions);
  const handleCloseSpecialCondition = () => {
    setSpecialConditionDialog({ open: false, blockId: null, equation: "" });
    setVariableName("");
    setExpression("");
    setContext("simple");
    setArrayCollection("");
  };

  const handleCopyPreview = () => {
    const equation = getPreviewEquation();
    navigator.clipboard
      .writeText(equation)
      .then(() => {
        setCopySuccess(true);
        setTimeout(() => setCopySuccess(false), 2000);
      })
      .catch((err) => {
        console.error("Failed to copy equation:", err);
      });
  };

  const handleAddVariable = () => {
    if (!variableName.trim() || !expression.trim()) {
      alert("Please enter both a variable name and an expression");
      return;
    }

    // Check if a list variable with the same name already exists
    if (customListVariables?.some((lv) => lv.name === variableName)) {
      alert(
        `A list variable with the name "${variableName}" already exists. Please choose a different name.`,
      );
      return;
    }

    // Check if an arithmetic variable with the same name already exists
    if (customVariables?.some((v) => v.name === variableName)) {
      alert(
        `An arithmetic variable with the name "${variableName}" already exists. Please choose a different name.`,
      );
      return;
    }

    const eq = `${variableName} = ${expression}`;

    dispatch(
      postElement({
        name: variableName,
        data: { variable: eq, type: "number" },
        elements: "variables",
      }),
    );

    setCustomVariables((prev) => {
      if (prev.some((v) => v.name === variableName)) return prev;
      return [...prev, { name: variableName, type: "number", variable: eq }];
    });

    // Add a new special condition to the block
    setFilters((prevFilters) => {
      const addConditionToBlock = (block) => {
        if (block.id === specialConditionDialog.blockId) {
          return {
            ...block,
            children: [
              ...block.children,
              {
                id: uuidv4(),
                category: "condition",
                type: "number",
                field: variableName,
                operator: "$eq",
                value: "",
                createdAt: Date.now(),
              },
            ],
          };
        }
        if (block.children) {
          return {
            ...block,
            children: block.children.map((child) =>
              child.category === "block" ? addConditionToBlock(child) : child,
            ),
          };
        }
        return block;
      };
      return prevFilters.map(addConditionToBlock);
    });

    handleCloseSpecialCondition();
  };

  const getPreviewEquation = () => {
    const varName = variableName || "yourVariableName";
    let expr = expression || "yourEquation";

    // Convert division notation to fraction format for better display
    // This function handles complex expressions including parentheses
    const convertDivisionToFraction = (str) => {
      // Helper function to find matching parentheses
      const findMatchingParen = (str, startIdx) => {
        let count = 1;
        for (let i = startIdx + 1; i < str.length; i++) {
          if (str[i] === "(") count++;
          if (str[i] === ")") count--;
          if (count === 0) return i;
        }
        return -1;
      };

      // Helper function to extract operand (handles parentheses, variables, numbers)
      const extractOperand = (str, fromEnd = false) => {
        if (fromEnd) {
          // Extract from end - look for the last complete operand
          const trimmed = str.trim();
          if (trimmed.endsWith(")")) {
            // Find matching opening parenthesis
            let count = 1;
            for (let i = trimmed.length - 2; i >= 0; i--) {
              if (trimmed[i] === ")") count++;
              if (trimmed[i] === "(") count--;
              if (count === 0) {
                return trimmed.substring(i);
              }
            }
          }
          // Extract last number, variable, or function call
          const match = trimmed.match(
            /([a-zA-Z_][a-zA-Z0-9_.]*(?:\([^)]*\))?|\d+(?:\.\d+)?)$/,
          );
          return match ? match[0] : trimmed;
        } else {
          // Extract from start - look for the first complete operand
          const trimmed = str.trim();
          if (trimmed.startsWith("(")) {
            const endIdx = findMatchingParen(trimmed, 0);
            if (endIdx !== -1) {
              return trimmed.substring(0, endIdx + 1);
            }
          }
          // Extract first number, variable, or function call
          const match = trimmed.match(
            /^([a-zA-Z_][a-zA-Z0-9_.]*(?:\([^)]*\))?|\d+(?:\.\d+)?)/,
          );
          return match ? match[0] : trimmed;
        }
      };

      // Process divisions from left to right, being careful about operator precedence
      let result = str;
      let changed = true;

      while (changed) {
        changed = false;
        // Find division operators that are not already inside \frac
        const divisionMatch = result.match(/(.*?)([^\\]|^)\/([^\/].*)/);
        if (divisionMatch && !result.includes("\\frac")) {
          const beforeDiv =
            divisionMatch[1] +
            (divisionMatch[2] === "^" ? "" : divisionMatch[2]);
          const afterDiv = divisionMatch[3];

          // Extract the immediate operands around the division
          const numerator = extractOperand(beforeDiv, true);
          const denominator = extractOperand(afterDiv, false);

          // Get the parts before numerator and after denominator
          const beforeNumerator = beforeDiv.substring(
            0,
            beforeDiv.length - numerator.length,
          );
          const afterDenominator = afterDiv.substring(denominator.length);

          // Don't convert if this looks like it's already a LaTeX fraction
          if (numerator.includes("\\frac") || denominator.includes("\\frac")) {
            break;
          }

          // Recursively convert nested divisions in operands
          const convertedNum = convertDivisionToFraction(numerator);
          const convertedDen = convertDivisionToFraction(denominator);

          result = `${beforeNumerator}\\frac{${convertedNum}}{${convertedDen}}${afterDenominator}`;
          changed = true;
        }
      }

      return result;
    };

    expr = convertDivisionToFraction(expr);

    return `${varName} = ${expr}`;
  };

  const previewEquation = useMemo(
    () => getPreviewEquation(),
    [variableName, expression],
  );

  return (
    <Dialog
      open={specialConditionDialog.open}
      onClose={handleCloseSpecialCondition}
      maxWidth="md"
      fullWidth
      disableRestoreFocus={false}
      PaperProps={{ sx: { minHeight: "420px", maxHeight: "80vh" } }}
    >
      <DialogTitle
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
        }}
      >
        <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
          <Typography variant="h6" component="div">
            Add Arithmetic Variable
          </Typography>
        </Box>
        <IconButton onClick={handleCloseSpecialCondition} size="small">
          <CloseIcon />
        </IconButton>
      </DialogTitle>

      <DialogContent dividers>
        {/* Context Selector */}
        <Box
          sx={{
            mb: 3,
            p: 2,
            bgcolor: "background.paper",
            borderRadius: 1,
            border: "1px solid",
            borderColor: "divider",
          }}
        >
          <Box
            sx={{ display: "flex", alignItems: "flex-start", gap: 1, mb: 2 }}
          >
            <Info fontSize="small" color="primary" />
            <Box>
              <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
                Where will this variable be used?
              </Typography>
              <Typography variant="caption" color="text.secondary">
                This affects how fields are referenced in your expression
              </Typography>
            </Box>
          </Box>

          <FormControl component="fieldset">
            <RadioGroup
              row
              value={context}
              onChange={(e) => setContext(e.target.value)}
            >
              <FormControlLabel
                value="simple"
                control={<Radio size="small" />}
                label={
                  <Typography variant="body2">
                    Simple condition (e.g., variable &gt; 21)
                  </Typography>
                }
              />
              <FormControlLabel
                value="arrayElement"
                control={<Radio size="small" />}
                label={
                  <Typography variant="body2">
                    Array operation (e.g., anyElementTrue)
                  </Typography>
                }
              />
            </RadioGroup>
          </FormControl>

          {context === "arrayElement" && (
            <FormControl fullWidth size="small" sx={{ mt: 2 }}>
              <InputLabel>Which collection is the array from?</InputLabel>
              <Select
                value={arrayCollection}
                onChange={(e) => setArrayCollection(e.target.value)}
                label="Which collection is the array from?"
              >
                {availableArrayCollections.map((collection) => (
                  <MenuItem key={collection.name} value={collection.name}>
                    <Box>
                      <Typography variant="body2">{collection.name}</Typography>
                      <Typography variant="caption" color="text.secondary">
                        {collection.description}
                      </Typography>
                    </Box>
                  </MenuItem>
                ))}
              </Select>
            </FormControl>
          )}
        </Box>

        {/* Variable Name */}
        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
            Variable Name
          </Typography>
          <TextField
            fullWidth
            value={variableName}
            onChange={(e) => setVariableName(e.target.value)}
            placeholder="myVariable"
            size="small"
            autoComplete="off"
            data-form-type="other"
            inputProps={{
              "data-lpignore": "true",
              "data-form-type": "other",
              autoComplete: "off",
            }}
          />
        </Box>

        {/* Expression Builder */}
        <Box sx={{ mb: 3 }}>
          <Typography variant="subtitle2" fontWeight="bold" gutterBottom>
            Expression
          </Typography>
          <Box sx={{ position: "relative" }}>
            <TextField
              ref={inputRef}
              inputRef={inputElementRef}
              fullWidth
              value={expression}
              onChange={handleExpressionChange}
              onKeyDown={handleKeyDown}
              onClick={(e) => setCursorPos(e.target.selectionStart || 0)}
              placeholder="Start typing your expression..."
              size="small"
              autoComplete="off"
              inputProps={{
                style: { fontFamily: "monospace", fontSize: "14px" },
              }}
              sx={{
                "& .MuiOutlinedInput-root": {
                  fontFamily: "monospace",
                },
              }}
            />

            {showSuggestions && suggestions.length > 0 && (
              <Paper
                ref={suggestionsRef}
                sx={{
                  position: "absolute",
                  top: "100%",
                  left: 0,
                  right: 0,
                  zIndex: 1000,
                  width: "100%",
                  mt: 0.5,
                  maxHeight: 250,
                  overflow: "auto",
                  boxShadow: 3,
                }}
              >
                {suggestions.map((suggestion, idx) => (
                  <Box
                    key={idx}
                    onMouseDown={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                    }}
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      insertSuggestion(suggestion);
                    }}
                    onMouseEnter={() => {
                      if (!isKeyboardNavigation.current) {
                        setSelectedSuggestion(idx);
                      }
                    }}
                    sx={{
                      px: 2,
                      py: 1.5,
                      cursor: "pointer",
                      bgcolor:
                        idx === selectedSuggestion
                          ? "primary.100"
                          : "transparent",
                      border:
                        idx === selectedSuggestion
                          ? "1px solid"
                          : "1px solid transparent",
                      borderColor:
                        idx === selectedSuggestion
                          ? "primary.300"
                          : "transparent",
                      "&:hover": {
                        bgcolor:
                          idx === selectedSuggestion
                            ? "primary.100"
                            : "grey.50",
                      },
                      borderBottom: "1px solid",
                      borderBottomColor: "divider",
                      "&:last-child": { borderBottom: "none" },
                      transition: "all 0.15s ease-in-out",
                    }}
                  >
                    <Box
                      sx={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                      }}
                    >
                      <Box>
                        <Typography
                          variant="body2"
                          sx={{ fontFamily: "monospace", fontWeight: "bold" }}
                        >
                          {suggestion.type === "field"
                            ? suggestion.fullPath
                            : suggestion.value}
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {suggestion.description}
                        </Typography>
                      </Box>
                      <Box
                        sx={{
                          px: 1,
                          py: 0.5,
                          borderRadius: 1,
                          bgcolor:
                            suggestion.type === "variable"
                              ? "warning.100"
                              : suggestion.type === "listVariable"
                                ? "success.100"
                                : suggestion.type === "field"
                                  ? "info.100"
                                  : "secondary.100",
                          color:
                            suggestion.type === "variable"
                              ? "warning.800"
                              : suggestion.type === "listVariable"
                                ? "success.800"
                                : suggestion.type === "field"
                                  ? "info.800"
                                  : "secondary.800",
                        }}
                      >
                        <Typography variant="caption" fontWeight="bold">
                          {suggestion.type}
                        </Typography>
                      </Box>
                    </Box>
                  </Box>
                ))}
              </Paper>
            )}
          </Box>
          <Typography
            variant="caption"
            color="text.secondary"
            sx={{ mt: 0.5, display: "block" }}
          >
            Type field names or operators. Use ↑↓ to navigate, Tab/Enter to
            select, Esc to close.
          </Typography>
        </Box>

        {/* Preview with EquationEditor */}
        <Box>
          <Box
            sx={{
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
              mb: 1,
            }}
          >
            <Typography variant="subtitle2" fontWeight="bold">
              Preview
            </Typography>
            <Button
              size="small"
              startIcon={<ContentCopy />}
              onClick={handleCopyPreview}
              variant="outlined"
              sx={{ minWidth: "auto" }}
            >
              {copySuccess ? "Copied!" : "Copy"}
            </Button>
          </Box>
          <Box
            sx={{
              p: 2,
              bgcolor: "background.default",
              borderRadius: 1,
              border: "1px solid",
              borderColor: "divider",
              minHeight: 80,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
            }}
          >
            <EquationEditor
              key={`${variableName}-${expression}`}
              value={previewEquation}
              onChange={() => {}}
              autoCommands="pi theta sqrt sum prod alpha beta gamma rho"
              autoOperatorNames="sin cos tan log ln exp abs"
              style={{ minHeight: 60, fontSize: 32 }}
            />
          </Box>
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={handleCloseSpecialCondition}>Cancel</Button>
        <Button
          variant="contained"
          color="primary"
          onClick={handleAddVariable}
          disabled={!variableName.trim() || !expression.trim()}
        >
          Add Variable
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default AddVariableDialog;
