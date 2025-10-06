import { useState, useCallback, useEffect } from "react";
import { v4 as uuidv4 } from "uuid";
import { getArrayFieldSubOptions } from "../constants/filterConstants";
import { useCurrentBuilder } from "./useContexts";

export const useDialogStates = () => {
  // Dialog states
  const [saveDialog, setSaveDialog] = useState({ open: false, block: null });
  const [saveName, setSaveName] = useState("");
  const [saveError, setSaveError] = useState("");
  const [specialConditionDialog, setSpecialConditionDialog] = useState({
    open: false,
    blockId: null,
  });
  const [listConditionDialog, setListConditionDialog] = useState({
    open: false,
    blockId: null,
  });
  const [mongoDialog, setMongoDialog] = useState({ open: false });

  // Reset all dialog states
  const resetDialogs = useCallback(() => {
    setSaveDialog({ open: false, block: null });
    setSaveName("");
    setSaveError("");
    setSpecialConditionDialog({ open: false, blockId: null });
    setListConditionDialog({ open: false, blockId: null });
    setMongoDialog({ open: false });
  }, []);

  // Individual dialog actions
  const openSaveDialog = useCallback((block) => {
    setSaveDialog({ open: true, block });
  }, []);

  const closeSaveDialog = useCallback(() => {
    setSaveDialog({ open: false, block: null });
    setSaveName("");
    setSaveError("");
  }, []);

  const openSpecialConditionDialog = useCallback((blockId) => {
    setSpecialConditionDialog({ open: true, blockId });
  }, []);

  const closeSpecialConditionDialog = useCallback(() => {
    setSpecialConditionDialog({ open: false, blockId: null });
  }, []);

  const openListConditionDialog = useCallback((blockId) => {
    setListConditionDialog({ open: true, blockId });
  }, []);

  const closeListConditionDialog = useCallback(() => {
    setListConditionDialog({ open: false, blockId: null });
  }, []);

  return {
    // States
    saveDialog,
    setSaveDialog,
    saveName,
    setSaveName,
    saveError,
    setSaveError,
    specialConditionDialog,
    setSpecialConditionDialog,
    listConditionDialog,
    setListConditionDialog,
    mongoDialog,
    setMongoDialog,

    // Actions
    resetDialogs,
    openSaveDialog,
    closeSaveDialog,
    openSpecialConditionDialog,
    closeSpecialConditionDialog,
    openListConditionDialog,
    closeListConditionDialog,
  };
};

export const useListConditionDialog = (
  listConditionDialog,
  filters,
  customListVariables,
  defaultBlock,
) => {
  const { schema } = useCurrentBuilder(); // Access schema from context
  const [localFilters, setLocalFilters] = useState([]);
  const [listFieldName, setListFieldName] = useState("");
  const [subFieldOptions, setSubFieldOptions] = useState([]);

  // Available operators for array conditions
  const arrayOperators = [
    { value: "$anyElementTrue", label: "Any Element True" },
    { value: "$allElementsTrue", label: "All Elements True" },
    { value: "$filter", label: "Filter" },
    { value: "$map", label: "Map" },
    { value: "$min", label: "Minimum Value" },
    { value: "$max", label: "Maximum Value" },
    { value: "$avg", label: "Average Value" },
    { value: "$sum", label: "Sum of Values" },
  ];

  // Find condition id in filters to retrieve the field name and operator
  const conditionId = listConditionDialog.conditionId;

  // Check all conditions in filters to find the field name and operator associated with the conditionId
  const getConditionDataFromId = () => {
    if (!conditionId) return { field: "", operator: "" };

    return filters.reduce(
      (foundData, block) => {
        if (foundData.field) return foundData; // Stop if already found

        const findDataInBlock = (block) => {
          if (block.id === listConditionDialog.blockId) {
            return block.children.reduce(
              (data, child) => {
                if (child.id === conditionId) {
                  return {
                    field: child.field || "",
                    operator: child.operator || "",
                  };
                }
                if (child.category === "block") {
                  return findDataInBlock(child) || data; // Recurse into child blocks
                }
                return data; // Return current data if not found
              },
              { field: "", operator: "" },
            );
          }

          if (block.children) {
            return block.children.reduce(
              (data, child) => {
                if (data.field) return data; // Stop if already found
                if (child.category === "block") {
                  return findDataInBlock(child) || data; // Recurse into child blocks
                }
                return data; // Return current data if not found
              },
              { field: "", operator: "" },
            );
          }
          return { field: "", operator: "" }; // Return empty if no data found in this block
        };

        return findDataInBlock(block);
      },
      { field: "", operator: "" },
    );
  };

  // Get the field name and operator from the condition
  const conditionData = getConditionDataFromId();
  const listFieldNameFromCondition = conditionData.field;
  const operatorFromCondition = conditionData.operator;

  // Update sub-field options when the field name changes
  useEffect(() => {
    const options = getArrayFieldSubOptions(listFieldNameFromCondition, schema);
    setSubFieldOptions(options);
  }, [listFieldNameFromCondition, schema]);

  const handleFieldSelection = (
    fieldLabel,
    selectedOperator,
    setSelectedSubField,
  ) => {
    setListFieldName(fieldLabel);

    // Check if this is a list variable
    const listVariable = customListVariables.find(
      (lv) => lv.name === fieldLabel,
    );
    if (listVariable && listVariable.listCondition?.subFieldOptions) {
      // Use sub-field options from the list variable
      setSubFieldOptions(listVariable.listCondition.subFieldOptions);
    } else {
      // Use regular array field sub-options
      const options = getArrayFieldSubOptions(fieldLabel, schema);
      setSubFieldOptions(options);
    }

    // Clear selected subfield when array field changes
    setSelectedSubField("");

    // Only initialize with a default block if operator requires conditions
    if (
      ["$anyElementTrue", "$allElementsTrue", "$filter"].includes(
        selectedOperator,
      )
    ) {
      setLocalFilters([defaultBlock("And")]);
    }
  };

  const handleOperatorChange = (newOperator, selectedArrayField) => {
    // Initialize or clear local filters based on whether operator needs conditions
    if (
      ["$anyElementTrue", "$allElementsTrue", "$filter", "$map"].includes(
        newOperator,
      )
    ) {
      if (selectedArrayField && localFilters.length === 0) {
        setLocalFilters([defaultBlock("And")]);
      }
    } else {
      setLocalFilters([]);
    }
  };

  const resetDialog = () => {
    setLocalFilters([]);
    setListFieldName("");
    setSubFieldOptions([]);
  };

  return {
    // State
    localFilters,
    setLocalFilters,
    listFieldName,
    subFieldOptions,
    arrayOperators,

    // Condition data from inline context
    listFieldNameFromCondition,
    operatorFromCondition,

    // Actions
    handleFieldSelection,
    handleOperatorChange,
    resetDialog,

    // Computed
    listFieldNameFromCondition,
  };
};

export const useListConditionForm = (
  fieldOptions,
  customListVariables = [],
) => {
  const [selectedArrayField, setSelectedArrayField] = useState("");
  const [selectedOperator, setSelectedOperator] = useState("");
  const [selectedSubField, setSelectedSubField] = useState("");
  const [conditionName, setConditionName] = useState("");
  const [nameError, setNameError] = useState("");

  // Get all available array fields (including list variables)
  const availableArrayFields = [
    // Include schema array fields - check for both type "array" and isExpandableArray
    ...(fieldOptions || []).filter(
      (field) => field.type === "array" || field.isExpandableArray,
    ),
    ...customListVariables.map((lv) => ({
      label: lv.name,
      type: "array_variable",
      isListVariable: true,
      isDbVariable: true, // All list variables from customListVariables are database variables
    })),
  ];

  const validateConditionName = useCallback((name) => {
    if (!name.trim()) {
      return "Condition name is required";
    }
    if (name.trim().length < 3) {
      return "Condition name must be at least 3 characters long";
    }
    if (name.trim().length > 50) {
      return "Condition name must be less than 50 characters";
    }
    if (!/^[a-zA-Z0-9_\s-]+$/.test(name.trim())) {
      return "Condition name can only contain letters, numbers, spaces, hyphens, and underscores";
    }
    return "";
  }, []);

  const handleNameChange = useCallback(
    (newName) => {
      setConditionName(newName);
      const error = validateConditionName(newName);
      setNameError(error);
    },
    [validateConditionName],
  );

  const resetForm = useCallback(() => {
    setSelectedArrayField("");
    setSelectedOperator("");
    setSelectedSubField("");
    setConditionName("");
    setNameError("");
  }, []);

  const isFormValid = useCallback(() => {
    if (
      !selectedArrayField ||
      !selectedOperator ||
      !conditionName.trim() ||
      nameError
    ) {
      return false;
    }

    // Check if subfield is required for aggregation operators
    const operatorNeedsSubField = ["$min", "$max", "$avg", "$sum"].includes(
      selectedOperator,
    );
    if (operatorNeedsSubField && !selectedSubField.trim()) {
      return false;
    }

    return true;
  }, [
    selectedArrayField,
    selectedOperator,
    selectedSubField,
    conditionName,
    nameError,
  ]);

  return {
    // State
    selectedArrayField,
    selectedOperator,
    selectedSubField,
    conditionName,
    nameError,
    availableArrayFields,

    // Actions
    setSelectedArrayField,
    setSelectedOperator,
    setSelectedSubField,
    handleNameChange,
    resetForm,

    // Computed
    isFormValid: isFormValid(),
    validateConditionName,
  };
};

export const useListConditionSave = () => {
  const validateSaveConditions = (
    listFieldName,
    selectedOperator,
    selectedSubField,
    conditionName,
    nameError,
    localFilters,
    validateConditionName,
  ) => {
    if (!listFieldName.trim()) {
      return "Please select an array field";
    }

    // Check if subfield is required for aggregation operators
    const operatorNeedsSubField = ["$min", "$max", "$avg", "$sum"].includes(
      selectedOperator,
    );
    if (operatorNeedsSubField && !selectedSubField.trim()) {
      return "Please select a subfield for the aggregation operation";
    }

    const nameValidationError = validateConditionName(conditionName);
    if (nameValidationError) {
      return nameValidationError;
    }

    // Check if conditions are required for this operator
    const operatorNeedsConditions = [
      "$anyElementTrue",
      "$allElementsTrue",
      "$filter",
    ].includes(selectedOperator);

    if (
      operatorNeedsConditions &&
      localFilters &&
      (localFilters?.length === 0 || localFilters[0].children.length === 0)
    ) {
      return "Please add at least one condition";
    }

    return null; // No validation errors
  };

  const saveListCondition = useCallback(
    async ({
      listFieldName,
      selectedOperator,
      selectedSubField,
      conditionName,
      localFilters,
      subFieldOptions,
      saved,
      listCondition,
      listConditionDialog,
      setCustomListVariables,
      setFilters,
      setLocalFilters, // Add this parameter for local state update
    }) => {
      if (saved) {
        setCustomListVariables((prev) => {
          return [
            ...prev.filter((lv) => lv.name !== conditionName.trim()),
            {
              name: conditionName.trim(),
              type: "array_variable",
              listCondition: listCondition,
              operator: selectedOperator,
            },
          ];
        });
      }

      // Create new condition for the filter
      const newCondition = {
        id: uuidv4(),
        category: "condition",
        field: conditionName.trim(),
        operator: selectedOperator,
        value: "",
        booleanSwitch: true, // Default to true for new conditions
        createdAt: Date.now(),
        isListVariable: true,
      };

      // Helper function to add condition to block
      const addConditionToBlock = (block) => {
        if (block.id === listConditionDialog.blockId) {
          let updatedChildren = [...block.children];

          // If conditionId is provided, delete the original condition
          if (listConditionDialog.conditionId) {
            updatedChildren = updatedChildren.filter(
              (child) => child.id !== listConditionDialog.conditionId,
            );
          }

          // Add the new list variable condition
          updatedChildren.push(newCondition);

          return {
            ...block,
            children: updatedChildren,
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

      // Update both context filters and local filters if available
      const updateFilters = (prevFilters) => {
        return prevFilters.map(addConditionToBlock);
      };

      // Update context filters
      setFilters(updateFilters);

      // Update local filters if setLocalFilters is provided
      if (setLocalFilters && typeof setLocalFilters === "function") {
        setLocalFilters(updateFilters);
      }

      return true; // Success
    },
    [],
  );

  return {
    validateSaveConditions,
    saveListCondition,
  };
};

export const usePopoverRegistry = (
  conditionId,
  customListVariables,
  setListPopoverAnchor,
) => {
  useEffect(() => {
    // Initialize registries if they don't exist
    if (!window.listPopoverRegistry) {
      window.listPopoverRegistry = new Map();
    }
    if (!window.listVariablePopoverRegistry) {
      window.listVariablePopoverRegistry = new Map();
    }

    // Register this component's callback
    window.listPopoverRegistry.set(conditionId, (anchorElement) => {
      setListPopoverAnchor(anchorElement);
      return true;
    });

    // Set up the global callback if it doesn't exist
    if (!window.openListPopover) {
      window.openListPopover = (conditionId, anchorElement) => {
        const callback = window.listPopoverRegistry?.get(conditionId);
        if (callback) {
          return callback(anchorElement);
        }
        return false;
      };
    }

    // Register list variable callback for this component
    const listVariableCallback = (listVariableName, anchorElement) => {
      const listVar = customListVariables.find(
        (lv) => lv.name === listVariableName,
      );
      if (listVar) {
        setListPopoverAnchor(anchorElement);
        window.currentListVariable = listVar;
        return true;
      }
      return false;
    };

    window.listVariablePopoverRegistry.set(conditionId, listVariableCallback);

    // Set up the global list variable callback if it doesn't exist
    if (!window.openListVariablePopover) {
      window.openListVariablePopover = (listVariableName, anchorElement) => {
        for (const callback of window.listVariablePopoverRegistry?.values() ||
          []) {
          if (callback(listVariableName, anchorElement)) {
            return true;
          }
        }
        return false;
      };
    }

    return () => {
      // Clean up this component's callbacks
      window.listPopoverRegistry?.delete(conditionId);
      window.listVariablePopoverRegistry?.delete(conditionId);

      // If this was the last component, clean up global callbacks
      if (window.listPopoverRegistry?.size === 0) {
        window.openListPopover = null;
      }
      if (window.listVariablePopoverRegistry?.size === 0) {
        window.openListVariablePopover = null;
      }
    };
  }, [conditionId, customListVariables, setListPopoverAnchor]);
};
