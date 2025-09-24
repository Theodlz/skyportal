import React, { useEffect } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  Paper,
  Chip,
  TextField,
  Autocomplete,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
} from "@mui/material";
import {
  useListConditionDialog,
  useListConditionForm,
  useListConditionSave,
} from "../../../hooks/useDialog";
import { useCurrentBuilder } from "../../../hooks/useContexts";
import BlockComponent from "../block/BlockComponent";
import { postElement } from "../../../ducks/boom_filter_modules";
import { useDispatch, useSelector } from "react-redux";
import { flattenFieldOptions } from "../../../constants/filterConstants";

const SubFieldSelector = ({
  selectedSubField,
  onSubFieldChange,
  subFieldOptions,
  selectedOperator,
}) => {
  // Get numeric subfields for aggregation operators
  const getNumericSubFields = () => {
    return subFieldOptions.filter((option) => option.type === "number");
  };

  const shouldShowSubFieldSelector = ["$min", "$max", "$avg", "$sum"].includes(
    selectedOperator,
  );

  if (!shouldShowSubFieldSelector) {
    return null;
  }

  const numericFields = getNumericSubFields();
  const selectedOption =
    numericFields.find((field) => field.label === selectedSubField) || null;

  return (
    <Box sx={{ mb: 2 }}>
      <Autocomplete
        fullWidth
        options={numericFields}
        getOptionLabel={(option) => option.label || ""}
        value={selectedOption}
        onChange={(_, newValue) => {
          onSubFieldChange(newValue ? newValue.label : "");
        }}
        filterOptions={(options, { inputValue }) => {
          // Custom filter function for better search experience
          const filterValue = inputValue.toLowerCase();
          return options.filter((option) =>
            option.label.toLowerCase().includes(filterValue),
          );
        }}
        renderInput={(params) => (
          <TextField
            {...params}
            label="Subfield"
            variant="outlined"
            placeholder="Type to search for numeric fields..."
            helperText={`Select the numeric field to perform the ${selectedOperator.replace(
              "$",
              "",
            )} operation on`}
          />
        )}
        renderOption={(props, option) => (
          <li {...props} key={option.label}>
            <div>
              <div style={{ fontWeight: "bold" }}>{option.label}</div>
              <div style={{ fontSize: "0.8em", color: "#666" }}>
                Type: {option.type}
              </div>
            </div>
          </li>
        )}
        noOptionsText="No numeric fields found"
        clearOnBlur={false}
        selectOnFocus
        handleHomeEndKeys
      />
    </Box>
  );
};

const ArrayFieldSelector = ({
  selectedArrayField,
  onFieldChange,
  availableArrayFields,
}) => {
  // Transform field objects to work with Autocomplete
  const fieldOptions = availableArrayFields || [];

  // Find the currently selected field object
  const selectedOption =
    fieldOptions.find((field) => field.label === selectedArrayField) || null;

  return (
    <Box sx={{ mb: 2 }}>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
        Select an array field or existing list variable to create conditions
        for:
      </Typography>

      <Autocomplete
        fullWidth
        options={fieldOptions}
        getOptionLabel={(option) => option.label || ""}
        value={selectedOption}
        onChange={(_, newValue) => {
          onFieldChange(newValue ? newValue.label : "");
        }}
        filterOptions={(options, { inputValue }) => {
          // Custom filter function for better search experience
          const filterValue = inputValue.toLowerCase();
          return options.filter((option) =>
            option.label.toLowerCase().includes(filterValue),
          );
        }}
        renderInput={(params) => (
          <TextField
            {...params}
            label="Array Field"
            variant="outlined"
            placeholder="Type to search for array fields..."
            helperText="Start typing to filter available fields"
          />
        )}
        renderOption={(props, option) => (
          <li {...props} key={option.label}>
            <div>
              <div>{option.label}</div>
              {option.isDbVariable && (
                <div style={{ fontSize: "0.8em", color: "#666" }}>
                  Database List Variable
                </div>
              )}
            </div>
          </li>
        )}
        noOptionsText="No array fields found"
        clearOnBlur={false}
        selectOnFocus
        handleHomeEndKeys
      />
    </Box>
  );
};

const ConditionNameInput = ({ conditionName, onNameChange, nameError }) => {
  return (
    <Box sx={{ mb: 2 }}>
      <TextField
        fullWidth
        label="Condition Name"
        value={conditionName}
        onChange={(e) => onNameChange(e.target.value)}
        variant="outlined"
        size="small"
        error={!!nameError}
        helperText={nameError || "Give your list condition a descriptive name"}
        placeholder="e.g., High Priority Observations, Quality Detections"
      />
    </Box>
  );
};

const ListOperatorSelector = ({
  selectedOperator,
  onOperatorChange,
  operators,
}) => {
  const getOperatorDescription = (operator) => {
    const descriptions = {
      $anyElementTrue:
        "Returns true if any element in the array matches the conditions",
      $allElementsTrue:
        "Returns true if all elements in the array match the conditions",
      $filter:
        "Filters the array to return only elements that match the conditions",
      $size: "Checks if the array has a specific number of elements",
      $all: "Returns true if the array contains all specified values",
      $min: "Returns the minimum value from the array elements",
      $max: "Returns the maximum value from the array elements",
      $avg: "Returns the average value from the array elements",
      $sum: "Returns the sum of all array elements",
    };
    return descriptions[operator] || "";
  };

  return (
    <Box sx={{ mb: 2 }}>
      <FormControl fullWidth variant="outlined">
        <InputLabel>List Operator</InputLabel>
        <Select
          value={selectedOperator || ""}
          onChange={(e) => onOperatorChange(e.target.value)}
          label="List Operator"
        >
          {operators?.map((op) => (
            <MenuItem key={op.value} value={op.value}>
              {op.label}
            </MenuItem>
          )) || []}
        </Select>
      </FormControl>
      {selectedOperator && (
        <Typography
          variant="caption"
          color="text.secondary"
          sx={{ mt: 0.5, display: "block" }}
        >
          {getOperatorDescription(selectedOperator)}
        </Typography>
      )}
    </Box>
  );
};

const ConditionBuilderSection = ({
  selectedOperator,
  selectedArrayField,
  conditionName,
  localFilters,
  setLocalFilters,
  subFieldOptions,
}) => {
  const schema = useSelector((state) => state.filter_modules?.schema);
  const fieldOptions = flattenFieldOptions(schema);

  const shouldShowConditionBuilder = [
    "$anyElementTrue",
    "$allElementsTrue",
    "$filter",
    "$map",
  ].includes(selectedOperator);

  if (!shouldShowConditionBuilder) {
    return null;
  }

  const fieldOptionsList = [
    ...fieldOptions,
    ...subFieldOptions.map((subField) => ({
      ...subField,
      block: selectedArrayField, // Block sub-fields under the array field name
    })),
  ];

  return (
    <Box>
      <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
        Define conditions that elements in the list must match:
      </Typography>

      {localFilters.length > 0 && (
        <Paper
          sx={{
            border: 2,
            borderColor: "success.light",
            borderRadius: 2,
            p: 2,
            background: "linear-gradient(90deg, #f0fdf4 60%, #d1fae5 100%)",
          }}
        >
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              mb: 1,
            }}
          >
            <Chip
              label={`Conditions for: ${selectedArrayField}`}
              size="small"
              sx={{
                fontSize: "0.75rem",
                fontWeight: 500,
                color: "success.dark",
                bgcolor: "success.light",
                border: 1,
                borderColor: "success.main",
              }}
            />
            {conditionName && (
              <Typography
                variant="caption"
                sx={{ color: "success.dark", fontStyle: "italic" }}
              >
                "{conditionName}"
              </Typography>
            )}
          </Box>
          <BlockComponent
            block={localFilters[0]}
            parentBlockId={null}
            isRoot={true}
            fieldOptionsList={fieldOptionsList}
            isListDialogOpen={true}
            // Pass the local filters and setter directly as props
            localFilters={localFilters}
            setLocalFilters={setLocalFilters}
          />
        </Paper>
      )}
    </Box>
  );
};

const AddListConditionDialog = () => {
  const {
    listConditionDialog,
    setListConditionDialog,
    filters,
    setFilters,
    createDefaultBlock,
    setCustomListVariables,
    customListVariables,
    localFiltersUpdater,
  } = useCurrentBuilder();
  const schema = useSelector((state) => state.filter_modules?.schema);
  const fieldOptions = flattenFieldOptions(schema);

  // Use our custom hooks
  const form = useListConditionForm(fieldOptions, customListVariables);
  const dialog = useListConditionDialog(
    listConditionDialog,
    filters,
    customListVariables,
    createDefaultBlock,
  );
  const save = useListConditionSave();
  const dispatch = useDispatch();

  // Auto-populate form when opening inline with condition data
  useEffect(() => {
    if (listConditionDialog.open && listConditionDialog.conditionId) {
      // Auto-set the array field from the condition
      console.log(
        "Auto-setting array field from condition:",
        dialog.listFieldNameFromCondition,
      );
      if (dialog.listFieldNameFromCondition && !form.selectedArrayField) {
        console.log(
          "Auto-setting array field from condition:",
          dialog.listFieldNameFromCondition,
        );
        form.setSelectedArrayField(dialog.listFieldNameFromCondition);
        dialog.handleFieldSelection(
          dialog.listFieldNameFromCondition,
          dialog.operatorFromCondition || "",
          form.setSelectedSubField,
        );
      }

      // Auto-set the operator from the condition
      if (dialog.operatorFromCondition && !form.selectedOperator) {
        form.setSelectedOperator(dialog.operatorFromCondition);
        dialog.handleOperatorChange(
          dialog.operatorFromCondition,
          dialog.listFieldNameFromCondition,
        );
      }
    }
  }, [
    listConditionDialog.open,
    listConditionDialog.conditionId,
    dialog.listFieldNameFromCondition,
    dialog.operatorFromCondition,
    form.selectedArrayField,
    form.selectedOperator,
    form.setSelectedArrayField,
    form.setSelectedSubField,
    form.setSelectedOperator,
    dialog.handleFieldSelection,
    dialog.handleOperatorChange,
  ]);

  const handleClose = () => {
    setListConditionDialog({ open: false, blockId: null });
    form.resetForm();
    dialog.resetDialog();
  };

  const handleFieldSelection = (fieldLabel) => {
    form.setSelectedArrayField(fieldLabel);
    // Pass empty string for operator since we haven't selected one yet when field changes
    dialog.handleFieldSelection(fieldLabel, "", form.setSelectedSubField);
  };

  const handleOperatorChange = (newOperator) => {
    form.setSelectedOperator(newOperator);
    dialog.handleOperatorChange(newOperator, form.selectedArrayField);
  };

  const handleSave = async () => {
    const validationError = save.validateSaveConditions(
      dialog.listFieldName,
      form.selectedOperator,
      form.selectedSubField,
      form.conditionName,
      form.nameError,
      dialog.localFilters,
      form.validateConditionName,
    );

    if (validationError) {
      alert(validationError);
      return;
    }

    // Check if conditions are required for this operator
    const operatorNeedsConditions = [
      "$anyElementTrue",
      "$allElementsTrue",
      "$filter",
    ].includes(form.selectedOperator);
    const operatorNeedsSubField = ["$min", "$max", "$avg", "$sum"].includes(
      form.selectedOperator,
    );

    // Create a list condition that wraps the inner conditions
    const listCondition = {
      type: "array",
      field: dialog.listFieldName,
      operator: form.selectedOperator,
      value: operatorNeedsConditions ? dialog.localFilters[0] : null,
      subField: operatorNeedsSubField ? dialog.selectedSubField : null,
      subFieldOptions: form.subFieldOptions,
      name: form.conditionName.trim(),
    };

    const apiResult = await dispatch(
      postElement({
        name: form.conditionName.trim(),
        data: { listCondition: listCondition, type: "array" },
        elements: "listVariables",
      }),
    );

    const success = await save.saveListCondition({
      listFieldName: dialog.listFieldName,
      selectedOperator: form.selectedOperator,
      selectedSubField: form.selectedSubField,
      conditionName: form.conditionName,
      localFilters: dialog.localFilters,
      subFieldOptions: dialog.subFieldOptions,
      saved: apiResult?.status === "success",
      listCondition: listCondition,
      listConditionDialog,
      setCustomListVariables,
      setFilters: setFilters,
      setLocalFilters: localFiltersUpdater, // Use the updater from context
    });

    // Only close if both the API call and the filter update succeeded
    if (success && apiResult?.status === "success") {
      // Add a small delay to ensure state updates have processed
      setTimeout(() => {
        handleClose();
      }, 100);
    }
  };

  const isFormValid = () => {
    const operatorNeedsSubField = ["$min", "$max", "$avg", "$sum"].includes(
      form.selectedOperator,
    );
    const operatorNeedsConditions = [
      "$anyElementTrue",
      "$allElementsTrue",
      "$filter",
      "$map",
    ].includes(form.selectedOperator);

    return (
      form.selectedArrayField &&
      form.selectedOperator &&
      form.conditionName.trim() &&
      !form.nameError &&
      (!operatorNeedsSubField || form.selectedSubField) &&
      (!operatorNeedsConditions ||
        (dialog.localFilters.length > 0 &&
          dialog.localFilters[0].children.length > 0))
    );
  };

  return (
    <Dialog
      open={listConditionDialog.open}
      onClose={handleClose}
      maxWidth="lg"
      fullWidth
      disableRestoreFocus={false}
      slotProps={{
        paper: {
          style: { minHeight: 600 },
          "aria-labelledby": "add-list-condition-dialog-title",
        },
        root: {
          "aria-hidden": false,
        },
      }}
    >
      <DialogTitle id="add-list-condition-dialog-title">
        Add List Condition
      </DialogTitle>
      <DialogContent>
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          <ArrayFieldSelector
            selectedArrayField={form.selectedArrayField}
            onFieldChange={handleFieldSelection}
            availableArrayFields={form.availableArrayFields}
          />

          {form.selectedArrayField && (
            <>
              <ListOperatorSelector
                selectedOperator={form.selectedOperator}
                onOperatorChange={handleOperatorChange}
                operators={dialog.arrayOperators}
              />

              <SubFieldSelector
                selectedSubField={form.selectedSubField}
                onSubFieldChange={form.setSelectedSubField}
                subFieldOptions={dialog.subFieldOptions}
                selectedOperator={form.selectedOperator}
              />

              <ConditionNameInput
                conditionName={form.conditionName}
                onNameChange={form.handleNameChange}
                nameError={form.nameError}
              />

              <ConditionBuilderSection
                selectedOperator={form.selectedOperator}
                selectedArrayField={form.selectedArrayField}
                conditionName={form.conditionName}
                localFilters={dialog.localFilters}
                setLocalFilters={dialog.setLocalFilters}
                subFieldOptions={dialog.subFieldOptions}
              />
            </>
          )}
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleClose}>Cancel</Button>
        <Button
          variant="contained"
          color="primary"
          onClick={handleSave}
          disabled={!isFormValid()}
        >
          Add List Condition
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default AddListConditionDialog;
