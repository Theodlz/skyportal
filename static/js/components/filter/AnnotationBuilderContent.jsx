import React, { useState, useEffect } from "react";
import {
  Button,
  Box,
  Typography,
  Paper,
  IconButton,
  FormControl,
  Select,
  MenuItem,
  TextField,
  Autocomplete,
  InputLabel,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Chip,
} from "@mui/material";
import {
  Code as CodeIcon,
  ArrowBack as ArrowBackIcon,
  Add as AddIcon,
  Delete as DeleteIcon,
  ExpandMore as ExpandMoreIcon,
  ChevronRight as ChevronRightIcon,
} from "@mui/icons-material";
import { useNavigate } from "react-router-dom";
import { useDispatch, useSelector } from "react-redux";
import {
  useFilterBuilder,
  useAnnotationBuilder,
} from "../../hooks/useContexts";
import { getFieldOptionsWithVariable } from "../../utils/conditionHelpers";
import AddVariableDialog from "./dialog/AddVariableDialog";
import AddListConditionDialog from "./dialog/AddListConditionDialog";
import SaveBlockDialogMenu from "./block/SaveBlockDialogMenu";
import MongoQueryDialog from "./dialog/MongoQueryDialog";
import { filterBuilderStyles } from "../../styles/componentStyles";
import { styled, lighten, darken } from "@mui/system";
import { latexToMongoConverter } from "../../utils/robustLatexConverter";
import CloseIcon from "@mui/icons-material/Close";
import {
  flattenFieldOptions,
  getArrayFieldSubOptions,
} from "../../constants/filterConstants";
import { fetchSchema } from "../../ducks/boom_filter_modules";

const GroupHeader = styled("div")(({ theme }) => {
  const primaryMain = theme.palette?.primary?.main || "#1976d2";
  const primaryLight = theme.palette?.primary?.light || "#42a5f5";
  const isDark = theme.palette?.mode === "dark";
  return {
    position: "sticky",
    top: "-8px",
    padding: "4px 10px",
    color: primaryMain,
    backgroundColor: isDark
      ? darken(primaryMain, 0.8)
      : lighten(primaryLight, 0.85),
    cursor: "pointer",
    display: "flex",
    alignItems: "center",
    gap: "4px",
    userSelect: "none",
    transition: "background-color 0.2s ease",
    "&:hover": {
      backgroundColor: isDark
        ? darken(primaryMain, 0.7)
        : lighten(primaryLight, 0.75),
    },
  };
});

const GroupItems = styled("ul")({
  padding: 0,
});

const useAnnotationBuilderData = (builderContext) => {
  const { filters, setFilters, createDefaultBlock } = builderContext;

  // Initialize filters if empty
  useEffect(() => {
    if (filters.length === 0) {
      setFilters([createDefaultBlock("And")]);
    }
  }, [filters.length, createDefaultBlock, setFilters]);

  return;
};

const MapAnnotationsDialog = ({
  open,
  onClose,
  arrayField,
  mapProjectionFields,
  setMapProjectionFields,
  onSave,
}) => {
  // Ensure there's always at least one empty field when dialog opens
  useEffect(() => {
    if (open && mapProjectionFields.length === 0) {
      setMapProjectionFields([
        {
          id: Date.now(),
          fieldName: "",
          outputName: "",
          type: "include",
        },
      ]);
    }
  }, [open, mapProjectionFields.length, setMapProjectionFields]);
  // Helper to generate MongoDB $map operator from mapProjectionFields
  // Get output field name for the $map query (first mapProjectionFields outputName, or fallback)
  const getOutputFieldName = () => {
    return arrayField?.outputName
      ? `${arrayField.outputName}`
      : `${arrayField.fieldName}_mapped`;
  };

  // Build the $map query for the output field
  const getMongoMapQuery = () => {
    if (
      !arrayField ||
      !arrayField.fieldName ||
      mapProjectionFields.length === 0
    )
      return null;
    const inObj = {};
    mapProjectionFields.forEach((f) => {
      if (!f.outputName || !f.fieldName) return;
      let value;
      switch (f.type) {
        case "exclude":
          value = 0;
          break;
        case "round":
          value = {
            $round: [
              `$$match.${f.fieldName.label}`,
              typeof f.roundDecimals === "number" ? f.roundDecimals : 4,
            ],
          };
          break;
        case "include":
        default:
          value = `$$match.${f.fieldName.label}`;
      }
      inObj[f.outputName] = value;
    });
    return {
      $map: {
        input: `$${arrayField.fieldName}`,
        as: "match",
        in: inObj,
      },
    };
  };
  const subFields = arrayField?.subFields || [];

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>
        Map Annotations to Array Field
        <IconButton
          aria-label="close"
          onClick={onClose}
          sx={{ position: "absolute", right: 8, top: 8 }}
        >
          <CloseIcon />
        </IconButton>
      </DialogTitle>
      <DialogContent>
        {/* Show selected array field below the title */}
        {arrayField && (
          <Box sx={{ mb: 2 }}>
            <Typography variant="subtitle2">Selected Array Field:</Typography>
            <Typography variant="body1" sx={{ fontWeight: 500, mb: 1 }}>
              {arrayField.fieldName}
            </Typography>
          </Box>
        )}
        {/* Map projection fields UI */}
        <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
          {mapProjectionFields.map((mapField, idx) => (
            <Paper key={mapField.id || idx} variant="outlined" sx={{ p: 2 }}>
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 2,
                  flexWrap: "wrap",
                }}
              >
                {/* Output Name */}
                <TextField
                  label="Annotation Name"
                  value={mapField.outputName}
                  onChange={(e) =>
                    setMapProjectionFields((fields) =>
                      fields.map((f) =>
                        f.id === mapField.id
                          ? { ...f, outputName: e.target.value }
                          : f,
                      ),
                    )
                  }
                  size="small"
                  sx={{ minWidth: 150 }}
                  placeholder={mapField.fieldName}
                />
                {/* Field Selection: autocomplete with subfields only */}
                <Autocomplete
                  value={
                    mapField.fieldName
                      ? subFields
                          .map((sf) => ({ label: sf, name: sf }))
                          .find((opt) => opt.name === mapField.fieldName) ||
                        null
                      : null
                  }
                  onChange={(_, newValue) => {
                    setMapProjectionFields((fields) =>
                      fields.map((f) =>
                        f.id === mapField.id
                          ? {
                              ...f,
                              fieldName:
                                typeof newValue === "string"
                                  ? newValue
                                  : newValue?.name || newValue?.label || "",
                            }
                          : f,
                      ),
                    );
                  }}
                  options={subFields.map((sf) => ({ label: sf, name: sf }))}
                  isOptionEqualToValue={(option, value) =>
                    option.name === value?.name
                  }
                  getOptionLabel={(option) => {
                    if (
                      typeof option.label === "object" &&
                      option.label !== null
                    ) {
                      return option.label.label || "";
                    }
                    if (
                      typeof option.name === "object" &&
                      option.name !== null
                    ) {
                      return option.name.label || "";
                    }
                    return option.label || option.name || "";
                  }}
                  sx={{ minWidth: 250 }}
                  renderInput={(params) => (
                    <TextField
                      {...params}
                      label="Field (subfield)"
                      size="small"
                    />
                  )}
                />
                {/* Type Selector (MUI Select style) */}
                <FormControl size="small" sx={{ minWidth: 120 }}>
                  <InputLabel>Type</InputLabel>
                  <Select
                    value={mapField.type || "include"}
                    onChange={(e) =>
                      setMapProjectionFields((fields) =>
                        fields.map((f) =>
                          f.id === mapField.id
                            ? { ...f, type: e.target.value }
                            : f,
                        ),
                      )
                    }
                    label="Type"
                  >
                    <MenuItem value="include">Include</MenuItem>
                    <MenuItem value="exclude">Exclude</MenuItem>
                    <MenuItem value="round">Round</MenuItem>
                  </Select>
                </FormControl>
                {/* Decimals input for round type */}
                {mapField.type === "round" && (
                  <TextField
                    label="Decimals"
                    type="number"
                    value={
                      typeof mapField.roundDecimals === "number"
                        ? mapField.roundDecimals
                        : 4
                    }
                    onChange={(e) => {
                      const val = Math.max(
                        0,
                        Math.min(10, parseInt(e.target.value) || 0),
                      );
                      setMapProjectionFields((fields) =>
                        fields.map((f) =>
                          f.id === mapField.id
                            ? { ...f, roundDecimals: val }
                            : f,
                        ),
                      );
                    }}
                    size="small"
                    sx={{ width: 100 }}
                    inputProps={{ min: 0, max: 10 }}
                  />
                )}
                {/* Remove Button */}
                <IconButton
                  onClick={() =>
                    setMapProjectionFields((fields) =>
                      fields.filter((f) => f.id !== mapField.id),
                    )
                  }
                  color="error"
                  size="small"
                >
                  <DeleteIcon />
                </IconButton>
              </Box>
            </Paper>
          ))}
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            size="small"
            onClick={() =>
              setMapProjectionFields((fields) => [
                ...fields,
                { id: Date.now(), fieldName: "", outputName: "" },
              ])
            }
          >
            Add Map Annotation
          </Button>
        </Box>
      </DialogContent>
      <DialogActions>
        <Button
          variant="contained"
          color="primary"
          onClick={() => {
            if (onSave) {
              const mongoMapQuery = getMongoMapQuery();
              const outputFieldName = getOutputFieldName();
              onSave({ outputFieldName, mongoMapQuery });
            }
            onClose();
          }}
        >
          Save
        </Button>
        <Button onClick={onClose} color="primary">
          Close
        </Button>
      </DialogActions>
    </Dialog>
  );
};

const AnnotationBuilderContent = ({ onBackToFilterBuilder }) => {
  const { filters, projectionFields, setProjectionFields } =
    useAnnotationBuilder();
  const { hasValidQuery: hasValidFilterQuery } = useFilterBuilder(); // Get filter page validation
  const annotationContext = useAnnotationBuilder();
  const filterContext = useFilterBuilder(); // Get full filter context
  const navigate = useNavigate();
  const dispatch = useDispatch();

  // Use the same schema fetching approach as FilterBuilder
  const filter_stream = useSelector(
    (state) => state.filter_v.stream.name.split(" ")[0],
  );
  const store_schema = useSelector((state) => state.filter_modules?.schema);

  const [schema, setSchema] = useState(null);
  const [fieldOptions, setFieldOptions] = useState([]);

  // State for expandable groups
  const [collapsedGroups, setCollapsedGroups] = useState(new Set());

  // State for map dialog
  const [mapDialogOpen, setMapDialogOpen] = useState(false);
  const [mapDialogFieldId, setMapDialogFieldId] = useState(null);
  const [mapProjectionFields, setMapProjectionFields] = useState([]);

  // Initialize projection fields with objectId if not already present
  React.useEffect(() => {
    // Only add a modifiable empty annotation if none exist
    if (!projectionFields || projectionFields.length === 0) {
      setProjectionFields([
        {
          id: `annotation-${Date.now()}`,
          fieldName: "",
          outputName: "",
          type: "include",
          roundDecimals: 4,
          isDefault: false,
        },
      ]);
    }
  }, [projectionFields, setProjectionFields]);

  // Load initial data using annotation context
  useAnnotationBuilderData(annotationContext);

  // Fetch schema using the same approach as FilterBuilder
  useEffect(() => {
    dispatch(fetchSchema({ name: filter_stream, elements: "schema" }));
  }, [filter_stream, dispatch]);

  useEffect(() => {
    if (store_schema) {
      setSchema(store_schema);
      setFieldOptions(flattenFieldOptions(store_schema));
    }
  }, [store_schema]);

  // Get available field options from schema and variables, matching filter page
  const getFieldOptions = () => {
    // Don't return options until fieldOptions are loaded from schema
    if (!fieldOptions || fieldOptions.length === 0) {
      return [];
    }

    // Use the same helper as filter page for unified options
    // Pass empty array as schemaFieldOptions since fieldOptions already contains them
    return getFieldOptionsWithVariable(
      fieldOptions,
      fieldOptions,
      filterContext.customVariables || [],
      filterContext.customListVariables || [],
      [], // Empty array to avoid duplication since fieldOptions already contains schema fields
    )
      .map((field) => {
        const fieldName = field.label || field.name;
        let subFields = field.subFields || [];
        // Use getArrayFieldSubOptions for array fields to match AddListConditionDialog
        if (field.type === "array") {
          subFields = getArrayFieldSubOptions(fieldName, schema);
        }
        // For arithmetic variables, get expression from customVariables DB if available
        let expression = field.expression;
        if (field.isVariable) {
          const dbVar = (filterContext.customVariables || []).find(
            (v) => v.name === fieldName,
          );
          if (dbVar && dbVar.variable) {
            expression = latexToMongoConverter.convertToMongo(
              dbVar.variable.split("=")[1].trim(),
            );
          } else if (field.value && !expression) {
            expression = latexToMongoConverter.convertToMongo(field.value);
          }
        }
        return {
          name: fieldName,
          type: field.type || "string",
          label: fieldName,
          description: field.description || "",
          isVariable: field.isVariable || false,
          isListVariable: field.isListVariable || false,
          group: field.isVariable
            ? "Arithmetic Variables"
            : field.isListVariable
              ? "Database List Variables"
              : field.group ||
                (fieldName.split(".").length > 1
                  ? fieldName.split(".")[0]
                  : `${filter_stream} fields`),
          isArray: field.type === "array",
          isArrayVariable: field.type === "array_variable",
          subFields:
            field.type === "array_variable"
              ? field.listCondition?.subFieldOptions
              : subFields,
          ...(expression ? { expression } : {}),
        };
      })
      .filter((field) => field && field.name && field.name !== "objectId") // Exclude objectId from annotation input options
      .sort((a, b) => {
        // Sort by group first, then by name
        if (a.group < b.group) return -1;
        if (a.group > b.group) return 1;
        return a.name.localeCompare(b.name);
      });
  };

  const addProjectionField = () => {
    const newField = {
      id: Date.now(),
      fieldName: "",
      outputName: "",
      type: "include", // include, exclude, round
      roundDecimals: 4,
      isDefault: false,
    };
    setProjectionFields([...projectionFields, newField]);
  };

  const removeProjectionField = (id) => {
    setProjectionFields((fields) => fields.filter((f) => f.id !== id));
  };

  const updateProjectionField = (id, updates) => {
    setProjectionFields((fields) =>
      fields.map((f) => (f.id === id ? { ...f, ...updates } : f)),
    );
  };

  const generateProjectionStage = () => {
    const projection = {};
    const annotations = {};

    // Find availableFields for variable lookup
    const availableFields = getFieldOptions();

    // Always add objectId to the root of the projection as 1 (include)
    projection.objectId = 1;

    // Collect all output names being defined in this stage to prevent subfield conflicts
    const definedFields = new Set();
    projectionFields.forEach((field) => {
      if (!field.fieldName || field.fieldName === "objectId") return;
      const outputName = field.outputName || field.fieldName;
      definedFields.add(outputName);
    });

    projectionFields.forEach((field) => {
      // Only add to annotations if fieldName is present and not objectId
      if (!field.fieldName || field.fieldName === "objectId") return;
      const outputName = field.outputName || field.fieldName;

      // Skip subfield projections if parent field is being defined in this stage
      if (outputName.includes(".")) {
        const parentField = outputName.split(".")[0];
        if (definedFields.has(parentField)) {
          return; // Skip this field to avoid MongoDB error
        }
      }

      // If mapSaved, use the $map operator
      if (field.type === "map" && field.mapSaved && field.mapMongoMapQuery) {
        annotations[field.mapOutputFieldName || outputName] =
          field.mapMongoMapQuery;
        return;
      }
      const fieldOption = availableFields.find(
        (opt) => opt.name === field.fieldName,
      );
      const isArithmeticVar = fieldOption?.isVariable;
      // Retrieve the actual value of the expression if present
      let expression = fieldOption?.expression;
      if (expression && typeof expression === "object" && expression.value) {
        expression = expression.value;
      }

      // Helper: if arithmetic variable, convert its expression to MongoDB expression, else $<fieldName>
      const getFieldExpr = () => {
        if (isArithmeticVar && expression) {
          if (typeof expression === "string") {
            try {
              // Try to parse as JSON (MongoDB expression)
              return JSON.parse(expression);
            } catch {
              try {
                // Try to evaluate as JS object literal (MongoDB op)

                return eval(`(${expression})`);
              } catch {
                // Fallback to string
                return expression;
              }
            }
          }
          return expression;
        }
        return `$${field.fieldName}`;
      };

      // Aggregation operators with output type
      if (
        ["sum", "avg", "min", "max"].includes(field.type) &&
        field.aggregationOutputType === "round"
      ) {
        // If round is selected for aggregation, wrap aggregation in $round
        let aggExpr;
        if (field.subField) {
          aggExpr = {
            [`$${field.type}`]: `$${field.fieldName}.${field.subField}`,
          };
        } else {
          aggExpr = { [`$${field.type}`]: getFieldExpr() };
        }
        annotations[outputName] = {
          $round: [aggExpr, field.roundDecimals],
        };
        return;
      }

      // All fields go inside the annotations key
      switch (field.type) {
        case "include":
          annotations[outputName] = getFieldExpr();
          break;
        case "exclude":
          annotations[outputName] = 0;
          break;
        case "round":
          annotations[outputName] = {
            $round: [getFieldExpr(), field.roundDecimals],
          };
          break;
        case "sum":
          if (field.aggregationOutputType === "exclude") {
            annotations[outputName] = 0;
          } else {
            annotations[outputName] = field.subField
              ? { $sum: `$${field.fieldName}.${field.subField}` }
              : { $sum: getFieldExpr() };
          }
          break;
        case "avg":
          if (field.aggregationOutputType === "exclude") {
            annotations[outputName] = 0;
          } else {
            annotations[outputName] = field.subField
              ? { $avg: `$${field.fieldName}.${field.subField}` }
              : { $avg: getFieldExpr() };
          }
          break;
        case "min":
          if (field.aggregationOutputType === "exclude") {
            annotations[outputName] = 0;
          } else {
            annotations[outputName] = field.subField
              ? { $min: `$${field.fieldName}.${field.subField}` }
              : { $min: getFieldExpr() };
          }
          break;
        case "max":
          if (field.aggregationOutputType === "exclude") {
            annotations[outputName] = 0;
          } else {
            annotations[outputName] = field.subField
              ? { $max: `$${field.fieldName}.${field.subField}` }
              : { $max: getFieldExpr() };
          }
          break;
        case "count":
          annotations[outputName] = { $size: getFieldExpr() };
          break;
        default:
          annotations[outputName] = getFieldExpr();
      }
    });

    // Add annotations to projection if there are any
    if (Object.keys(annotations).length > 0) {
      projection.annotations = annotations;
    }

    return projection;
  };

  const handleShowMongoQuery = () => {
    // Use the unified context directly (same as filter builder)
    filterContext.setMongoDialog({ open: true });
  };

  const handleBackToFilters = () => {
    if (onBackToFilterBuilder) {
      onBackToFilterBuilder();
    } else {
      navigate("/");
    }
  };

  // Toggle group collapse state
  const toggleGroupCollapse = (groupName) => {
    setCollapsedGroups((prev) => {
      const newCollapsed = new Set(prev);
      if (newCollapsed.has(groupName)) {
        newCollapsed.delete(groupName);
      } else {
        newCollapsed.add(groupName);
      }
      return newCollapsed;
    });
  };

  const availableFields = getFieldOptions();

  // Initialize collapsed groups (collapse all groups by default)
  React.useEffect(() => {
    const allGroups = [...new Set(availableFields.map((field) => field.group))];
    if (allGroups.length > 0 && allGroups.length > collapsedGroups.size) {
      setCollapsedGroups(new Set(allGroups));
    }
  }, [availableFields.length]);

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        gap: 2,
        ...filterBuilderStyles.container,
      }}
    >
      {/* Header */}
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 2,
        }}
      >
        <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
          <Button
            variant="outlined"
            startIcon={<ArrowBackIcon />}
            onClick={handleBackToFilters}
            sx={{
              "&:hover": {
                backgroundColor: "primary.50",
              },
            }}
          >
            Back to Filters
          </Button>
          <Typography variant="h4" sx={{ color: "text.primary" }}>
            Annotations
          </Typography>
        </Box>
        <Button
          variant="outlined"
          startIcon={<CodeIcon />}
          onClick={handleShowMongoQuery}
          disabled={!hasValidFilterQuery()}
          sx={{
            borderColor: hasValidFilterQuery() ? "primary.main" : undefined,
            color: hasValidFilterQuery() ? "primary.main" : undefined,
            "&:hover": {
              borderColor: hasValidFilterQuery() ? "primary.dark" : undefined,
              backgroundColor: hasValidFilterQuery() ? "primary.50" : undefined,
            },
          }}
        >
          Preview MongoDB Query
        </Button>
      </Box>

      {/* Projection Fields */}
      <Paper sx={{ p: 3 }}>
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            mb: 3,
          }}
        >
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={addProjectionField}
            size="small"
          >
            Add Annotation
          </Button>
        </Box>

        {projectionFields.length === 0 ? (
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{ textAlign: "center", py: 4 }}
          >
            No annotations configured. Add annotations to specify what should be
            included in the query results.
          </Typography>
        ) : (
          <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
            {projectionFields.map((field) => (
              <Paper key={field.id} variant="outlined" sx={{ p: 2 }}>
                <Box
                  sx={{
                    display: "flex",
                    alignItems: "center",
                    gap: 2,
                    flexWrap: "wrap",
                  }}
                >
                  {/* Output Name */}
                  <TextField
                    label="Annotation Name"
                    value={field.outputName}
                    onChange={(e) =>
                      updateProjectionField(field.id, {
                        outputName: e.target.value,
                      })
                    }
                    size="small"
                    sx={{ minWidth: 150 }}
                    placeholder={field.fieldName}
                  />

                  {/* Field Selection */}
                  <Autocomplete
                    value={
                      field.fieldName
                        ? availableFields.find(
                            (opt) => opt.name === field.fieldName,
                          ) || null
                        : null
                    }
                    onChange={(_, newValue) => {
                      // If array field, ensure subFields are populated from schema if missing
                      let subFields = newValue?.subFields || [];
                      if (
                        newValue?.isArray &&
                        subFields.length === 0 &&
                        newValue?.schema &&
                        Array.isArray(newValue.schema)
                      ) {
                        const firstObj = newValue.schema[0];
                        if (firstObj && typeof firstObj === "object") {
                          subFields = Object.keys(firstObj);
                        }
                      }

                      updateProjectionField(field.id, {
                        fieldName: newValue?.name || "",
                        isArray:
                          newValue?.type === "array" ||
                          newValue?.type === "array_variable",
                        subFields,
                      });
                    }}
                    options={availableFields}
                    groupBy={(option) => option.group}
                    getOptionLabel={(option) => option.label || option.name}
                    isOptionEqualToValue={(option, value) =>
                      option.name === value?.name
                    }
                    sx={{ minWidth: 250 }}
                    renderInput={(params) => (
                      <TextField {...params} label="Field" size="small" />
                    )}
                    renderOption={(props, option) => {
                      const { key, ...otherProps } = props;
                      let displayText = option.name;
                      if (
                        option.group !== "Arithmetic Variables" &&
                        option.group !== "Other Fields"
                      ) {
                        displayText = option.name.includes(option.group)
                          ? option.name.split(".").slice(-1)[0]
                          : option.name;
                      }
                      return (
                        <li key={key} {...otherProps}>
                          <Box>
                            <Typography variant="body2">
                              {displayText}
                            </Typography>
                            <Typography
                              variant="caption"
                              color="text.secondary"
                            >
                              {option.type}{" "}
                              {option.isVariable ? "(variable)" : ""}
                            </Typography>
                          </Box>
                        </li>
                      );
                    }}
                    renderGroup={(params) => {
                      const isCollapsed = collapsedGroups.has(params.group);
                      return (
                        <li key={params.key}>
                          <GroupHeader
                            onClick={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              toggleGroupCollapse(params.group);
                            }}
                            title={`Click to ${
                              isCollapsed ? "expand" : "collapse"
                            } ${params.group} section`}
                          >
                            {isCollapsed ? (
                              <ChevronRightIcon fontSize="small" />
                            ) : (
                              <ExpandMoreIcon fontSize="small" />
                            )}
                            {params.group}
                          </GroupHeader>
                          <GroupItems
                            style={{ display: isCollapsed ? "none" : "block" }}
                          >
                            {params.children}
                          </GroupItems>
                        </li>
                      );
                    }}
                  />

                  {/* Projection Type */}
                  <Box sx={{ display: "flex", alignItems: "center", gap: 2 }}>
                    <FormControl size="small" sx={{ minWidth: 120 }}>
                      <InputLabel>Type</InputLabel>
                      <Select
                        value={field.type}
                        onChange={(e) => {
                          const newType = e.target.value;
                          let updates = { type: newType };
                          if (["sum", "avg", "min", "max"].includes(newType)) {
                            let subFields = field.subFields || [];
                            if (
                              (!subFields || subFields.length === 0) &&
                              field.fieldName
                            ) {
                              const found = availableFields.find(
                                (opt) => opt.name === field.fieldName,
                              );
                              if (
                                found &&
                                found.subFields &&
                                found.subFields.length > 0
                              ) {
                                subFields = found.subFields;
                              }
                            }
                            if (
                              (!subFields || subFields.length === 0) &&
                              field.listCondition?.subFieldOptions
                            ) {
                              subFields = field.listCondition.subFieldOptions;
                            }
                            if (subFields && subFields.length > 0) {
                              updates.subField = subFields[0];
                              updates.subFields = subFields;
                            }
                          }
                          updateProjectionField(field.id, updates);
                        }}
                        label="Type"
                      >
                        <MenuItem value="include">Include</MenuItem>
                        <MenuItem value="exclude">Exclude</MenuItem>
                        <MenuItem value="round">Round</MenuItem>
                        {/* Aggregation operators for array fields */}
                        {field.isArray && <MenuItem value="map">Map</MenuItem>}
                        {field.isArray && <MenuItem value="min">Min</MenuItem>}
                        {field.isArray && <MenuItem value="max">Max</MenuItem>}
                        {field.isArray && <MenuItem value="sum">Sum</MenuItem>}
                        {field.isArray && (
                          <MenuItem value="avg">Average</MenuItem>
                        )}
                        {field.isArray && (
                          <MenuItem value="count">Count</MenuItem>
                        )}
                      </Select>
                    </FormControl>
                    {/* Show map dialog button if type is map */}
                    {field.type === "map" && (
                      <Button
                        variant="outlined"
                        size="small"
                        sx={{ ml: 1, alignSelf: "center" }}
                        onClick={() => {
                          setMapDialogFieldId(field.id);
                          setMapDialogOpen(true);
                        }}
                      >
                        {field.mapSaved && field.mapOutputFieldName
                          ? `${field.mapOutputFieldName}_mapped`
                          : "Map Annotations"}
                      </Button>
                    )}
                    {/* Subfield selector for aggregation operators, inline to the right */}
                    {field.isArray &&
                      ["sum", "avg", "min", "max"].includes(field.type) &&
                      field.subFields &&
                      field.subFields.length > 0 && (
                        <>
                          <FormControl size="small" sx={{ minWidth: 150 }}>
                            <InputLabel>Subfield</InputLabel>
                            <Select
                              value={field.subField || ""}
                              onChange={(e) =>
                                updateProjectionField(field.id, {
                                  subField: e.target.value,
                                })
                              }
                              label="Subfield"
                            >
                              {(
                                field.subFields ||
                                field.listCondition?.subFieldOptions ||
                                []
                              ).map((opt) => {
                                const value =
                                  typeof opt === "object" &&
                                  opt !== null &&
                                  opt.label
                                    ? opt.label
                                    : String(opt);
                                return (
                                  <MenuItem key={value} value={value}>
                                    {value}
                                  </MenuItem>
                                );
                              })}
                            </Select>
                          </FormControl>
                          {/* Additional field for include/exclude/round when aggregation operator is selected */}
                          <FormControl size="small" sx={{ minWidth: 120 }}>
                            <InputLabel>Aggregation Output</InputLabel>
                            <Select
                              value={field.aggregationOutputType || "include"}
                              onChange={(e) =>
                                updateProjectionField(field.id, {
                                  aggregationOutputType: e.target.value,
                                })
                              }
                              label="Aggregation Output"
                            >
                              <MenuItem value="include">Include</MenuItem>
                              <MenuItem value="exclude">Exclude</MenuItem>
                              <MenuItem value="round">Round</MenuItem>
                            </Select>
                          </FormControl>
                          {/* Decimals input for aggregation round type */}
                          {field.aggregationOutputType === "round" && (
                            <TextField
                              label="Decimals"
                              type="number"
                              value={field.roundDecimals}
                              onChange={(e) =>
                                updateProjectionField(field.id, {
                                  roundDecimals: Math.max(
                                    0,
                                    Math.min(10, parseInt(e.target.value) || 0),
                                  ),
                                })
                              }
                              size="small"
                              sx={{ width: 100 }}
                              inputProps={{ min: 0, max: 10 }}
                            />
                          )}
                        </>
                      )}
                  </Box>

                  {/* Round Decimals (only for round type) */}
                  {field.type === "round" && (
                    <TextField
                      label="Decimals"
                      type="number"
                      value={field.roundDecimals}
                      onChange={(e) =>
                        updateProjectionField(field.id, {
                          roundDecimals: Math.max(
                            0,
                            Math.min(10, parseInt(e.target.value) || 0),
                          ),
                        })
                      }
                      size="small"
                      sx={{ width: 100 }}
                      inputProps={{ min: 0, max: 10 }}
                    />
                  )}

                  {/* Remove Button */}
                  <IconButton
                    onClick={() => removeProjectionField(field.id)}
                    color="error"
                    size="small"
                  >
                    <DeleteIcon />
                  </IconButton>
                </Box>
              </Paper>
            ))}
          </Box>
        )}

        {/* Projection Preview */}
        {projectionFields.length > 0 && (
          <Box sx={{ mt: 3, p: 2, bgcolor: "grey.50", borderRadius: 1 }}>
            <Typography variant="subtitle2" sx={{ mb: 1 }}>
              Generated $project Stage:
            </Typography>
            <Typography
              component="pre"
              variant="body2"
              sx={{
                fontFamily: "monospace",
                fontSize: "0.75rem",
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
              }}
            >
              {JSON.stringify(generateProjectionStage(), null, 2)}
            </Typography>
          </Box>
        )}
      </Paper>

      {/* Dialogs */}
      <AddVariableDialog />
      <AddListConditionDialog />
      <SaveBlockDialogMenu />

      {/* Map dialog for map type */}
      <MapAnnotationsDialog
        open={mapDialogOpen}
        onClose={() => setMapDialogOpen(false)}
        arrayField={projectionFields.find((f) => f.id === mapDialogFieldId)}
        mapProjectionFields={mapProjectionFields}
        setMapProjectionFields={setMapProjectionFields}
        onSave={({ outputFieldName, mongoMapQuery }) => {
          // Find the field and update its map state
          setProjectionFields((fields) =>
            fields.map((f) =>
              f.id === mapDialogFieldId
                ? {
                    ...f,
                    mapSaved: true,
                    mapOutputFieldName: outputFieldName,
                    mapMongoMapQuery: mongoMapQuery,
                  }
                : f,
            ),
          );
        }}
      />

      {/* MongoDB Query Dialog - use same context as filter builder */}
      <MongoQueryDialog />
    </Box>
  );
};

export default AnnotationBuilderContent;
