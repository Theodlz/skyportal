import React, { useState, useMemo, useEffect } from "react";
import PropTypes from "prop-types";
import TextField from "@mui/material/TextField";
import Autocomplete from "@mui/material/Autocomplete";
import { styled, lighten, darken } from "@mui/system";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import {
  mongoOperatorTypes,
  mongoOperatorLabels,
} from "../../../constants/filterConstants";

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

const AutocompleteFields = ({
  fieldOptions,
  value,
  onChange,
  conditionOrBlock,
  setOpenEquationIds,
  setSelectedChip,
  side,
  setEquationAnchor = null,
}) => {
  const [showFullPath, setShowFullPath] = useState(false);
  // Initialize collapsed groups as empty Set, will be populated when groups are computed
  const [collapsedGroups, setCollapsedGroups] = useState(new Set());

  // Helper function to get operator display label
  const getOperatorDisplayLabel = (operator) => {
    return mongoOperatorLabels[operator] || operator;
  };

  // Group by category and collect unique group names
  const { options, allGroups } = useMemo(() => {
    const baseOptions = fieldOptions || [];

    // Group by category (origin, "Simple", "Arithmetic Variables", or "Database List Variables")
    let processedOptions = baseOptions.map((option) => ({
      ...option,
      group:
        option.group ||
        (option.isVariable
          ? "Arithmetic Variables"
          : option.isListVariable
            ? "Database List Variables"
            : option.label?.split(".").length > 1
              ? option.label?.split(".")[0]
              : "Simple"),
    }));

    // Sort options by group to avoid duplicated headers warning
    processedOptions = processedOptions.sort((a, b) => {
      if (a.group < b.group) return -1;
      if (a.group > b.group) return 1;
      return 0;
    });

    // Collect all unique group names
    const uniqueGroups = [
      ...new Set(processedOptions.map((option) => option.group)),
    ];

    return { options: processedOptions, allGroups: uniqueGroups };
  }, [fieldOptions]);

  // Set all field options groups as collapsed when they change (only on initial load)
  useEffect(() => {
    if (allGroups.length > 0 && allGroups.length > collapsedGroups.size) {
      setCollapsedGroups(new Set(allGroups));
    }
  }, [allGroups.length]);

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

  // Helper function to get display name for nested fields
  const getDisplayName = (fieldName) => {
    if (!fieldName) return "";
    const parts = fieldName.split(".");
    return parts.length > 1 ? parts[parts.length - 1] : fieldName;
  };

  // Helper function to check if field is nested
  const isNestedField = (fieldName) => {
    return fieldName && fieldName.includes(".");
  };

  // Helper function to calculate chip width and styling based on text length
  const getChipStyles = (text, baseStyles) => {
    return {
      ...baseStyles,
      maxWidth: "calc(100% - 16px)",
      fontSize: 15,
      padding: "2px 12px",
      transition: "all 0.2s ease",
      overflow: "hidden",
      whiteSpace: baseStyles.whiteSpace,
      textOverflow: "ellipsis",
    };
  };

  // Helper function to get comprehensive tooltip text
  const getTooltipText = (fieldOption, displayText) => {
    const parts = [];

    if (fieldOption.label !== displayText) {
      parts.push(`Full path: ${fieldOption.label}`);
    }

    if (fieldOption.group && fieldOption.group !== "Simple") {
      parts.push(`Category: ${fieldOption.group}`);
    }

    if (fieldOption.type) {
      parts.push(`Type: ${fieldOption.type}`);
    }

    parts.push("Click to toggle path display");

    return parts.join(" | ");
  };

  // // Helper function to check if field is nested in condition
  // const isNestedFieldCondition = (field) => {
  //   if (!field.label) return false;
  //   else if (!field.group) return field.label.includes('.');
  //   else {
  //     return !!field.label;
  //   }
  // };

  const togglePathDisplay = () => {
    setShowFullPath(!showFullPath);
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        minWidth: 200,
        width: "100%",
        maxWidth: "100%",
        position: "relative",
      }}
    >
      <div style={{ position: "relative", width: "100%" }}>
        <Autocomplete
          key={conditionOrBlock.id}
          freeSolo
          size="small"
          options={options}
          groupBy={(option) => option.group}
          getOptionLabel={(option) => {
            // Handle string labels
            if (typeof option === "string") return option;
            // Handle option objects
            if (option && option.label) {
              return typeof option.label === "string"
                ? option.label
                : String(option.label);
            }
            // Handle unexpected objects
            return "";
          }}
          sx={{
            width: "100%",
            minWidth: 200,
            maxWidth: "100%",
            "& .MuiAutocomplete-inputRoot": {
              minWidth: 200,
              width: "100%",
              maxWidth: "100%",
            },
          }}
          slotProps={{
            popper: {
              style: {
                zIndex: 10000,
              },
              placement: "bottom-start",
              modifiers: [
                {
                  name: "preventOverflow",
                  enabled: true,
                  options: {
                    altAxis: true,
                    altBoundary: true,
                    tether: false,
                    rootBoundary: "document",
                  },
                },
                {
                  name: "flip",
                  enabled: true,
                  options: {
                    altBoundary: true,
                    rootBoundary: "document",
                  },
                },
              ],
            },
          }}
          disablePortal={false} // Ensure dropdown is rendered in a portal to escape clipping
          value={(() => {
            // Handle list condition objects
            if (value && typeof value === "object" && value.type === "array") {
              return null; // Don't show in autocomplete dropdown, let chip handle display
            }
            // Handle regular options
            return (
              options.find((opt) => opt.label === value) ||
              (value ? { label: value } : null)
            );
          })()}
          onChange={(_, newValue) =>
            onChange &&
            onChange(
              newValue
                ? typeof newValue === "string"
                  ? newValue
                  : newValue.label
                : "",
            )
          }
          onInputChange={(_, newInputValue, reason) => {
            if (reason === "input" || reason === "clear") {
              onChange && onChange(newInputValue);
            }
          }}
          renderInput={(params) => {
            const variableOption = options.find(
              (opt) => opt.label === value && opt.isVariable,
            );
            const fieldOption = (fieldOptions || []).find(
              (opt) => opt.label === value && !opt.isVariable,
            );
            const listOption = (fieldOptions || []).find(
              (opt) => opt.label === value && opt.type === "array",
            );
            const isListCondition =
              value && typeof value === "object" && value.type === "array";
            const hasChip =
              variableOption || fieldOption || listOption || isListCondition;

            return (
              <TextField
                {...params}
                label="Fields"
                sx={{
                  "& .MuiInputBase-input": {
                    color: hasChip ? "transparent" : "inherit",
                  },
                }}
              />
            );
          }}
          renderOption={(props, option) => {
            const { key, ...otherProps } = props;

            // For list variables, include the operator in the display
            let displayText = option.label;
            if (option.isListVariable && option.listCondition?.operator) {
              const operatorDisplay = getOperatorDisplayLabel(
                option.listCondition.operator,
              );
              displayText = `${option.label}`;
            } else if (
              option.group !== "Arithmetic Variables" &&
              option.group !== "Simple"
            ) {
              // For other grouped options, show only the last part after the dot
              displayText = option.label.includes(option.group)
                ? option.label.split(".").slice(-1)[0] // Show only the last part after the dot
                : option.label;
            }

            return (
              <li key={key} {...otherProps}>
                {displayText}
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
                  title={`Click to ${isCollapsed ? "expand" : "collapse"} ${
                    params.group
                  } section`}
                >
                  {isCollapsed ? (
                    <ChevronRightIcon fontSize="small" />
                  ) : (
                    <ExpandMoreIcon fontSize="small" />
                  )}
                  {params.group}
                </GroupHeader>
                <GroupItems style={{ display: isCollapsed ? "none" : "block" }}>
                  {params.children}
                </GroupItems>
              </li>
            );
          }}
        />
        {/* Chip for variable field, clickable to show equation */}
        {(() => {
          const variableOption = options.find(
            (opt) => opt.label === value && opt.isVariable,
          );
          const fieldOption = options.find(
            (opt) =>
              opt.label === value && !opt.isVariable && !opt.isListVariable,
          );
          const listVariableOption = options.find(
            (opt) => opt.label === value && opt.isListVariable,
          );

          // Check for aggregation list condition (when conditionOrBlock has aggregation value)
          if (
            conditionOrBlock &&
            conditionOrBlock.value &&
            typeof conditionOrBlock.value === "object" &&
            conditionOrBlock.value.type === "array" &&
            conditionOrBlock.value.subField &&
            mongoOperatorTypes[conditionOrBlock.operator] === "aggregation"
          ) {
            return (
              <span
                style={{
                  position: "absolute",
                  top: "50%",
                  left: 8,
                  transform: "translateY(-50%)",
                  display: "inline-flex",
                  alignItems: "center",
                  background:
                    "linear-gradient(90deg, #bbf7d0 60%, #a7f3d0 100%)",
                  color: "#166534",
                  borderRadius: 16,
                  padding: "2px 12px",
                  fontWeight: 700,
                  fontSize: 15,
                  border: "1.5px solid #4ade80",
                  cursor: "pointer",
                  minWidth: 0,
                  maxWidth: "calc(100% - 16px)",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  pointerEvents: "auto",
                  zIndex: 2,
                  boxShadow: "0 2px 8px 0 rgba(16,185,129,0.08)",
                }}
                onClick={(e) => {
                  e.stopPropagation();
                  // Trigger the list popover to open
                  if (
                    conditionOrBlock &&
                    conditionOrBlock.id &&
                    window.openListPopover
                  ) {
                    window.openListPopover(
                      conditionOrBlock.id,
                      e.currentTarget,
                    );
                  }
                }}
                title={`Click to view aggregation details: ${conditionOrBlock.value.name}`}
              >
                {conditionOrBlock.value.name}
              </span>
            );
          }

          // Check for list variable (used with $filter operator or database variables)
          if (listVariableOption) {
            // Get the operator for display
            const operatorDisplay = listVariableOption.listCondition?.operator
              ? getOperatorDisplayLabel(
                  listVariableOption.listCondition.operator,
                )
              : "";

            // Create display text with operator if available
            const displayText = listVariableOption.label;

            return (
              <span
                style={{
                  position: "absolute",
                  top: "50%",
                  left: 8,
                  transform: "translateY(-50%)",
                  display: "inline-flex",
                  alignItems: "center",
                  background: listVariableOption.isDbVariable
                    ? "linear-gradient(90deg, #ddd6fe 60%, #c4b5fd 100%)"
                    : "linear-gradient(90deg, #bbf7d0 60%, #a7f3d0 100%)",
                  color: listVariableOption.isDbVariable
                    ? "#5b21b6"
                    : "#166534",
                  borderRadius: 16,
                  padding: "2px 12px",
                  fontWeight: 700,
                  fontSize: 15,
                  border: listVariableOption.isDbVariable
                    ? "1.5px solid #8b5cf6"
                    : "1.5px solid #4ade80",
                  cursor: "pointer",
                  minWidth: 0,
                  maxWidth: "calc(100% - 16px)",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  pointerEvents: "auto",
                  zIndex: 2,
                  boxShadow: listVariableOption.isDbVariable
                    ? "0 2px 8px 0 rgba(139,92,246,0.08)"
                    : "0 2px 8px 0 rgba(16,185,129,0.08)",
                }}
                onClick={(e) => {
                  e.stopPropagation();
                  // Use the global list variable popover callback
                  if (window.openListVariablePopover) {
                    window.openListVariablePopover(
                      listVariableOption.label,
                      e.currentTarget,
                    );
                  }
                }}
                title={`Click to view ${
                  listVariableOption.isDbVariable ? "database " : ""
                }list variable: ${listVariableOption.label}${
                  operatorDisplay ? ` (${operatorDisplay})` : ""
                }`}
              >
                {displayText}
              </span>
            );
          }

          // Check for array field (used in list conditions)
          const arrayFieldOption = options.find(
            (opt) => opt.label === value && opt.type === "array",
          );
          if (arrayFieldOption) {
            const hasCategory =
              arrayFieldOption.group && arrayFieldOption.group !== "Simple";
            const displayText = () => {
              if (showFullPath && hasCategory) {
                return `${arrayFieldOption.group}.${getDisplayName(
                  arrayFieldOption.label,
                )}`;
              } else if (
                showFullPath &&
                isNestedField(arrayFieldOption.label)
              ) {
                return arrayFieldOption.label;
              } else {
                return getDisplayName(arrayFieldOption.label);
              }
            };

            const currentDisplayText = displayText();
            const baseStyles = {
              position: "absolute",
              top: "50%",
              left: 8,
              transform: "translateY(-50%)",
              display: "inline-flex",
              alignItems: "center",
              background: "linear-gradient(90deg, #bbf7d0 60%, #a7f3d0 100%)",
              color: "#166534",
              borderRadius: 16,
              fontWeight: 700,
              border: "1.5px solid #4ade80",
              cursor: "pointer",
              minWidth: 0,
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              pointerEvents: "auto",
              zIndex: 2,
              boxShadow: "0 2px 8px 0 rgba(16,185,129,0.08)",
            };

            return (
              <span
                style={getChipStyles(currentDisplayText, baseStyles)}
                onClick={(e) => {
                  e.stopPropagation();
                  togglePathDisplay();
                }}
                title={getTooltipText(arrayFieldOption, currentDisplayText)}
              >
                {currentDisplayText}
              </span>
            );
          }

          // Check for configured list condition
          if (
            value &&
            typeof value === "object" &&
            value.type === "array" &&
            value.field
          ) {
            return (
              <span
                style={{
                  position: "absolute",
                  top: "50%",
                  left: 8,
                  transform: "translateY(-50%)",
                  display: "inline-flex",
                  alignItems: "center",
                  background:
                    "linear-gradient(90deg, #bbf7d0 60%, #a7f3d0 100%)",
                  color: "#166534",
                  borderRadius: 16,
                  padding: "2px 12px",
                  fontWeight: 700,
                  fontSize: 15,
                  border: "1.5px solid #4ade80",
                  cursor: "pointer",
                  minWidth: 0,
                  maxWidth: "calc(100% - 16px)",
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  pointerEvents: "auto",
                  zIndex: 2,
                  boxShadow: "0 2px 8px 0 rgba(16,185,129,0.08)",
                }}
                onClick={(e) => {
                  e.stopPropagation();
                  if (
                    conditionOrBlock &&
                    conditionOrBlock.id &&
                    window.openListPopover
                  ) {
                    window.openListPopover(
                      conditionOrBlock.id,
                      e.currentTarget,
                    );
                  }
                }}
                title={`Click to view list condition: ${
                  value.name || value.field
                }`}
              >
                {value.name}
              </span>
            );
          }

          // Show chip if value matches a variable, a field option, or a list option (array type)
          if (variableOption) {
            const displayText = () => {
              if (
                showFullPath &&
                (isNestedField(variableOption.label) ||
                  (variableOption.group &&
                    variableOption.group !== "Arithmetic Variables"))
              ) {
                return isNestedField(variableOption.label)
                  ? variableOption.label
                  : `${variableOption.group}.${variableOption.label}`;
              } else {
                return getDisplayName(variableOption.label);
              }
            };

            const currentDisplayText = displayText();
            const baseStyles = {
              position: "absolute",
              top: "50%",
              left: 8,
              transform: "translateY(-50%)",
              display: "inline-flex",
              alignItems: "center",
              background: "#fde68a",
              color: "#b45309",
              borderRadius: 16,
              fontWeight: 700,
              border: "1.5px solid #fbbf24",
              cursor: "pointer",
              minWidth: 0,
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              pointerEvents: "auto",
              zIndex: 2,
            };

            return (
              <span
                style={getChipStyles(currentDisplayText, baseStyles)}
                onClick={(e) => {
                  if (
                    isNestedField(variableOption.label) ||
                    (variableOption.group &&
                      variableOption.group !== "Arithmetic Variables")
                  ) {
                    e.stopPropagation();
                    togglePathDisplay();
                  } else {
                    e.stopPropagation();
                    setSelectedChip(side);
                    if (setEquationAnchor) {
                      setEquationAnchor(e.currentTarget);
                    }
                    setOpenEquationIds((prev) =>
                      prev.includes(conditionOrBlock.id)
                        ? prev.filter((id) => id !== conditionOrBlock.id)
                        : [...prev, conditionOrBlock.id],
                    );
                  }
                }}
                title={
                  isNestedField(variableOption.label) ||
                  (variableOption.group &&
                    variableOption.group !== "Arithmetic Variables")
                    ? getTooltipText(variableOption, currentDisplayText)
                    : "Click to view equation"
                }
              >
                {currentDisplayText}
              </span>
            );
          }
          if (fieldOption) {
            const hasCategory =
              fieldOption.group &&
              !["candidate", "Simple"].includes(fieldOption.group);
            const displayText = () => {
              if (showFullPath && hasCategory) {
                return `${fieldOption.group}.${getDisplayName(
                  fieldOption.label,
                )}`;
              } else if (showFullPath && isNestedField(fieldOption.label)) {
                return fieldOption.label;
              } else {
                return getDisplayName(fieldOption.label);
              }
            };

            const currentDisplayText = displayText();
            const baseStyles = {
              position: "absolute",
              top: "50%",
              left: 8,
              transform: "translateY(-50%)",
              display: "inline-flex",
              alignItems: "center",
              background: "#e0e7ff",
              color: "#3730a3",
              borderRadius: 16,
              fontWeight: 600,
              boxShadow: "0 1px 4px 0 rgba(80,120,255,0.08)",
              border: "1.5px solid #a5b4fc",
              cursor: "pointer",
              minWidth: 0,
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              pointerEvents: "auto",
              zIndex: 2,
            };

            return (
              <span
                style={getChipStyles(currentDisplayText, baseStyles)}
                onClick={(e) => {
                  e.stopPropagation();
                  togglePathDisplay();
                }}
                title={getTooltipText(fieldOption, currentDisplayText)}
              >
                {currentDisplayText}
              </span>
            );
          }
          // Show chip for list/array type options
          const listOption = (fieldOptions || []).find(
            (opt) => opt.label === value && opt.type === "array",
          );
          if (listOption) {
            const hasCategory =
              listOption.group && listOption.group !== "Simple";
            const displayText = () => {
              if (showFullPath && hasCategory) {
                return `${listOption.group}.${getDisplayName(
                  listOption.label,
                )}`;
              } else if (showFullPath && isNestedField(listOption.label)) {
                return listOption.label;
              } else {
                return getDisplayName(listOption.label), showFullPath;
              }
            };

            const currentDisplayText = displayText();
            const baseStyles = {
              position: "absolute",
              top: "50%",
              left: 8,
              transform: "translateY(-50%)",
              display: "inline-flex",
              alignItems: "center",
              background: "#bbf7d0",
              color: "#166534",
              borderRadius: 16,
              fontWeight: 700,
              border: "1.5px solid #4ade80",
              cursor: "pointer",
              minWidth: 0,
              whiteSpace: "nowrap",
              overflow: "hidden",
              textOverflow: "ellipsis",
              pointerEvents: "auto",
              zIndex: 2,
            };

            return (
              <span
                style={getChipStyles(currentDisplayText, baseStyles)}
                onClick={(e) => {
                  e.stopPropagation();
                  togglePathDisplay();
                }}
                title={getTooltipText(listOption, currentDisplayText)}
              >
                {currentDisplayText}
              </span>
            );
          }
          // Do not show chip for free input values
          return null;
        })()}
      </div>
    </div>
  );
};

AutocompleteFields.propTypes = {
  fieldOptions: PropTypes.arrayOf(
    PropTypes.shape({
      label: PropTypes.string.isRequired,
      value: PropTypes.string.isRequired,
      group: PropTypes.string,
      isVariable: PropTypes.bool,
      isListVariable: PropTypes.bool,
    }),
  ).isRequired,
  value: PropTypes.string,
  onChange: PropTypes.func.isRequired,
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    field: PropTypes.string,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  setOpenEquationIds: PropTypes.func.isRequired,
  setSelectedChip: PropTypes.func.isRequired,
  side: PropTypes.string.isRequired,
  setEquationAnchor: PropTypes.func,
};

export default AutocompleteFields;
