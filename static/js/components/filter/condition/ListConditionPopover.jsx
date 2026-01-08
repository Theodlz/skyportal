import React, { useRef, useEffect, useState } from "react";
import { Popover, Button, IconButton, Tooltip } from "@mui/material";
import EditIcon from "@mui/icons-material/Edit";
import SaveIcon from "@mui/icons-material/Save";
import CancelIcon from "@mui/icons-material/Cancel";
import BlockComponent from "../block/BlockComponent";
import { useCurrentBuilder, useFilterBuilder } from "../../../hooks/useContexts";
import { useDispatch } from "react-redux";
import { putElement } from "../../../ducks/boom_filter_modules";

const ListConditionPopover = ({
  listPopoverAnchor,
  setListPopoverAnchor,
  conditionOrBlock,
  customListVariables,
  createDefaultCondition, // Fixed: use createDefaultCondition
  customVariables,
  block,
  updateCondition,
  fieldOptions,
  fieldOptionsList,
}) => {
  const popoverRef = useRef(null);
  const isOpen = Boolean(listPopoverAnchor);
  const [editMode, setEditMode] = useState(false);
  const [editedConditions, setEditedConditions] = useState(null);
  const { setCustomListVariables } = useCurrentBuilder();
  const dispatch = useDispatch();
  
  // Get the filter builder context to access dialog handlers
  const filterBuilder = useFilterBuilder();

  // Debug: log when editedConditions changes
  useEffect(() => {
    if (editMode) {
      console.log("editedConditions updated:", editedConditions);
    }
  }, [editedConditions, editMode]);

  // Handle focus management when popover opens/closes
  useEffect(() => {
    if (isOpen && popoverRef.current) {
      // Set focus to the popover container when it opens
      const timer = setTimeout(() => {
        if (popoverRef.current) {
          popoverRef.current.focus();
        }
      }, 100);
      return () => clearTimeout(timer);
    }
  }, [isOpen]);

  const handleClose = () => {
    const anchorElement = listPopoverAnchor;
    setListPopoverAnchor(null);
    window.currentListVariable = null; // Clear temporary data
    setEditMode(false); // Reset edit mode
    setEditedConditions(null); // Clear edited conditions

    // Ensure focus is properly managed when closing
    if (anchorElement) {
      // Small delay to ensure the popover has closed before returning focus
      setTimeout(() => {
        if (anchorElement && typeof anchorElement.focus === "function") {
          try {
            anchorElement.focus();
          } catch {
            // Fallback - focus on the document body if anchor focus fails
            document.body.focus();
          }
        }
      }, 100);
    }
  };

  const handleSaveEdit = async (listVar) => {
    if (editedConditions && setCustomListVariables) {
      const updatedListCondition = {
        ...listVar.listCondition,
        value: editedConditions,
      };

      try {
        // Update in the database using Redux action
        await dispatch(
          putElement({
            name: listVar.name,
            data: {
              listCondition: updatedListCondition,
              type: listVar.type || "array",
            },
            elements: "listVariables",
          })
        );

        // Update in the context (local state) - this will trigger a re-render
        setCustomListVariables((prev) => {
          return prev.map((lv) => {
            if (lv.name === listVar.name) {
              return {
                ...lv,
                listCondition: updatedListCondition,
              };
            }
            return lv;
          });
        });

        // Exit edit mode
        setEditMode(false);
        setEditedConditions(null);
      } catch (error) {
        console.error("Failed to save list variable:", error);
        alert("Failed to save changes to the database. Please try again.");
      }
    }
  };

  const handleCancelEdit = () => {
    setEditMode(false);
    setEditedConditions(null);
  };

  const handleStartEdit = (listVar) => {
    setEditMode(true);
    // Deep clone to ensure we have an independent copy for editing
    const clonedValue = JSON.parse(JSON.stringify(listVar.listCondition.value));
    console.log("Starting edit with cloned value:", clonedValue);
    setEditedConditions(clonedValue);
  };

  const renderPopoverContent = () => {
    // Priority 1: Check if this is a list variable popover (from global callback)
    let listVar = window.currentListVariable;
    if (listVar) {
      // Always get the fresh data from customListVariables if available
      const freshListVar = customListVariables.find(
        (lv) => lv.name === listVar.name,
      );
      return renderListVariableContent(freshListVar || listVar);
    }

    // Priority 2: Check for aggregation operator popover (newly created with subField)
    if (
      conditionOrBlock.value &&
      typeof conditionOrBlock.value === "object" &&
      conditionOrBlock.value.type === "array" &&
      conditionOrBlock.value.subField
    ) {
      return renderAggregationDisplay(conditionOrBlock);
    }
    // Priority 2.5: Check for direct aggregation operator with subField in conditionOrBlock
    if (
      conditionOrBlock.operator &&
      ["$min", "$max", "$avg", "$sum"].includes(conditionOrBlock.operator)
    ) {
      return renderAggregationDisplay(conditionOrBlock);
    }

    // Priority 3: Check for reused list variable (from AutocompleteFields chip click)
    if (conditionOrBlock.isListVariable && conditionOrBlock.field) {
      const reusedListVar = customListVariables.find(
        (lv) => lv.name === conditionOrBlock.field,
      );
      if (reusedListVar) {
        return renderListVariableContent(reusedListVar);
      }
    }

    // Priority 4: Regular list condition popover (for newly created conditions with value but no subField)
    if (
      conditionOrBlock.value &&
      typeof conditionOrBlock.value === "object" &&
      conditionOrBlock.value.type === "array" &&
      !conditionOrBlock.value.subField
    ) {
      return renderRegularListCondition(
        conditionOrBlock,
        block,
        updateCondition,
        createDefaultCondition,
        customVariables,
        customListVariables,
      );
    }

    return null;
  };

  const renderListVariableContent = (listVar) => {
    const getOperatorLabel = (operator) => {
      const operatorLabels = {
        $anyElementTrue: "Any Element True",
        $allElementsTrue: "All Elements True",
        $filter: "Filter",
        $min: "Minimum",
        $max: "Maximum",
        $avg: "Average",
        $sum: "Sum",
        $size: "Size",
        $all: "All",
      };
      return operatorLabels[operator] || operator;
    };

    // Determine if this list variable can be edited (has conditions to edit)
    const canEdit = listVar.listCondition.value && 
                    typeof listVar.listCondition.value === "object" &&
                    !["$min", "$max", "$avg", "$sum"].includes(listVar.listCondition.operator);

    return (
      <div
        style={{
          width: "90vw",
          maxWidth: 900,
          minWidth: 400,
          display: "flex",
          flexDirection: "column",
          gap: 12,
        }}
      >
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: 8,
          }}
        >
          <div
            style={{
              fontWeight: 600,
              color: "#166534",
              fontSize: 17,
              letterSpacing: 0.2,
            }}
          >
            <span style={{ color: "#059669" }}>{listVar.name}</span>
            <span style={{ fontSize: 14, color: "#6b7280", marginLeft: 8 }}>
              ({listVar.listCondition.field})
            </span>
          </div>
          
          {canEdit && (
            <div style={{ display: "flex", gap: 8 }}>
              {!editMode ? (
                <Tooltip title="Edit list condition">
                  <IconButton
                    size="small"
                    onClick={() => handleStartEdit(listVar)}
                    style={{
                      backgroundColor: "#e0f2fe",
                      color: "#0369a1",
                      padding: 6,
                    }}
                  >
                    <EditIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              ) : (
                <>
                  <Tooltip title="Save changes">
                    <IconButton
                      size="small"
                      onClick={() => handleSaveEdit(listVar)}
                      style={{
                        backgroundColor: "#dcfce7",
                        color: "#166534",
                        padding: 6,
                      }}
                    >
                      <SaveIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                  <Tooltip title="Cancel editing">
                    <IconButton
                      size="small"
                      onClick={handleCancelEdit}
                      style={{
                        backgroundColor: "#fee2e2",
                        color: "#991b1b",
                        padding: 6,
                      }}
                    >
                      <CancelIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                </>
              )}
            </div>
          )}
        </div>

        {/* Display the list operator */}
        {listVar.listCondition.operator && (
          <div
            style={{
              padding: "8px 12px",
              backgroundColor: "#f0f9ff",
              borderRadius: 6,
              fontSize: 14,
              color: "#0369a1",
              border: "1px solid #bae6fd",
              fontWeight: 500,
              marginBottom: 4,
            }}
          >
            <span style={{ fontWeight: 600 }}>Operator:</span>{" "}
            {getOperatorLabel(listVar.listCondition.operator)}
          </div>
        )}

        <div style={{ width: "100%" }}>
          {listVar.listCondition.value ? (
            <BlockComponent
              block={editMode ? (editedConditions || listVar.listCondition.value) : listVar.listCondition.value}
              parentBlockId={null}
              isRoot={true}
              fieldOptionsList={(() => {
                // Combine subFieldOptions with full field options for comprehensive coverage
                const subFieldOpts =
                  listVar.listCondition.subFieldOptions || [];
                const fullFieldOpts = fieldOptionsList || fieldOptions || [];
                // If we have subFieldOptions, combine them with full options, otherwise just use full options
                return subFieldOpts.length > 0
                  ? [...fullFieldOpts, ...subFieldOpts]
                  : fullFieldOpts;
              })()}
              isListDialogOpen={false}
              localFilters={editMode ? [(editedConditions || listVar.listCondition.value)] : null}
              setLocalFilters={editMode ? (newFiltersOrUpdater) => {
                // Handle both direct values and updater functions
                console.log("setLocalFilters called with:", typeof newFiltersOrUpdater);
                
                let newFilters;
                if (typeof newFiltersOrUpdater === 'function') {
                  // If it's an updater function, call it with current localFilters
                  const currentFilters = [(editedConditions || listVar.listCondition.value)];
                  newFilters = newFiltersOrUpdater(currentFilters);
                  console.log("Updater function returned:", newFilters);
                } else {
                  // If it's a direct value, use it as-is
                  newFilters = newFiltersOrUpdater;
                  console.log("Direct value:", newFilters);
                }
                
                // Update via setLocalFilters for full editing support
                // Use JSON deep clone to ensure nested structures are preserved
                if (newFilters && newFilters.length > 0 && newFilters[0]?.id) {
                  try {
                    const clonedBlock = JSON.parse(JSON.stringify(newFilters[0]));
                    console.log("Setting editedConditions to:", clonedBlock);
                    setEditedConditions(clonedBlock);
                  } catch (error) {
                    console.error("Failed to clone block:", error);
                    setEditedConditions(newFilters[0]);
                  }
                } else {
                  console.warn("Invalid newFilters:", newFilters);
                }
              } : null}
            />
          ) : listVar.listCondition.subField ||
            (listVar.listCondition.operator &&
              ["$min", "$max", "$avg", "$sum"].includes(
                listVar.listCondition.operator,
              )) ? (
            renderAggregationDisplay(listVar.listCondition)
          ) : (
            <div
              style={{ fontSize: 14, color: "#6b7280", fontStyle: "italic" }}
            >
              This list condition doesn't have sub-conditions to display.
            </div>
          )}
        </div>
      </div>
    );
  };

  const renderAggregationDisplay = (listCondition) => {
    // Handle different possible data structures
    const operator = listCondition.operator || "";
    const arrayField =
      listCondition.field ||
      (listCondition.value && listCondition.value.field) ||
      "";
    const subField =
      listCondition.subField ||
      (listCondition.value && listCondition.value.subField) ||
      "";

    if (!operator || !arrayField || !subField) {
      return (
        <div style={{ fontSize: 14, color: "#6b7280", fontStyle: "italic" }}>
          Incomplete aggregation information available.
        </div>
      );
    }

    return (
      <div>
        <div
          style={{
            padding: "12px 16px",
            backgroundColor: "#f0fdf4",
            borderRadius: 8,
            fontSize: 16,
            color: "#166534",
            border: "1px solid #bbf7d0",
            fontWeight: 600,
            fontFamily: "monospace",
            marginBottom: 12,
          }}
        >
          {operator.replace("$", "").toUpperCase()}({arrayField}.{subField})
        </div>
        <div style={{ fontSize: 14, color: "#6b7280" }}>
          This aggregation operation calculates the{" "}
          {operator.replace("$", "").toLowerCase()} value of the "{subField}"
          field across all elements in the "{arrayField}" array.
        </div>
      </div>
    );
  };

  const renderRegularListCondition = (
    conditionOrBlock,
    block,
    updateCondition,
    createDefaultCondition,
    customVariables,
    customListVariables,
  ) => {
    // Get the operator label from mongoOperatorLabels
    const getOperatorLabel = (operator) => {
      const operatorLabels = {
        $anyElementTrue: "Any Element True",
        $allElementsTrue: "All Elements True",
        $filter: "Filter",
        $map: "Map",
        $min: "Minimum",
        $max: "Maximum",
        $avg: "Average",
        $sum: "Sum",
        $size: "Size",
        $all: "All",
      };
      return operatorLabels[operator] || operator;
    };

    return (
      <div
        style={{
          width: "90vw",
          maxWidth: 900,
          minWidth: 400,
          display: "flex",
          flexDirection: "column",
          gap: 12,
        }}
      >
        <div
          style={{
            fontWeight: 600,
            color: "#166534",
            fontSize: 17,
            marginBottom: 8,
            letterSpacing: 0.2,
          }}
        >
          {conditionOrBlock.value.name ? (
            <>
              <span style={{ color: "#059669" }}>
                {conditionOrBlock.value.name}
              </span>
              <span style={{ fontSize: 14, color: "#6b7280", marginLeft: 8 }}>
                ({conditionOrBlock.value.field})
              </span>
            </>
          ) : (
            <>
              List Condition:{" "}
              <span style={{ color: "#059669" }}>
                {conditionOrBlock.value.field}
              </span>
            </>
          )}
        </div>

        {/* Display the list operator */}
        {conditionOrBlock.value.operator && (
          <div
            style={{
              padding: "8px 12px",
              backgroundColor: "#f0f9ff",
              borderRadius: 6,
              fontSize: 14,
              color: "#0369a1",
              border: "1px solid #bae6fd",
              fontWeight: 500,
              marginBottom: 4,
            }}
          >
            <span style={{ fontWeight: 600 }}>Operator:</span>{" "}
            {getOperatorLabel(conditionOrBlock.value.operator)}
          </div>
        )}
        <div style={{ width: "100%" }}>
          {conditionOrBlock.value.value ? (
            <BlockComponent
              block={conditionOrBlock.value.value}
              parentBlockId={null}
              isRoot={true}
              fieldOptionsList={(() => {
                // Combine subFieldOptions with full field options for comprehensive coverage
                const subFieldOpts =
                  conditionOrBlock.value.subFieldOptions || [];
                const fullFieldOpts = fieldOptionsList || fieldOptions || [];
                // If we have subFieldOptions, combine them with full options, otherwise just use full options
                return subFieldOpts.length > 0
                  ? [...fullFieldOpts, ...subFieldOpts]
                  : fullFieldOpts;
              })()}
              localFilters={[conditionOrBlock.value.value]}
              setLocalFilters={(newFilters) => {
                // Update the list condition value in the main filters
                const updatedListValue = {
                  ...conditionOrBlock.value,
                  value: newFilters[0],
                };
                updateCondition(
                  block.id,
                  conditionOrBlock.id,
                  "value",
                  updatedListValue,
                );
              }}
            />
          ) : (
            <div
              style={{ fontSize: 14, color: "#6b7280", fontStyle: "italic" }}
            >
              This list condition doesn't have sub-conditions to display.
            </div>
          )}
        </div>
      </div>
    );
  };

  return (
    <Popover
      open={Boolean(listPopoverAnchor)}
      anchorEl={listPopoverAnchor}
      onClose={handleClose}
      anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
      transformOrigin={{ vertical: "top", horizontal: "left" }}
      disableEnforceFocus={false}
      disableAutoFocus={true}
      disableRestoreFocus={true}
      disablePortal={false}
      keepMounted={false}
      hideBackdrop={false}
      slotProps={{
        root: {
          // Prevent aria-hidden on the root when focus is inside
          "aria-hidden": false,
        },
      }}
      PaperProps={{
        style: {
          minWidth: 500,
          maxWidth: 1000,
          width: "80vw",
          padding: 18,
          borderRadius: 16,
          boxShadow: "0 8px 32px 0 rgba(16,185,129,0.13)",
          background: "linear-gradient(90deg, #f0fdf4 60%, #d1fae5 100%)",
          overflowY: "auto",
          overflowX: "hidden",
          maxHeight: "80vh",
        },
        role: "dialog",
        "aria-modal": "true",
        "aria-labelledby": "list-condition-popover-title",
        // Prevent aria-hidden on the paper when focus is inside
        "aria-hidden": false,
      }}
    >
      <div
        ref={popoverRef}
        id="list-condition-popover-title"
        style={{ position: "absolute", left: "-10000px" }}
        tabIndex={-1}
      >
        List Condition Details
      </div>
      <div tabIndex={0} style={{ outline: "none" }}>
        {renderPopoverContent()}
      </div>
    </Popover>
  );
};

export default ListConditionPopover;
