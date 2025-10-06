import React, { useRef, useEffect } from "react";
import { Popover } from "@mui/material";
import BlockComponent from "../block/BlockComponent";

const ListConditionPopover = ({
  listPopoverAnchor,
  setListPopoverAnchor,
  conditionOrBlock,
  customListVariables,
  createDefaultCondition, // Fixed: use createDefaultCondition
  customVariables,
  block,
  updateCondition,
}) => {
  const popoverRef = useRef(null);
  const isOpen = Boolean(listPopoverAnchor);

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

  const renderPopoverContent = () => {
    // Priority 1: Check if this is a list variable popover (from global callback)
    const listVar = window.currentListVariable;
    if (listVar) {
      return renderListVariableContent(listVar);
    }

    // Priority 2: Check for aggregation operator popover (newly created with subField)
    if (
      conditionOrBlock.value &&
      typeof conditionOrBlock.value === "object" &&
      conditionOrBlock.value.type === "array" &&
      conditionOrBlock.value.subField
    ) {
      return renderAggregationContent(conditionOrBlock);
    }
    // Priority 2.5: Check for direct aggregation operator with subField in conditionOrBlock
    if (
      conditionOrBlock.operator &&
      ["$min", "$max", "$avg", "$sum"].includes(conditionOrBlock.operator)
    ) {
      return renderDirectAggregationContent(conditionOrBlock);
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
      ); // Fixed
    }

    return null;
  };

  const renderListVariableContent = (listVar) => {
    // Get the operator label from mongoOperatorLabels
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
          <span style={{ color: "#059669" }}>{listVar.name}</span>
          <span style={{ fontSize: 14, color: "#6b7280", marginLeft: 8 }}>
            ({listVar.listCondition.field})
          </span>
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
              block={listVar.listCondition.value}
              parentBlockId={null}
              isRoot={true}
              customBlocks={[]}
              collapsedBlocks={{}}
              filters={[listVar.listCondition.value]}
              setFilters={() => {}} // Read-only
              setCollapsedBlocks={() => {}}
              setSaveDialog={() => {}}
              setSaveName={() => {}}
              setSaveError={() => {}}
              defaultCondition={createDefaultCondition} // Fixed: use createDefaultCondition
              defaultBlock={() => ({})}
              setSpecialConditionDialog={() => {}}
              customVariables={customVariables}
              setListConditionDialog={() => {}}
              fieldOptionsList={listVar.listCondition.subFieldOptions || []}
              customListVariables={customListVariables}
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

  const renderAggregationContent = (conditionOrBlock) => {
    const operatorName = conditionOrBlock.operator
      .replace("$", "")
      .toUpperCase();
    const arrayField = conditionOrBlock.value.field;
    const subField = conditionOrBlock.value.subField;

    return (
      <div
        style={{
          width: "90vw",
          maxWidth: 900,
          minWidth: 300,
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
          <span style={{ color: "#059669" }}>
            {conditionOrBlock.value.name}
          </span>
        </div>
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
          }}
        >
          {operatorName}({arrayField}.{subField})
        </div>
        <div style={{ fontSize: 14, color: "#6b7280" }}>
          This aggregation operation calculates the {operatorName.toLowerCase()}{" "}
          value of the "{subField}" field across all elements in the "
          {arrayField}" array.
        </div>
      </div>
    );
  };

  const renderDirectAggregationContent = (conditionOrBlock) => {
    const operatorName = conditionOrBlock.operator
      .replace("$", "")
      .toUpperCase();

    // Try to get field and subField from different possible locations
    const arrayField = conditionOrBlock.value?.field || conditionOrBlock.field;
    const subField =
      conditionOrBlock.value?.subField || conditionOrBlock.subField;
    const conditionName =
      conditionOrBlock.value?.name || conditionOrBlock.name || "Aggregation";

    return (
      <div
        style={{
          width: "90vw",
          maxWidth: 900,
          minWidth: 300,
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
          <span style={{ color: "#059669" }}>{conditionName}</span>
        </div>
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
          }}
        >
          {operatorName}({arrayField}.{subField})
        </div>
        <div style={{ fontSize: 14, color: "#6b7280" }}>
          This aggregation operation calculates the {operatorName.toLowerCase()}{" "}
          value of the "{subField}" field across all elements in the "
          {arrayField}" array.
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
    // Fixed
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
              customBlocks={[]}
              collapsedBlocks={{}}
              filters={[conditionOrBlock.value.value]}
              setFilters={(newFilters) => {
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
              setCollapsedBlocks={() => {}}
              setSaveDialog={() => {}}
              setSaveName={() => {}}
              setSaveError={() => {}}
              defaultCondition={createDefaultCondition} // Fixed: use createDefaultCondition
              defaultBlock={() => ({})}
              setSpecialConditionDialog={() => {}}
              customVariables={customVariables}
              setListConditionDialog={() => {}}
              fieldOptionsList={conditionOrBlock.value.subFieldOptions || []}
              customListVariables={customListVariables}
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
