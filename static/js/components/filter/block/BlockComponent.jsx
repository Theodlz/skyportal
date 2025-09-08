import React, { useState, useMemo } from "react";
import PropTypes from "prop-types";
import {
  useFilterBuilder,
  useConditionContext,
} from "../../../hooks/useContexts";
import { v4 as uuidv4 } from "uuid";
import ClearIcon from "@mui/icons-material/Clear";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import { blockHeaderStyles } from "../../../styles/componentStyles";
import {
  Paper,
  Button,
  Box,
  TextField,
  ClickAwayListener,
  Divider,
  Typography,
  Popper,
  Switch,
  FormControl,
  FormControlLabel,
  Select,
  MenuItem,
  Chip,
  IconButton,
  Popover,
} from "@mui/material";
import AddIcon from "@mui/icons-material/Add";
import SaveIcon from "@mui/icons-material/Save";
import AutocompleteFields from "../condition/AutocompleteFields";
import OperatorSelector from "../condition/OperatorSelector";
import ListConditionPopover from "../condition/ListConditionPopover";
import { ConditionProvider } from "../../../contexts/ConditionContext";
import { useCurrentBuilder } from "../../../hooks/useContexts";
import { usePopoverRegistry } from "../../../hooks/useDialog";
import { useHoverState } from "../../../hooks/useFilter";
import {
  getOperatorsForField,
  getFieldOptionsWithVariable,
  createUpdateConditionFunction,
  createRemoveItemFunction,
  isFieldType,
} from "../../../utils/conditionHelpers";
import "katex/dist/katex.min.css";
import Latex from "react-latex-next";
import {
  mongoOperatorTypes,
  fieldOptions,
} from "../../../constants/filterConstants";

const useBlockState = (block, isRoot) => {
  const { collapsedBlocks, customBlocks } = useCurrentBuilder();

  // Memoize custom block name resolution to avoid recalculation
  const customBlockName = useMemo(() => {
    if (!block) return null;

    // Start with the block's existing custom block name
    let blockName = block.customBlockName;

    // If no custom block name, try to find it from customBlocks by ID
    if (!blockName) {
      const found = customBlocks?.find((cb) => cb.block?.id === block?.id);
      if (found) {
        blockName = found.name.replace(/^Custom\./, "");
      }
    }

    // If block has custom block name, check if it's been updated in customBlocks
    if (block.customBlockName) {
      const found = customBlocks?.find((cb) => cb.block?.id === block?.id);
      if (found) {
        const latestName = found.name.replace(/^Custom\./, "");
        if (latestName !== block.customBlockName) {
          blockName = latestName;
        }
      }
    }

    return blockName;
  }, [block, customBlocks]);

  // Memoize derived state
  const isCustomBlock = useMemo(() => !!customBlockName, [customBlockName]);

  const isCollapsed = useMemo(() => {
    if (!collapsedBlocks || !block?.id || isRoot) return false;
    return !!collapsedBlocks[block.id];
  }, [collapsedBlocks, block?.id, isRoot]);

  return {
    customBlockName,
    isCustomBlock,
    isCollapsed,
  };
};

const CustomAddElement = ({
  block,
  uiState: { activeBlockForAdd, setActiveBlockForAdd },
  customBlocks,
  defaultCondition,
  defaultBlock,
  setFilters,
  filters,
  setSpecialConditionDialog,
  setListConditionDialog,
  setCollapsedBlocks,
}) => {
  const [customBlockSearch, setCustomBlockSearch] = useState("");
  const [hoveredVariable, setHoveredVariable] = useState(false);
  const [variableButtonRef, setVariableButtonRef] = useState(null);
  const [addButtonRef, setAddButtonRef] = useState(null);

  const addItemToBlock = (blockId, category) => {
    const addToBlock = (block) => {
      if (block.id !== blockId) {
        return {
          ...block,
          children: block.children.map((child) =>
            child.category === "block" ? addToBlock(child) : child,
          ),
        };
      }
      let newItem =
        category === "condition" ? defaultCondition() : defaultBlock();
      return { ...block, children: [...block.children, newItem] };
    };
    setFilters(filters.map(addToBlock));
    setActiveBlockForAdd(null);
  };

  const handleOpenSpecialCondition = (blockId) => {
    setSpecialConditionDialog({
      open: true,
      blockId,
      equation: "yourVariableName = yourEquation",
    });
    setActiveBlockForAdd(null);
    setHoveredVariable(false);
  };

  const handleOpenListCondition = (blockId, conditionId = null) => {
    setListConditionDialog({ open: true, blockId, conditionId });
    setActiveBlockForAdd(null);
    setHoveredVariable(false);
  };

  const addCustomBlockToBlock = (blockId, customBlockName) => {
    const customBlock = customBlocks.find((cb) => cb.name === customBlockName);
    if (!customBlock) return;

    const nestedBlockIds = [];

    const collectNestedBlockIds = (block, isTopLevel = false) => {
      if (block.category === "block" && !isTopLevel) {
        nestedBlockIds.push(block.id);
      }
      if (block.children) {
        block.children.forEach((child) => collectNestedBlockIds(child, false));
      }
    };

    // Helper function to check if a condition is empty/default
    const isEmptyCondition = (condition) => {
      return (
        condition.category === "condition" &&
        (condition.field === null ||
          condition.field === undefined ||
          condition.field === "") &&
        (condition.operator === null ||
          condition.operator === undefined ||
          condition.operator === "")
      );
    };

    const addToBlock = (block) => {
      if (block.id !== blockId) {
        return {
          ...block,
          children: block.children.map((child) =>
            child.category === "block" ? addToBlock(child) : child,
          ),
        };
      }

      const cloneBlock = (block, parentName, isTopLevel = true) => {
        const newId = uuidv4();
        const clonedBlock = {
          ...block,
          id: newId,
          customBlockName: isTopLevel
            ? parentName.replace(/^Custom\./, "")
            : block.customBlockName,
          children: block.children
            ? block.children.map((child) =>
                child.category === "block"
                  ? cloneBlock(child, parentName, false)
                  : {
                      ...child,
                      id: uuidv4(),
                      // Deep clone any object properties that might contain nested data
                      ...(child.value && typeof child.value === "object"
                        ? { value: JSON.parse(JSON.stringify(child.value)) }
                        : {}),
                      ...(child.listCondition
                        ? {
                            listCondition: JSON.parse(
                              JSON.stringify(child.listCondition),
                            ),
                          }
                        : {}),
                    },
              )
            : [],
        };

        if (clonedBlock.category === "block" && !isTopLevel) {
          nestedBlockIds.push(clonedBlock.id);
        }

        return clonedBlock;
      };

      const clonedBlock = cloneBlock(customBlock.block, customBlock.name, true);

      // Check if this block only has one child and it's an empty condition
      // If so, remove it before adding the custom block
      let updatedChildren = [...block.children];
      if (
        updatedChildren.length === 1 &&
        isEmptyCondition(updatedChildren[0])
      ) {
        updatedChildren = [];
      }

      return { ...block, children: [...updatedChildren, clonedBlock] };
    };

    setFilters(filters.map(addToBlock));

    if (nestedBlockIds.length > 0 && setCollapsedBlocks) {
      setCollapsedBlocks((prev) => {
        const newCollapsed = { ...prev };
        nestedBlockIds.forEach((id) => {
          newCollapsed[id] = true;
        });
        return newCollapsed;
      });
    }
    setActiveBlockForAdd(null);
  };

  return (
    <Box>
      <Button
        ref={setAddButtonRef}
        variant="contained"
        size="medium"
        startIcon={<AddIcon />}
        onClick={() =>
          setActiveBlockForAdd(activeBlockForAdd === block.id ? null : block.id)
        }
        sx={{
          minHeight: 40, // Match the typical height of a small Select component
          px: 2, // Add some horizontal padding to match Select width better
        }}
      >
        Add
      </Button>

      <Popper
        open={activeBlockForAdd === block?.id}
        anchorEl={addButtonRef}
        placement="bottom-start"
        sx={{ zIndex: 1500 }}
        modifiers={[
          {
            name: "offset",
            options: {
              offset: [0, 8],
            },
          },
        ]}
      >
        <ClickAwayListener onClickAway={() => setActiveBlockForAdd(null)}>
          <Paper
            sx={{
              minWidth: 220,
              maxWidth: 300,
              maxHeight: 340,
              overflowY: "auto",
              p: 1,
              boxShadow: "0 4px 24px 0 rgba(80,120,255,0.13)",
              // Hide scrollbars while keeping scroll functionality
              "&::-webkit-scrollbar": {
                display: "none",
              },
              msOverflowStyle: "none", // IE and Edge
              scrollbarWidth: "none", // Firefox
            }}
          >
            <Button
              fullWidth
              variant="text"
              sx={{
                mb: 0.5,
                justifyContent: "flex-start",
                fontWeight: 600,
                borderRadius: 1,
                "&:hover": {
                  bgcolor: "info.light",
                },
              }}
              onClick={() => addItemToBlock(block.id, "condition")}
            >
              + Condition
            </Button>

            {/* Variable Button with submenu */}
            <Box
              sx={{
                position: "relative",
                mb: 0.5,
              }}
              onMouseEnter={() => setHoveredVariable(true)}
              onMouseLeave={() => setHoveredVariable(false)}
            >
              <Button
                ref={setVariableButtonRef}
                fullWidth
                variant="text"
                sx={{
                  justifyContent: "flex-start",
                  fontWeight: 600,
                  borderRadius: 1,
                  color: "warning.dark",
                  "&:hover": {
                    bgcolor: "warning.light",
                  },
                }}
              >
                + Variable
              </Button>

              <Popper
                open={hoveredVariable}
                anchorEl={variableButtonRef}
                placement="right-start"
                sx={{ zIndex: 2000 }}
                modifiers={[
                  {
                    name: "offset",
                    options: {
                      offset: [0, -4],
                    },
                  },
                ]}
              >
                <Paper
                  sx={{
                    minWidth: 180,
                    p: 0.5,
                    boxShadow: "0 4px 24px 0 rgba(180,83,9,0.13)",
                  }}
                  onMouseEnter={() => setHoveredVariable(true)}
                  onMouseLeave={() => setHoveredVariable(false)}
                >
                  <Button
                    fullWidth
                    variant="text"
                    sx={{
                      justifyContent: "flex-start",
                      fontWeight: 600,
                      borderRadius: 1,
                      color: "warning.dark",
                      fontSize: "0.875rem",
                      "&:hover": {
                        bgcolor: "warning.light",
                      },
                    }}
                    onClick={() => handleOpenSpecialCondition(block.id)}
                  >
                    + Arithmetic
                  </Button>
                  <Button
                    fullWidth
                    variant="text"
                    sx={{
                      justifyContent: "flex-start",
                      fontWeight: 600,
                      borderRadius: 1,
                      color: "success.dark",
                      fontSize: "0.875rem",
                      "&:hover": {
                        bgcolor: "success.light",
                      },
                    }}
                    onClick={() => handleOpenListCondition(block.id)}
                  >
                    + List
                  </Button>
                </Paper>
              </Popper>
            </Box>

            <Button
              fullWidth
              variant="text"
              sx={{
                mb: 1,
                justifyContent: "flex-start",
                fontWeight: 600,
                borderRadius: 1,
                "&:hover": {
                  bgcolor: "info.light",
                },
              }}
              onClick={() => addItemToBlock(block.id, "block")}
            >
              + Block
            </Button>

            {customBlocks.length > 0 && (
              <>
                <Divider sx={{ my: 1 }} />
                <Typography
                  variant="caption"
                  sx={{
                    color: "text.secondary",
                    px: 1,
                    pb: 0.5,
                    display: "block",
                  }}
                >
                  Custom Blocks
                </Typography>

                <TextField
                  size="small"
                  placeholder="Search custom blocks..."
                  fullWidth
                  sx={{ mb: 1 }}
                  value={customBlockSearch}
                  onChange={(e) => setCustomBlockSearch(e.target.value)}
                />

                <Box
                  sx={{
                    maxHeight: 180,
                    overflowY: "auto",
                    // Hide scrollbars while keeping scroll functionality
                    "&::-webkit-scrollbar": {
                      display: "none",
                    },
                    msOverflowStyle: "none", // IE and Edge
                    scrollbarWidth: "none", // Firefox
                  }}
                >
                  {customBlocks
                    .filter(
                      (cb) =>
                        !customBlockSearch ||
                        cb.name
                          .replace(/^Custom\./, "")
                          .toLowerCase()
                          .includes(customBlockSearch.toLowerCase()),
                    )
                    .map((cb) => (
                      <Button
                        key={cb.name}
                        fullWidth
                        variant="text"
                        sx={{
                          justifyContent: "flex-start",
                          fontWeight: 600,
                          borderRadius: 1,
                          color: "secondary.dark",
                          mb: 0.5,
                          "&:hover": {
                            bgcolor: "secondary.light",
                          },
                        }}
                        onClick={() => addCustomBlockToBlock(block.id, cb.name)}
                      >
                        {cb.name.replace(/^Custom\./, "")}
                      </Button>
                    ))}
                </Box>
              </>
            )}
          </Paper>
        </ClickAwayListener>
      </Popper>
    </Box>
  );
};

CustomAddElement.propTypes = {
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
    children: PropTypes.array,
    category: PropTypes.string,
    customBlockName: PropTypes.string,
  }).isRequired,
  uiState: PropTypes.shape({
    activeBlockForAdd: PropTypes.string,
    setActiveBlockForAdd: PropTypes.func.isRequired,
  }).isRequired,
  customBlocks: PropTypes.arrayOf(
    PropTypes.shape({
      name: PropTypes.string.isRequired,
      block: PropTypes.object,
    }),
  ).isRequired,
  defaultCondition: PropTypes.func.isRequired,
  defaultBlock: PropTypes.func.isRequired,
  setFilters: PropTypes.func.isRequired,
  filters: PropTypes.array.isRequired,
  setSpecialConditionDialog: PropTypes.func.isRequired,
  setListConditionDialog: PropTypes.func.isRequired,
  setCollapsedBlocks: PropTypes.func.isRequired,
};

const SaveBlockComponent = ({
  setSaveDialog,
  setSaveName,
  setSaveError,
  setFilters,
  isCustomBlock,
  isCollapsed,
  block,
}) => {
  // TODO: Implement robust validation logic for the block
  const validateBlock = (block) => {
    if (block.category === "condition") {
      if (block.isListVariable) {
        return !!block.field;
      }
      // Regular conditions need field, operator, and value
      return !!block.field && !!block.operator && block.value !== "";
    }
    if (block.category === "block") {
      return block.children.length > 0 && block.children.every(validateBlock);
    }
    return false;
  };

  const handleSaveBlock = (block) => {
    if (!validateBlock(block)) {
      setSaveError("Please fill all fields before saving.");
      setTimeout(() => setSaveError(""), 2000);
      return;
    }
    setFilters((prevFilters) => {
      const updateBlock = (b) => {
        if (b.id !== block.id) {
          return {
            ...b,
            children: b.children
              ? b.children.map((child) =>
                  child.category === "block" ? updateBlock(child) : child,
                )
              : [],
          };
        }
        return { ...b, isTrue: true };
      };
      return prevFilters.map(updateBlock);
    });
    block = { ...block, isTrue: true };
    setSaveDialog({ open: true, block });
    setSaveName("");
    setSaveError("");
  };

  return (
    <>
      {/* Save Block Button (always right-aligned) */}
      {!isCustomBlock || !isCollapsed ? (
        <Button
          size="medium"
          startIcon={<SaveIcon />}
          variant="outlined"
          onClick={() => handleSaveBlock(block)}
          sx={{
            minHeight: 40, // Match the typical height of a small Select component
            px: 2, // Add some horizontal padding to match Select width better
          }}
        >
          Save Block
        </Button>
      ) : null}
    </>
  );
};

SaveBlockComponent.propTypes = {
  setSaveDialog: PropTypes.func.isRequired,
  setSaveName: PropTypes.func.isRequired,
  setSaveError: PropTypes.func.isRequired,
  setFilters: PropTypes.func.isRequired,
  isCustomBlock: PropTypes.bool.isRequired,
  isCollapsed: PropTypes.bool.isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
    category: PropTypes.string,
    children: PropTypes.array,
    field: PropTypes.string,
    operator: PropTypes.string,
    value: PropTypes.any,
    isListVariable: PropTypes.bool,
  }).isRequired,
};

const EquationPopover = ({
  openEquationIds,
  conditionId,
  selectedChip,
  fieldOptionsWithVariable,
  conditionOrBlock,
  customVariables,
  anchorEl,
  onClose,
}) => {
  const isOpen = openEquationIds.includes(conditionId) && anchorEl;

  if (!isOpen) return null;

  let variableOption;
  if (selectedChip === "left") {
    variableOption = fieldOptionsWithVariable.find(
      (opt) =>
        opt.label ===
          (conditionOrBlock.field || conditionOrBlock.variableName) &&
        opt.isVariable,
    );
  }
  if (selectedChip === "right") {
    variableOption = fieldOptionsWithVariable.find(
      (opt) => opt.label === conditionOrBlock.value && opt.isVariable,
    );
  }

  if (!variableOption) return null;

  const eqObj = customVariables.find(
    (eq) => eq.variable === variableOption.label,
  );
  const equation = eqObj ? eqObj.equation : variableOption.equation;

  return (
    <Popover
      open={!!isOpen}
      anchorEl={anchorEl}
      onClose={onClose}
      anchorOrigin={{
        vertical: "center",
        horizontal: "right",
      }}
      transformOrigin={{
        vertical: "center",
        horizontal: "left",
      }}
      sx={{
        "& .MuiPopover-paper": {
          maxWidth: 600,
          minWidth: 300,
        },
      }}
    >
      <Paper
        elevation={3}
        sx={{
          p: 2,
          background: "#fef3c7",
          border: "1px solid #fde68a",
          borderRadius: 2,
        }}
      >
        <Latex>{`$$${equation}$$`}</Latex>
      </Paper>
    </Popover>
  );
};

EquationPopover.propTypes = {
  openEquationIds: PropTypes.arrayOf(PropTypes.string).isRequired,
  conditionId: PropTypes.string.isRequired,
  selectedChip: PropTypes.string.isRequired,
  fieldOptionsWithVariable: PropTypes.array.isRequired,
  conditionOrBlock: PropTypes.shape({
    field: PropTypes.string,
    variableName: PropTypes.string,
    value: PropTypes.any,
    fieldType: PropTypes.string,
    booleanSwitch: PropTypes.bool,
  }).isRequired,
  customVariables: PropTypes.array.isRequired,
  anchorEl: PropTypes.object,
  onClose: PropTypes.func.isRequired,
};

const ValueInput = ({
  conditionOrBlock,
  block,
  updateCondition,
  getFieldOptionsWithVariable,
  setOpenEquationIds,
  setSelectedChip,
  setEquationAnchor = null,
}) => {
  const { customListVariables, customVariables, fieldOptionsList } =
    useConditionContext();

  // Check for operators that have their own UI components - don't show autocomplete fields for these
  if (shouldSkipValueInput(conditionOrBlock)) {
    return null;
  }

  // Check if this is a boolean field - don't show right autocomplete for boolean fields
  if (isBooleanField(conditionOrBlock, customVariables, fieldOptionsList)) {
    return null;
  }

  // Check if this is an array field with an array operator that should show "+ List Variable" button
  if (
    isArrayFieldWithArrayOperator(
      conditionOrBlock,
      customVariables,
      fieldOptionsList,
    )
  ) {
    return (
      <ArrayFieldInput conditionOrBlock={conditionOrBlock} block={block} />
    );
  }

  // Check if this is a list variable
  const listVariable = customListVariables.find(
    (lv) => lv.name === conditionOrBlock.field,
  );
  if (listVariable) {
    return (
      <ListVariableInput
        listVariable={listVariable}
        conditionOrBlock={conditionOrBlock}
        block={block}
        updateCondition={updateCondition}
        getFieldOptionsWithVariable={getFieldOptionsWithVariable}
        setOpenEquationIds={setOpenEquationIds}
        setSelectedChip={setSelectedChip}
        setEquationAnchor={setEquationAnchor}
      />
    );
  }

  // Regular value input
  return (
    <RegularValueInput
      conditionOrBlock={conditionOrBlock}
      block={block}
      updateCondition={updateCondition}
      getFieldOptionsWithVariable={getFieldOptionsWithVariable}
      setOpenEquationIds={setOpenEquationIds}
      setSelectedChip={setSelectedChip}
      setEquationAnchor={setEquationAnchor}
    />
  );
};

ValueInput.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
    field: PropTypes.string,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  updateCondition: PropTypes.func.isRequired,
  getFieldOptionsWithVariable: PropTypes.func,
  setOpenEquationIds: PropTypes.func.isRequired,
  setSelectedChip: PropTypes.func.isRequired,
  setEquationAnchor: PropTypes.func,
};

const shouldSkipValueInput = (conditionOrBlock) => {
  return (
    mongoOperatorTypes[conditionOrBlock.operator] === "exists" ||
    mongoOperatorTypes[conditionOrBlock.operator] === "array_single" ||
    mongoOperatorTypes[conditionOrBlock.operator] === "array_number" ||
    mongoOperatorTypes[conditionOrBlock.operator] === "round" ||
    (mongoOperatorTypes[conditionOrBlock.operator] === "aggregation" &&
      conditionOrBlock.value &&
      typeof conditionOrBlock.value === "object" &&
      conditionOrBlock.value.type === "array" &&
      conditionOrBlock.value.subField)
  );
};

shouldSkipValueInput.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
};

const isBooleanField = (
  conditionOrBlock,
  customVariables,
  fieldOptionsList,
) => {
  const fieldVar = customVariables?.find(
    (v) => v.name === conditionOrBlock.field,
  );
  const fieldObjList = fieldOptionsList
    ? fieldOptionsList.find((f) => f.label === conditionOrBlock.field)
    : null;
  const baseFieldOption = fieldOptions.find(
    (f) => f.label === conditionOrBlock.field,
  );

  return (
    fieldVar?.type === "boolean" ||
    fieldObjList?.type === "boolean" ||
    baseFieldOption?.type === "boolean"
  );
};

isBooleanField.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  customVariables: PropTypes.array.isRequired,
  fieldOptionsList: PropTypes.array.isRequired,
};

const isArrayFieldWithArrayOperator = (
  conditionOrBlock,
  customVariables,
  fieldOptionsList,
) => {
  const fieldVar = customVariables?.find(
    (v) => v.name === conditionOrBlock.field,
  );
  const fieldObjList = fieldOptionsList
    ? fieldOptionsList.find((f) => f.label === conditionOrBlock.field)
    : null;
  const baseFieldOption = fieldOptions.find(
    (f) => f.label === conditionOrBlock.field,
  );

  const isArrayField =
    fieldVar?.type === "array" ||
    fieldObjList?.type === "array" ||
    baseFieldOption?.type === "array";
  const currentOperator = conditionOrBlock.operator;

  // Operators that should show the "+ List Variable" button for array fields
  const arrayOperatorsForButton = [
    "$filter",
    "$min",
    "$max",
    "$avg",
    "$sum",
    "$anyElementTrue",
    "$allElementsTrue",
  ];

  return isArrayField && arrayOperatorsForButton.includes(currentOperator);
};

isArrayFieldWithArrayOperator.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  customVariables: PropTypes.array.isRequired,
  fieldOptionsList: PropTypes.array.isRequired,
};

const ArrayFieldInput = ({ conditionOrBlock, block }) => {
  const { setListConditionDialog } = useConditionContext();

  // For all array operators that should show the "+ List Variable" button
  return (
    <Button
      variant="outlined"
      color="primary"
      onClick={() => {
        if (setListConditionDialog) {
          setListConditionDialog({
            open: true,
            blockId: block.id,
            conditionId: conditionOrBlock.id,
          });
        }
      }}
      sx={{
        minWidth: 150,
        height: 40,
        borderStyle: "fixed",
        borderWidth: 2,
        "&:hover": {
          borderStyle: "fixed",
          borderWidth: 2,
        },
      }}
    >
      + List Variable
    </Button>
  );
};

ArrayFieldInput.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
};

const ListVariableInput = ({
  listVariable,
  conditionOrBlock,
  block,
  updateCondition,
  getFieldOptionsWithVariable,
  setOpenEquationIds,
  setSelectedChip,
  setEquationAnchor = null,
}) => {
  const { customListVariables, isListDialogOpen, setListConditionDialog } =
    useConditionContext();
  const operator =
    listVariable.listCondition?.operator || listVariable.operator;
  const selectedOperator = conditionOrBlock.operator;

  // For $in and $nin operators, always show AutocompleteFields
  if (selectedOperator === "$in" || selectedOperator === "$nin") {
    return (
      <AutocompleteFields
        key={`${conditionOrBlock.id}.right`}
        fieldOptions={getFieldOptionsWithVariable()}
        value={conditionOrBlock.value}
        onChange={(newValue) =>
          updateCondition(block.id, conditionOrBlock.id, "value", newValue)
        }
        conditionOrBlock={conditionOrBlock}
        setOpenEquationIds={setOpenEquationIds}
        customVariables={[]}
        setSelectedChip={setSelectedChip}
        side={"right"}
        style={{ width: "100%" }}
        isListDialog={isListDialogOpen}
        customListVariables={customListVariables}
        setEquationAnchor={setEquationAnchor}
      />
    );
  }

  // For array operators ($anyElementTrue, $allElementsTrue), show a boolean switch
  if (
    conditionOrBlock.operator === "$anyElementTrue" ||
    conditionOrBlock.operator === "$allElementsTrue"
  ) {
    return (
      <FormControlLabel
        control={
          <Switch
            checked={conditionOrBlock.booleanSwitch !== false}
            onChange={(e) =>
              updateCondition(
                block.id,
                conditionOrBlock.id,
                "booleanSwitch",
                e.target.checked,
              )
            }
            color="primary"
          />
        }
        label={conditionOrBlock.booleanSwitch !== false ? "True" : "False"}
        labelPlacement="end"
        style={{ marginLeft: 0, marginRight: 8, minWidth: 100 }}
      />
    );
  }

  // For aggregation list variables with comparison operators, show regular value input
  if (
    mongoOperatorTypes[operator] === "aggregation" &&
    mongoOperatorTypes[conditionOrBlock.operator] === "comparison"
  ) {
    return (
      <AutocompleteFields
        key={`${conditionOrBlock.id}.right`}
        fieldOptions={getFieldOptionsWithVariable()}
        value={conditionOrBlock.value}
        onChange={(newValue) =>
          updateCondition(block.id, conditionOrBlock.id, "value", newValue)
        }
        conditionOrBlock={conditionOrBlock}
        setOpenEquationIds={setOpenEquationIds}
        customVariables={[]}
        setSelectedChip={setSelectedChip}
        side={"right"}
        style={{ width: "100%" }}
        isListDialog={isListDialogOpen}
        customListVariables={customListVariables}
        setEquationAnchor={setEquationAnchor}
      />
    );
  }

  // Check if we need the "+ List Variable" button
  const isFilterVariable =
    listVariable &&
    (listVariable.listCondition?.operator === "$filter" ||
      listVariable.operator === "$filter");
  const currentOperator = conditionOrBlock.operator;
  const isArrayOrAggregationOperator =
    mongoOperatorTypes[currentOperator] === "array" ||
    mongoOperatorTypes[currentOperator] === "aggregation";

  if (isFilterVariable && isArrayOrAggregationOperator) {
    return (
      <Button
        variant="outlined"
        color="primary"
        onClick={() => {
          if (setListConditionDialog) {
            setListConditionDialog({
              open: true,
              blockId: block.id,
              conditionId: conditionOrBlock.id,
            });
          }
        }}
        sx={{
          minWidth: 150,
          height: 40,
          borderStyle: "fixed",
          borderWidth: 2,
          "&:hover": {
            borderStyle: "fixed",
            borderWidth: 2,
          },
        }}
      >
        + List Variable
      </Button>
    );
  }

  // For other list variables, show empty AutocompleteFields (disabled)
  return (
    <AutocompleteFields
      key={`${conditionOrBlock.id}.right`}
      fieldOptions={[]}
      value={""}
      onChange={() => {}} // Disabled for list variables
      conditionOrBlock={{ ...conditionOrBlock, value: "" }}
      setOpenEquationIds={setOpenEquationIds}
      customVariables={[]}
      setSelectedChip={setSelectedChip}
      side={"right"}
      style={{ width: "100%", opacity: 0.5 }}
      isListDialog={isListDialogOpen}
      customListVariables={customListVariables}
      setEquationAnchor={setEquationAnchor}
    />
  );
};

ListVariableInput.propTypes = {
  listVariable: PropTypes.shape({
    name: PropTypes.string.isRequired,
    type: PropTypes.string.isRequired,
    value: PropTypes.any,
    operator: PropTypes.string,
    listCondition: PropTypes.object,
  }).isRequired,
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
    booleanSwitch: PropTypes.bool,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  updateCondition: PropTypes.func.isRequired,
  getFieldOptionsWithVariable: PropTypes.func.isRequired,
  setOpenEquationIds: PropTypes.func.isRequired,
  setSelectedChip: PropTypes.func.isRequired,
  setEquationAnchor: PropTypes.func,
};

const RegularValueInput = ({
  conditionOrBlock,
  block,
  updateCondition,
  getFieldOptionsWithVariable,
  setOpenEquationIds,
  setSelectedChip,
  setEquationAnchor = null,
}) => {
  const { customListVariables, isListDialogOpen } = useConditionContext();

  return (
    <AutocompleteFields
      key={`${conditionOrBlock.id}.right`}
      fieldOptions={getFieldOptionsWithVariable()}
      value={(() => {
        // Check if this is an aggregation operator that should be shown on the left
        const isAggregationOnLeft =
          conditionOrBlock.value &&
          typeof conditionOrBlock.value === "object" &&
          conditionOrBlock.value.type === "array" &&
          conditionOrBlock.value.subField &&
          ["$min", "$max", "$avg", "$sum"].includes(conditionOrBlock.operator);

        return isAggregationOnLeft ? "" : conditionOrBlock.value;
      })()}
      onChange={(newValue) => {
        updateCondition(block.id, conditionOrBlock.id, "value", newValue);
      }}
      conditionOrBlock={(() => {
        // Check if this is an aggregation operator that should be shown on the left
        const isAggregationOnLeft =
          conditionOrBlock.value &&
          typeof conditionOrBlock.value === "object" &&
          conditionOrBlock.value.type === "array" &&
          conditionOrBlock.value.subField &&
          ["$min", "$max", "$avg", "$sum"].includes(conditionOrBlock.operator);

        if (isAggregationOnLeft) {
          return {
            ...conditionOrBlock,
            value: "", // This prevents the aggregation chip from showing on the right
          };
        }

        return conditionOrBlock;
      })()}
      setOpenEquationIds={setOpenEquationIds}
      customVariables={[]}
      setSelectedChip={setSelectedChip}
      side={"right"}
      style={{ width: "100%" }}
      isListDialog={isListDialogOpen}
      customListVariables={customListVariables}
      setEquationAnchor={setEquationAnchor}
    />
  );
};

RegularValueInput.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  updateCondition: PropTypes.func.isRequired,
  getFieldOptionsWithVariable: PropTypes.func,
  setOpenEquationIds: PropTypes.func.isRequired,
  setSelectedChip: PropTypes.func.isRequired,
  setEquationAnchor: PropTypes.func,
};

const BlockHeader = ({
  block,
  parentBlockId,
  isRoot,
  blockState: { customBlockName, isCollapsed, isCustomBlock },
  uiState: { activeBlockForAdd, setActiveBlockForAdd },
  localFilters = null,
  setLocalFilters = null,
  isStickyHeader = false,
}) => {
  const {
    filters: contextFilters,
    setFilters: contextSetFilters,
    setCollapsedBlocks,
    setSaveDialog,
    setSaveName,
    setSaveError,
    createDefaultCondition,
    createDefaultBlock,
    setSpecialConditionDialog,
    setListConditionDialog,
    customBlocks,
    updateBlockLogic,
    removeBlock,
    addConditionToBlock,
  } = useFilterBuilder();

  // Use local filters if provided, otherwise use context filters
  const filters = localFilters || contextFilters;
  const setFilters = setLocalFilters || contextSetFilters;

  // Create a wrapper for removeBlock that works with both local and context filters
  const handleRemoveBlock = (blockId, parentBlockId) => {
    if (localFilters && setLocalFilters) {
      // Use local filter handling
      if (parentBlockId === null) return;

      const updatedFilters = localFilters.map((currentBlock) => {
        const removeBlockFromTree = (blockToUpdate) => {
          if (blockToUpdate.id !== parentBlockId) {
            return {
              ...blockToUpdate,
              children: blockToUpdate.children.map((child) =>
                child.category === "block" ? removeBlockFromTree(child) : child,
              ),
            };
          }
          return {
            ...blockToUpdate,
            children: blockToUpdate.children.filter(
              (child) => child.id !== blockId,
            ),
          };
        };
        return removeBlockFromTree(currentBlock);
      });
      setLocalFilters(updatedFilters);
    } else {
      // Use context removeBlock function
      removeBlock(blockId, parentBlockId);
    }
  };

  const resetBlockToOriginal = (blockId, customBlockName) => {
    const customBlock = customBlocks.find((cb) => cb.name === customBlockName);
    if (!customBlock) return;

    // Collect all nested block IDs that will be created
    const nestedBlockIds = [];

    // Deep clone the original block, but keep the current block id and parent linkage
    const cloneWithId = (block, newId, isTopLevel = true) => {
      const { ...rest } = block;
      const clonedBlock = {
        ...rest,
        id: newId,
        createdAt: Date.now(),
        children: block.children
          ? block.children.map((child) =>
              child.category === "block"
                ? cloneWithId(child, uuidv4(), false) // Pass false for nested blocks
                : { ...child, id: uuidv4() },
            )
          : [],
        // Only set customBlockName on the top level block, preserve existing names for nested blocks
        customBlockName: isTopLevel ? customBlockName : block.customBlockName,
      };

      // If this is a nested block, add its ID to the list for collapsing
      if (clonedBlock.category === "block" && !isTopLevel) {
        nestedBlockIds.push(clonedBlock.id);
      }

      return clonedBlock;
    };

    setFilters((prevFilters) => {
      const updateBlock = (block) => {
        if (block.id !== blockId) {
          return {
            ...block,
            children: block.children.map((child) =>
              child.category === "block" ? updateBlock(child) : child,
            ),
          };
        }
        // Replace block with original, but keep the same id
        const original = cloneWithId(customBlock.block, blockId, true);
        return { ...original };
      };
      return prevFilters.map(updateBlock);
    });

    // Set all nested blocks as collapsed
    if (nestedBlockIds.length > 0) {
      setCollapsedBlocks((prev) => {
        const newCollapsed = { ...prev };
        nestedBlockIds.forEach((id) => {
          newCollapsed[id] = true;
        });
        return newCollapsed;
      });
    }
  };

  // A REVOIR. QUAND LES ID SONT DIFFÉRENTS, RETURNS FALSE WHILE IT SHOULD BE TRUE IF THAT'S THE ONLY DIFFERENCE
  const isBlockEdited = (block, customBlocks) => {
    // Only applies to custom blocks
    const customBlock = customBlocks.find(
      (cb) => cb.name === block.customBlockName,
    );
    if (!customBlock) return false;
    // Deep compare block and customBlock.block (excluding id, createdAt, customBlockName)
    const omitMeta = (obj) => {
      if (Array.isArray(obj)) return obj.map(omitMeta);
      if (obj && typeof obj === "object") {
        const { ...rest } = obj;
        const result = {};
        for (const k in rest) {
          result[k] = omitMeta(rest[k]);
        }
        return result;
      }
      return obj;
    };

    const a = omitMeta(block);
    const b = omitMeta(customBlock.block);
    return JSON.stringify(a) !== JSON.stringify(b);
  };

  const edited = isCustomBlock && !isBlockEdited(block, customBlocks);

  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        gap: 1,
        position: isStickyHeader ? "sticky" : "relative",
        top: isStickyHeader ? 0 : "auto",
        zIndex: isStickyHeader ? 1000 : "auto",
        backgroundColor: isStickyHeader ? "background.paper" : "transparent",
        borderRadius: isStickyHeader ? "8px 8px 0 0" : "0",
        p: isStickyHeader ? 2 : 1,
        mx: isStickyHeader ? -2 : 0, // Compensate for container padding
        mt: isStickyHeader ? -2 : 0, // Compensate for container padding
        mb: isStickyHeader ? 1 : 0,
        border: isStickyHeader ? 1 : 0,
        borderColor: isStickyHeader ? "grey.300" : "transparent",
        borderBottom: isStickyHeader ? 0 : 0, // Remove bottom border to connect with content
        justifyContent: "space-between",
      }}
    >
      <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
        {/* Collapse/Expand and Delete buttons (left) */}
        {!isRoot && (
          <>
            <Button
              size="small"
              onClick={() =>
                setCollapsedBlocks((prev) => ({
                  ...prev,
                  [block.id]: !prev[block.id],
                }))
              }
              style={blockHeaderStyles.collapseButton}
            >
              {isCollapsed ? <ExpandMoreIcon /> : <ExpandLessIcon />}
            </Button>
            <IconButton
              size="small"
              color="error"
              onClick={() => handleRemoveBlock(block.id, parentBlockId)}
              sx={{ p: 0.5 }}
            >
              <ClearIcon fontSize="small" />
            </IconButton>
          </>
        )}

        {/* Controls (except Save) */}
        {isCustomBlock && isCollapsed ? null : (
          <>
            <FormControl size="small" sx={{ minWidth: 80 }}>
              <Select
                value={(block?.operator || block?.logic || "and").toLowerCase()}
                onChange={(e) => {
                  if (localFilters && setLocalFilters) {
                    const updatedFilters = localFilters.map((currentBlock) => {
                      const updateBlockOperator = (blockToUpdate) => {
                        if (blockToUpdate.id === block.id) {
                          return {
                            ...blockToUpdate,
                            operator: e.target.value.toLowerCase(),
                            logic: e.target.value,
                          };
                        }
                        if (blockToUpdate.children) {
                          return {
                            ...blockToUpdate,
                            children: blockToUpdate.children.map((child) =>
                              child.category === "block"
                                ? updateBlockOperator(child)
                                : child,
                            ),
                          };
                        }
                        return blockToUpdate;
                      };
                      return updateBlockOperator(currentBlock);
                    });
                    console.log("Updated filters:", updatedFilters);
                    setLocalFilters(updatedFilters);
                  } else {
                    // Fallback to context update
                    updateBlockLogic(block.id, e.target.value);
                  }
                }}
              >
                <MenuItem value="and">And</MenuItem>
                <MenuItem value="or">Or</MenuItem>
              </Select>
            </FormControl>

            {/* Add Button with neat menu */}
            <CustomAddElement
              block={block}
              uiState={{ activeBlockForAdd, setActiveBlockForAdd }}
              customBlocks={customBlocks}
              defaultCondition={createDefaultCondition}
              defaultBlock={createDefaultBlock}
              setFilters={setFilters}
              filters={filters}
              setSpecialConditionDialog={setSpecialConditionDialog}
              setListConditionDialog={setListConditionDialog}
              setCollapsedBlocks={setCollapsedBlocks}
            />
          </>
        )}
      </Box>

      <Box
        sx={{
          display: "flex",
          alignItems: "center",
          ...(isCustomBlock &&
            isCollapsed && { justifyContent: "center", flex: 1 }),
        }}
      >
        {/* Custom block name and switch - centered when collapsed */}
        {isCustomBlock && (
          <Box
            sx={{
              display: "flex",
              alignItems: "center",
              gap: 1.5,
              ...(isCollapsed && { justifyContent: "center" }),
            }}
          >
            <Chip
              label={customBlockName}
              onClick={
                edited
                  ? () => resetBlockToOriginal(block.id, block.customBlockName)
                  : undefined
              }
              sx={{
                fontWeight: 600,
                px: 1,
                py: 0.5,
                cursor: edited ? "pointer" : "default",
                bgcolor: edited ? "warning.light" : "info.light",
                color: edited ? "warning.contrastText" : "primary.contrastText",
                border: edited ? 1 : 0,
                borderColor: edited ? "warning.main" : "transparent",
                transition: "all 0.2s ease",
                "&:hover": edited
                  ? {
                      bgcolor: "warning.main",
                      transform: "scale(1.02)",
                    }
                  : {},
              }}
              title={edited ? "Click to reset to original values" : undefined}
            />

            {edited && (
              <Box
                component="span"
                sx={{ ...blockHeaderStyles.editedIndicator }}
              >
                (edited)
              </Box>
            )}

            {/* Switch for custom block boolean value */}
            <Switch
              checked={block?.isTrue !== false}
              onChange={(e) => {
                // Set a 'value' property on the block for boolean state
                setFilters((prevFilters) => {
                  const updateBlock = (b) => {
                    if (b.id !== block.id) {
                      return {
                        ...b,
                        children: b.children
                          ? b.children.map((child) =>
                              child.category === "block"
                                ? updateBlock(child)
                                : child,
                            )
                          : [],
                      };
                    }
                    return { ...b, isTrue: e.target.checked };
                  };
                  return prevFilters.map(updateBlock);
                });
              }}
              color="primary"
              size="medium"
              inputProps={{ "aria-label": "Custom block boolean value" }}
            />
            <Box
              component="span"
              sx={{
                fontSize: "0.875rem",
                color: "text.secondary",
                fontWeight: 500,
                ml: 0.5,
              }}
            >
              {block?.isTrue !== false ? "True" : "False"}
            </Box>
          </Box>
        )}

        {/* Switch for non-custom blocks (keeps original position) */}
        {!isCustomBlock && (
          <Box
            sx={{
              ...blockHeaderStyles.switchContainer,
              display: "flex",
              alignItems: "center",
              gap: 0.5,
            }}
          >
            <Switch
              checked={block?.isTrue !== false}
              onChange={(e) => {
                // Set a 'value' property on the block for boolean state
                setFilters((prevFilters) => {
                  const updateBlock = (b) => {
                    if (b.id !== block.id) {
                      return {
                        ...b,
                        children: b.children
                          ? b.children.map((child) =>
                              child.category === "block"
                                ? updateBlock(child)
                                : child,
                            )
                          : [],
                      };
                    }
                    return { ...b, isTrue: e.target.checked };
                  };
                  return prevFilters.map(updateBlock);
                });
              }}
              color="primary"
              size="medium"
              inputProps={{ "aria-label": "Custom block boolean value" }}
            />
            <Box
              component="span"
              sx={{
                fontSize: "0.875rem",
                color: "text.secondary",
                fontWeight: 500,
              }}
            >
              {block?.isTrue !== false ? "True" : "False"}
            </Box>
          </Box>
        )}
      </Box>

      <SaveBlockComponent
        setSaveDialog={setSaveDialog}
        setSaveName={setSaveName}
        setSaveError={setSaveError}
        setFilters={setFilters}
        isCustomBlock={isCustomBlock}
        isCollapsed={isCollapsed}
        block={block}
      />
    </Box>
  );
};

BlockHeader.propTypes = {
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
    type: PropTypes.string,
    children: PropTypes.array.isRequired,
    isTrue: PropTypes.bool,
    logic: PropTypes.string,
    operator: PropTypes.string,
    customBlockName: PropTypes.string,
  }).isRequired,
  parentBlockId: PropTypes.string,
  isRoot: PropTypes.bool.isRequired,
  blockState: PropTypes.object.isRequired,
  uiState: PropTypes.shape({
    activeBlockForAdd: PropTypes.string,
    setActiveBlockForAdd: PropTypes.func,
  }).isRequired,
  localFilters: PropTypes.array,
  setLocalFilters: PropTypes.func,
  isStickyHeader: PropTypes.bool,
};

const SpecialOperatorInputs = ({
  conditionOrBlock,
  block,
  updateCondition,
}) => {
  if (mongoOperatorTypes[conditionOrBlock.operator] === "exists") {
    return (
      <FormControlLabel
        control={
          <Switch
            checked={conditionOrBlock.value !== false}
            onChange={(e) =>
              updateCondition(
                block.id,
                conditionOrBlock.id,
                "value",
                e.target.checked,
              )
            }
            color="primary"
          />
        }
        label={
          conditionOrBlock.operator === "$exists"
            ? conditionOrBlock.value !== false
              ? "True"
              : "False"
            : conditionOrBlock.value !== false
              ? "True"
              : "False"
        }
        labelPlacement="end"
        style={{ marginLeft: 0, marginRight: 8 }}
      />
    );
  }

  if (mongoOperatorTypes[conditionOrBlock.operator] === "round") {
    return (
      <TextField
        size="small"
        type="number"
        label="Decimal Places"
        value={
          conditionOrBlock.value !== undefined ? conditionOrBlock.value : 0
        }
        onChange={(e) =>
          updateCondition(
            block.id,
            conditionOrBlock.id,
            "value",
            parseInt(e.target.value) || 0,
          )
        }
        style={{ minWidth: 120, maxWidth: 150 }}
        inputProps={{ min: 0, max: 10 }}
      />
    );
  }

  if (mongoOperatorTypes[conditionOrBlock.operator] === "array_single") {
    return (
      <TextField
        size="small"
        type="number"
        label={
          conditionOrBlock.operator === "$lengthGt"
            ? "Length Greater Than"
            : conditionOrBlock.operator === "$lengthLt"
              ? "Length Less Than"
              : "Value"
        }
        value={conditionOrBlock.value !== "" ? conditionOrBlock.value : 0}
        onChange={(e) =>
          updateCondition(
            block.id,
            conditionOrBlock.id,
            "value",
            parseInt(e.target.value) || 0,
          )
        }
        style={{ minWidth: 120, maxWidth: 150 }}
      />
    );
  }

  if (mongoOperatorTypes[conditionOrBlock.operator] === "array_number") {
    return (
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <TextField
          size="small"
          type="number"
          label="Divisor"
          value={
            Array.isArray(conditionOrBlock.value)
              ? conditionOrBlock.value[0] || ""
              : ""
          }
          onChange={(e) => {
            const divisor = parseInt(e.target.value) || 0;
            const remainder = Array.isArray(conditionOrBlock.value)
              ? conditionOrBlock.value[1] || 0
              : 0;
            updateCondition(block.id, conditionOrBlock.id, "value", [
              divisor,
              remainder,
            ]);
          }}
          style={{ minWidth: 80, maxWidth: 100 }}
        />
        <TextField
          size="small"
          type="number"
          label="Remainder"
          value={
            Array.isArray(conditionOrBlock.value)
              ? conditionOrBlock.value[1] || ""
              : ""
          }
          onChange={(e) => {
            const remainder = parseInt(e.target.value) || 0;
            const divisor = Array.isArray(conditionOrBlock.value)
              ? conditionOrBlock.value[0] || 0
              : 0;
            updateCondition(block.id, conditionOrBlock.id, "value", [
              divisor,
              remainder,
            ]);
          }}
          style={{ minWidth: 80, maxWidth: 100 }}
        />
      </div>
    );
  }

  // For aggregation operators, don't show anything additional
  if (
    mongoOperatorTypes[conditionOrBlock.operator] === "aggregation" ||
    (conditionOrBlock.value &&
      typeof conditionOrBlock.value === "object" &&
      conditionOrBlock.value.type === "array" &&
      conditionOrBlock.value.subField)
  ) {
    return null;
  }

  return null;
};

SpecialOperatorInputs.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  updateCondition: PropTypes.func.isRequired,
};

const ConditionComponent = ({
  conditionOrBlock,
  block,
  fieldOptionsList,
  isListDialogOpen = false,
  setListConditionDialog,
  localFilters = null,
  setLocalFilters = null,
}) => {
  const {
    filters: contextFilters,
    setFilters: contextSetFilters,
    customVariables,
    customListVariables,
    createDefaultCondition, // Fixed: use createDefaultCondition instead of defaultCondition
  } = useCurrentBuilder();

  // Use local filters if provided, otherwise use context filters
  const filters = localFilters || contextFilters;
  const setFilters = setLocalFilters || contextSetFilters;
  return (
    <ConditionProvider
      customVariables={customVariables || []}
      customListVariables={customListVariables || []}
      fieldOptionsList={fieldOptionsList}
      isListDialogOpen={isListDialogOpen}
      setListConditionDialog={setListConditionDialog}
    >
      <ConditionComponentInner
        conditionOrBlock={conditionOrBlock}
        block={block}
        setFilters={setFilters}
        filters={filters}
        createDefaultCondition={createDefaultCondition} // Fixed: pass createDefaultCondition
        customVariables={customVariables || []}
        fieldOptionsList={fieldOptionsList}
        customListVariables={customListVariables || []}
        isListDialogOpen={isListDialogOpen}
      />
    </ConditionProvider>
  );
};

ConditionComponent.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  fieldOptionsList: PropTypes.array.isRequired,
  isListDialogOpen: PropTypes.bool,
  setListConditionDialog: PropTypes.func.isRequired,
  localFilters: PropTypes.array,
  setLocalFilters: PropTypes.func,
};

const ConditionComponentInner = ({
  conditionOrBlock,
  block,
  setFilters,
  filters,
  createDefaultCondition,
  customVariables,
  fieldOptionsList,
  customListVariables,
  isListDialogOpen = false,
}) => {
  const [openEquationIds, setOpenEquationIds] = useState([]);
  const [selectedChip, setSelectedChip] = useState("");
  const [listPopoverAnchor, setListPopoverAnchor] = useState(null);
  const [equationAnchor, setEquationAnchor] = useState(null);

  // Custom hooks
  usePopoverRegistry(
    conditionOrBlock.id,
    customListVariables,
    setListPopoverAnchor,
  );
  const { isYoungestHovered, handleMouseEnter, handleMouseLeave } =
    useHoverState(conditionOrBlock.id, filters);

  // Helper functions
  const updateCondition = createUpdateConditionFunction(filters, setFilters);
  const removeItem = createRemoveItemFunction(
    filters,
    setFilters,
    createDefaultCondition,
  ); // Fixed: use createDefaultCondition
  const fieldOptionsWithVariable = getFieldOptionsWithVariable(
    fieldOptions, // Keep as fallback
    fieldOptionsList, // Use the passed prop as primary source
    customVariables,
    customListVariables,
  );
  const operatorOptions = conditionOrBlock.field
    ? getOperatorsForField(
        conditionOrBlock.field,
        customVariables,
        fieldOptionsList,
        customListVariables,
      )
    : [];

  const handleFieldChange = (newField) => {
    const ops = getOperatorsForField(
      newField,
      customVariables,
      fieldOptionsList,
      customListVariables,
    );
    const isBooleanField = isFieldType(
      newField,
      "boolean",
      customVariables,
      fieldOptionsList,
      customListVariables,
    );

    // Check if this is a list variable and get its operator
    const listVariable = customListVariables?.find(
      (lv) => lv.name === newField,
    );
    const defaultOperator = listVariable
      ? listVariable.listCondition?.operator
      : ops.length > 0
        ? ops[0]
        : null;

    setFilters((prevFilters) => {
      const updateBlockTree = (currentBlock) => {
        if (currentBlock.id !== block.id) {
          return {
            ...currentBlock,
            children: currentBlock.children.map((child) =>
              child.category === "block" ? updateBlockTree(child) : child,
            ),
          };
        }
        return {
          ...currentBlock,
          children: currentBlock.children.map((child) =>
            child.id === conditionOrBlock.id
              ? {
                  ...child,
                  field: newField,
                  variableName: undefined,
                  // Only change operator if current operator is not valid for new field
                  operator:
                    child.operator && ops.includes(child.operator)
                      ? child.operator
                      : defaultOperator,
                  // Only reset value if it's a boolean field change or if current operator is invalid
                  value: isBooleanField
                    ? typeof child.value === "boolean"
                      ? child.value
                      : true
                    : child.operator && ops.includes(child.operator)
                      ? child.value
                      : undefined,
                }
              : child,
          ),
        };
      };
      return prevFilters.map(updateBlockTree);
    });
  };

  const renderEquationPopover = () => (
    <EquationPopover
      openEquationIds={openEquationIds}
      conditionId={conditionOrBlock.id}
      selectedChip={selectedChip}
      fieldOptionsWithVariable={fieldOptionsWithVariable}
      conditionOrBlock={conditionOrBlock}
      customVariables={customVariables}
      anchorEl={equationAnchor}
      onClose={() => {
        setOpenEquationIds((prev) =>
          prev.filter((id) => id !== conditionOrBlock.id),
        );
        setEquationAnchor(null);
      }}
    />
  );

  return (
    <Box
      key={conditionOrBlock.id}
      sx={{
        ml: 2,
        width: "100%",
        display: "grid",
        gridTemplateColumns:
          "auto minmax(250px, 2fr) minmax(200px, 1fr) minmax(250px, 2fr)",
        gap: 1,
        alignItems: "center",
        maxWidth: "100%",
        position: "relative",
        transition: "all 0.2s ease",
        p: 1,
        borderRadius: 1,
        border: 1,
        borderColor: isYoungestHovered ? "primary.light" : "transparent",
        background: isYoungestHovered
          ? "linear-gradient(to right, #e3f2fd, #f3e5f5)"
          : "transparent",
        boxShadow: isYoungestHovered ? 2 : 0,
        zIndex: 1,
      }}
      onMouseEnter={() => handleMouseEnter(conditionOrBlock.id)}
      onMouseLeave={() => handleMouseLeave(conditionOrBlock.id)}
    >
      {/* Remove button */}
      <IconButton
        size="small"
        color="error"
        onClick={() => removeItem(block.id, conditionOrBlock.id)}
        sx={{ p: 0.5 }}
      >
        <ClearIcon fontSize="small" />
      </IconButton>

      {/* Field Autocomplete */}
      <FieldSelector
        conditionOrBlock={conditionOrBlock}
        fieldOptionsWithVariable={fieldOptionsWithVariable}
        handleFieldChange={handleFieldChange}
        setOpenEquationIds={setOpenEquationIds}
        setSelectedChip={setSelectedChip}
        customVariables={customVariables}
        customListVariables={customListVariables}
        isListDialogOpen={isListDialogOpen}
        setEquationAnchor={setEquationAnchor}
      />

      {/* Operator Selector */}
      <OperatorSelector
        conditionOrBlock={conditionOrBlock}
        block={block}
        operatorOptions={operatorOptions}
        updateCondition={updateCondition}
      />

      {/* Value Input */}
      <ValueInput
        conditionOrBlock={conditionOrBlock}
        block={block}
        updateCondition={updateCondition}
        getFieldOptionsWithVariable={() => fieldOptionsWithVariable}
        setOpenEquationIds={setOpenEquationIds}
        setSelectedChip={setSelectedChip}
        setEquationAnchor={setEquationAnchor}
      />

      {/* Special Operator Inputs */}
      <SpecialOperatorInputs
        conditionOrBlock={conditionOrBlock}
        block={block}
        updateCondition={updateCondition}
      />

      {/* Equation Popover */}
      {renderEquationPopover()}

      {/* List Condition Popover */}
      <ListConditionPopover
        listPopoverAnchor={listPopoverAnchor}
        setListPopoverAnchor={setListPopoverAnchor}
        conditionOrBlock={conditionOrBlock}
        customListVariables={customListVariables}
        createDefaultCondition={createDefaultCondition} // Fixed: use createDefaultCondition
        customVariables={customVariables}
        block={block}
        updateCondition={updateCondition}
      />
    </Box>
  );
};

ConditionComponentInner.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    operator: PropTypes.string,
    value: PropTypes.any,
    field: PropTypes.string,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  setFilters: PropTypes.func.isRequired,
  filters: PropTypes.array.isRequired,
  createDefaultCondition: PropTypes.func.isRequired,
  customVariables: PropTypes.array.isRequired,
  fieldOptionsList: PropTypes.array.isRequired,
  customListVariables: PropTypes.array.isRequired,
  isListDialogOpen: PropTypes.bool,
};

const FieldSelector = ({
  conditionOrBlock,
  fieldOptionsWithVariable,
  handleFieldChange,
  setOpenEquationIds,
  setSelectedChip,
  customVariables,
  customListVariables,
  setEquationAnchor,
}) => {
  const conditionComponentStyles = {
    deleteIcon: {
      color: "red",
      fontSize: "medium",
    },

    container: {
      display: "flex",
      alignItems: "center",
      width: "100%",
      minWidth: 0, // Allow shrinking in grid layout
    },
  };

  return (
    <div
      style={{
        ...conditionComponentStyles.container,
        maxWidth: "100%",
        overflow: "visible", // Changed from 'hidden' to 'visible' to allow dropdown to show
        position: "relative",
        zIndex: 1, // Reduced from 10 to avoid interfering with dropdown
      }}
    >
      <div style={{ position: "relative", width: "100%" }}>
        <AutocompleteFields
          key={`${conditionOrBlock.id}.left`}
          fieldOptions={fieldOptionsWithVariable}
          value={conditionOrBlock.field || conditionOrBlock.variableName}
          onChange={handleFieldChange}
          conditionOrBlock={conditionOrBlock}
          setOpenEquationIds={setOpenEquationIds}
          customVariables={customVariables}
          setSelectedChip={setSelectedChip}
          side={"left"}
          style={{ width: "100%" }}
          customListVariables={customListVariables}
          setEquationAnchor={setEquationAnchor}
        />
      </div>
    </div>
  );
};

FieldSelector.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    field: PropTypes.string,
    variableName: PropTypes.string,
  }).isRequired,
  fieldOptionsWithVariable: PropTypes.array.isRequired,
  handleFieldChange: PropTypes.func.isRequired,
  setOpenEquationIds: PropTypes.func.isRequired,
  setSelectedChip: PropTypes.func.isRequired,
  customVariables: PropTypes.array.isRequired,
  customListVariables: PropTypes.array.isRequired,
  setEquationAnchor: PropTypes.func.isRequired,
};

const BlockComponent = ({
  block,
  parentBlockId = null,
  isRoot = false,
  fieldOptionsList = [],
  isListDialogOpen = false,
  localFilters = null,
  setLocalFilters = null,
  stickyBlockId = null,
}) => {
  if (!block?.id) {
    console.warn("BlockComponent: Invalid block provided", block);
    return null;
  }

  const [activeBlockForAdd, setActiveBlockForAdd] = useState(null);

  // Get dialog state from FilterBuilder context
  const { setListConditionDialog } = useFilterBuilder();

  // Use custom hook for block state management
  const blockState = useBlockState(block, isRoot);

  // Determine if this specific block should have a sticky header
  const isStickyHeader = block.id === stickyBlockId;

  // Block UI interaction state - no need to memoize simple object
  const uiState = {
    activeBlockForAdd,
    setActiveBlockForAdd,
  };

  // Memoize child components to prevent unnecessary re-renders
  const renderChildren = useMemo(() => {
    if (blockState.isCollapsed || !block?.children?.length) return null;

    return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
        {block.children.map((conditionOrBlock) => {
          // Early return with error handling
          if (!conditionOrBlock?.id) {
            console.warn("BlockComponent: child missing ID", conditionOrBlock);
            return null;
          }

          if (conditionOrBlock.category === "block") {
            return (
              <BlockComponent
                key={conditionOrBlock.id}
                block={conditionOrBlock}
                parentBlockId={block.id}
                isRoot={false}
                fieldOptionsList={fieldOptionsList}
                isListDialogOpen={isListDialogOpen}
                localFilters={localFilters}
                setLocalFilters={setLocalFilters}
                stickyBlockId={stickyBlockId}
              />
            );
          }

          return (
            <ConditionComponent
              key={conditionOrBlock.id}
              conditionOrBlock={conditionOrBlock}
              block={block}
              isListDialogOpen={isListDialogOpen}
              fieldOptionsList={fieldOptionsList}
              localFilters={localFilters}
              setLocalFilters={setLocalFilters}
              setListConditionDialog={setListConditionDialog}
            />
          );
        })}
      </Box>
    );
  }, [
    blockState.isCollapsed,
    block?.children,
    block?.id,
    fieldOptionsList,
    isListDialogOpen,
    localFilters,
    setLocalFilters,
    setListConditionDialog,
    stickyBlockId,
  ]);

  return (
    <Paper
      component="section"
      sx={{
        display: "flex",
        flexDirection: "column",
        gap: isStickyHeader ? 0 : 1, // Remove gap when sticky to prevent visual separation
        p: 2,
        pt: isStickyHeader ? 0 : 2, // Remove top padding when sticky
        border: 1,
        borderColor: "grey.300",
        borderRadius: 2,
      }}
      aria-label={`${block.category} block${
        blockState.customBlockName ? ` - ${blockState.customBlockName}` : ""
      }`}
    >
      {/* Block Header - can be sticky */}
      <BlockHeader
        block={block}
        parentBlockId={parentBlockId}
        isRoot={isRoot}
        blockState={blockState}
        uiState={uiState}
        localFilters={localFilters}
        setLocalFilters={setLocalFilters}
        isStickyHeader={isStickyHeader}
      />

      {/* Content */}
      <Box>{renderChildren}</Box>
    </Paper>
  );
};

// Custom comparison function for React.memo to ensure it re-renders when stickyBlockId changes
BlockComponent.displayName = "BlockComponent";

BlockComponent.propTypes = {
  block: PropTypes.object.isRequired,
  parentBlockId: PropTypes.string,
  isRoot: PropTypes.bool,
  fieldOptionsList: PropTypes.array,
  isListDialogOpen: PropTypes.bool,
  localFilters: PropTypes.array,
  setLocalFilters: PropTypes.func,
  stickyBlockId: PropTypes.string,
};

export default BlockComponent;
