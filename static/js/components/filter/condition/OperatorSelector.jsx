import React, { useCallback, useEffect } from "react";
import { useSelector } from "react-redux";
import PropTypes from "prop-types";
import { FormControlLabel, Switch } from "@mui/material";
import {
  mongoOperatorLabels,
  flattenFieldOptions,
} from "../../../constants/filterConstants";
import { getOperatorsForField } from "../../../utils/conditionHelpers";
import { useConditionContext } from "../../../hooks/useContexts";
import TextField from "@mui/material/TextField";
import Autocomplete from "@mui/material/Autocomplete";
import { styled, lighten, darken } from "@mui/system";

const GroupHeader = styled("div")(({ theme }) => {
  // Fallbacks for palette
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
  };
});

const GroupItems = styled("ul")({
  padding: 0,
});

const AutocompleteOperators = ({
  operators = [],
  value,
  onChange,
  mongoOperatorLabels = {},
}) => {
  // Prepare options with label
  const options = (operators || []).map((op) => ({
    value: op,
    label: mongoOperatorLabels[op] || op,
    group: "Operators",
  }));

  return (
    <Autocomplete
      size="small"
      options={options}
      groupBy={(option) => option.group}
      getOptionLabel={(option) => option.label}
      sx={{
        width: "100%",
        minWidth: 150,
        maxWidth: 300,
        "& .MuiAutocomplete-popper": {
          zIndex: 1300,
        },
      }}
      value={options.find((opt) => opt.value === value) || null}
      onChange={(_, newValue) =>
        onChange && onChange(newValue ? newValue.value : "")
      }
      renderInput={(params) => <TextField {...params} label="Operator" />}
      renderGroup={(params) => (
        <li key={params.key}>
          <GroupHeader>{params.group}</GroupHeader>
          <GroupItems>{params.children}</GroupItems>
        </li>
      )}
      isOptionEqualToValue={(option, val) => option.value === val.value}
    />
  );
};

AutocompleteOperators.propTypes = {
  operators: PropTypes.arrayOf(PropTypes.string),
  value: PropTypes.string,
  onChange: PropTypes.func,
  mongoOperatorLabels: PropTypes.object,
};

AutocompleteOperators.defaultProps = {
  operators: [],
  value: "",
  onChange: null,
  mongoOperatorLabels: {},
};

// Helper function to check if a field is boolean (same logic as ValueInput)
const isBooleanField = (
  conditionOrBlock,
  customVariables,
  fieldOptionsList,
) => {
  const schema = useSelector((state) => state.filter_modules?.schema);
  const fieldOptions = flattenFieldOptions(schema);

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

const OperatorSelector = ({
  conditionOrBlock,
  block,
  operatorOptions,
  updateCondition,
}) => {
  const { customListVariables, customVariables, fieldOptionsList } =
    useConditionContext();

  // Check if this is a list variable
  const listVariable = customListVariables.find(
    (lv) => lv.name === conditionOrBlock.field,
  );
  if (listVariable) {
    return (
      <ListVariableOperator
        conditionOrBlock={conditionOrBlock}
        block={block}
        updateCondition={updateCondition}
        listOperator={listVariable.listCondition.operator}
      />
    );
  }

  // Check if this is a boolean field - using the same logic as ValueInput
  if (isBooleanField(conditionOrBlock, customVariables, fieldOptionsList)) {
    return (
      <BooleanFieldSwitch
        conditionOrBlock={conditionOrBlock}
        block={block}
        updateCondition={updateCondition}
      />
    );
  }

  // Regular operator autocomplete
  return (
    <AutocompleteOperators
      operators={operatorOptions}
      value={conditionOrBlock.operator}
      onChange={(op) =>
        updateCondition(block.id, conditionOrBlock.id, "operator", op)
      }
      mongoOperatorLabels={mongoOperatorLabels}
      style={{ minWidth: 60, maxWidth: 80 }}
    />
  );
};

OperatorSelector.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    field: PropTypes.string,
    operator: PropTypes.string,
    value: PropTypes.any,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  operatorOptions: PropTypes.arrayOf(PropTypes.string).isRequired,
  updateCondition: PropTypes.func.isRequired,
};

const ListVariableOperator = ({
  conditionOrBlock,
  block,
  updateCondition,
  listOperator,
}) => {
  const { customVariables, fieldOptionsList, customListVariables } =
    useConditionContext();
  const schema = useSelector((state) => state.filter_modules?.schema);
  const fieldOptions = flattenFieldOptions(schema);

  const handleOperatorChange = useCallback(
    (op) => {
      updateCondition(block.id, conditionOrBlock.id, "operator", op);
    },
    [updateCondition, block.id, conditionOrBlock.id],
  );

  // Get available operators based on the list variable's creation operator
  const getAvailableOperatorsForListVariable = () => {
    const baseOperators = ["$exists", "$isNumber"];
    const lengthOperators = ["$lengthGt", "$lengthLt"];

    // If the list variable was created with $min, $max, $avg, or $sum
    if (["$min", "$max", "$avg", "$sum"].includes(listOperator)) {
      return [
        "$eq",
        "$ne",
        "$lt",
        "$lte",
        "$gt",
        "$gte",
        ...lengthOperators,
        ...baseOperators,
      ];
    }

    // If the list variable was created with $anyElementTrue or $allElementsTrue
    if (
      listOperator === "$anyElementTrue" ||
      listOperator === "$allElementsTrue"
    ) {
      return [
        "$in",
        "$nin",
        listOperator,
        ...lengthOperators,
        ...baseOperators,
      ];
    }

    // If the list variable was created with $filter or $map
    if (listOperator === "$filter" || listOperator === "$map") {
      return [
        "$in",
        "$nin",
        "$filter",
        "$map",
        ...lengthOperators,
        ...baseOperators,
      ];
    }

    // For length operators
    if (["$lengthGt", "$lengthLt"].includes(listOperator)) {
      return [
        "$eq",
        "$ne",
        "$lt",
        "$lte",
        "$gt",
        "$gte",
        ...lengthOperators,
        ...baseOperators,
      ];
    }

    // For other list variable types, get all available operators including length operators
    const standardOperators = getOperatorsForField(
      conditionOrBlock.field,
      customVariables,
      schema,
      fieldOptions,
      fieldOptionsList,
      customListVariables,
    );

    // Always include length operators for list variables
    return [
      ...new Set([...standardOperators, ...lengthOperators, ...baseOperators]),
    ];
  };

  const availableOperators = getAvailableOperatorsForListVariable();

  // Set the default operator to the first available operator if none is currently set
  useEffect(() => {
    if (!conditionOrBlock.operator && availableOperators.length > 0) {
      updateCondition(
        block.id,
        conditionOrBlock.id,
        "operator",
        availableOperators[0],
      );
    }
  }, [
    block.id,
    conditionOrBlock.id,
    conditionOrBlock.operator,
    availableOperators,
    updateCondition,
  ]);

  // Use the current operator if set, otherwise fall back to the first available operator
  const currentOperator =
    conditionOrBlock.operator ||
    (availableOperators.length > 0 ? availableOperators[0] : null);

  return (
    <AutocompleteOperators
      operators={availableOperators}
      value={currentOperator}
      onChange={handleOperatorChange}
      mongoOperatorLabels={mongoOperatorLabels}
      style={{ minWidth: 60, maxWidth: 80 }}
    />
  );
};

ListVariableOperator.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    field: PropTypes.string,
    operator: PropTypes.string,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  updateCondition: PropTypes.func.isRequired,
  listOperator: PropTypes.string.isRequired,
};

const BooleanFieldSwitch = ({ conditionOrBlock, block, updateCondition }) => {
  const handleSwitchChange = useCallback(
    (e) => {
      updateCondition(block.id, conditionOrBlock.id, "value", e.target.checked);
    },
    [updateCondition, block.id, conditionOrBlock.id],
  );

  return (
    <FormControlLabel
      control={
        <Switch
          checked={conditionOrBlock.value === true}
          onChange={handleSwitchChange}
          color="primary"
        />
      }
      label={String(conditionOrBlock.value === true)}
      labelPlacement="end"
      style={{ marginLeft: 0, marginRight: 8 }}
    />
  );
};

BooleanFieldSwitch.propTypes = {
  conditionOrBlock: PropTypes.shape({
    id: PropTypes.string.isRequired,
    value: PropTypes.any,
  }).isRequired,
  block: PropTypes.shape({
    id: PropTypes.string.isRequired,
  }).isRequired,
  updateCondition: PropTypes.func.isRequired,
};

export default OperatorSelector;
