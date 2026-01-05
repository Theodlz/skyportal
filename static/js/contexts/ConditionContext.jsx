import React, { createContext } from "react";
import PropTypes from "prop-types";

const ConditionContext = createContext();

export const ConditionProvider = ({
  children,
  customVariables,
  customListVariables,
  customSwitchCases,
  fieldOptionsList,
  isListDialogOpen,
  setListConditionDialog,
}) => {
  const value = {
    customVariables,
    customListVariables,
    customSwitchCases,
    fieldOptionsList,
    isListDialogOpen,
    setListConditionDialog,
  };

  return (
    <ConditionContext.Provider value={value}>
      {children}
    </ConditionContext.Provider>
  );
};

// Export the context for the hook
export { ConditionContext };

// props validation
ConditionProvider.propTypes = {
  children: PropTypes.node.isRequired,
  customVariables: PropTypes.array.isRequired,
  customListVariables: PropTypes.array.isRequired,
  customSwitchCases: PropTypes.array.isRequired,
  fieldOptionsList: PropTypes.array.isRequired,
  isListDialogOpen: PropTypes.bool.isRequired,
  setListConditionDialog: PropTypes.func.isRequired,
};
