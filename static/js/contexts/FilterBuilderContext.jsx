import React, { useState, createContext } from "react";
import PropTypes from "prop-types";
import { useFilterManipulation, useFilterFactories } from "../hooks/useFilter";
import { useDialogStates } from "../hooks/useDialog";
import {
  convertToMongoAggregation,
  formatMongoAggregation,
  isValidPipeline,
} from "../utils/mongoConverter";

export const FilterBuilderContext = createContext();

export const FilterBuilderProvider = ({ children }) => {
  // Core state
  const [filters, setFilters] = useState([]);
  const [collapsedBlocks, setCollapsedBlocks] = useState({});
  const [hasInitialized, setHasInitialized] = useState(false);

  // Custom data state
  const [customBlocks, setCustomBlocks] = useState([]);
  const [customVariables, setCustomVariables] = useState([]);
  const [customListVariables, setCustomListVariables] = useState([]);

  // Local filters update function reference (to be set by FilterBuilderContent)
  const [localFiltersUpdater, setLocalFiltersUpdater] = useState(null);

  // Get hook functionalities
  const filterManipulation = useFilterManipulation(filters, setFilters);
  const factories = useFilterFactories();
  const dialogs = useDialogStates();

  // MongoDB aggregation conversion
  const generateMongoQuery = () => {
    return convertToMongoAggregation(
      filters,
      customVariables,
      customListVariables,
    );
  };

  const getFormattedMongoQuery = () => {
    const pipeline = generateMongoQuery();
    return formatMongoAggregation(pipeline);
  };

  const hasValidQuery = () => {
    const pipeline = generateMongoQuery();
    return isValidPipeline(pipeline);
  };

  // Context value
  const value = {
    // Core state
    filters,
    setFilters,
    collapsedBlocks,
    setCollapsedBlocks,
    hasInitialized,
    setHasInitialized,

    // Custom data
    customBlocks,
    setCustomBlocks,
    customVariables,
    setCustomVariables,
    customListVariables,
    setCustomListVariables,

    // Local filters updater
    localFiltersUpdater,
    setLocalFiltersUpdater,

    // Spread hook functionalities
    ...filterManipulation,
    ...factories,
    ...dialogs,

    // MongoDB aggregation functions
    generateMongoQuery,
    getFormattedMongoQuery,
    hasValidQuery,
  };

  return (
    <FilterBuilderContext.Provider value={value}>
      {children}
    </FilterBuilderContext.Provider>
  );
};

// props validation
FilterBuilderProvider.propTypes = {
  children: PropTypes.node.isRequired,
};
