import React, { useState, createContext, useEffect } from "react";
import PropTypes from "prop-types";
import { useDispatch, useSelector } from "react-redux";
import { useFilterManipulation, useFilterFactories } from "../hooks/useFilter";
import { useDialogStates } from "../hooks/useDialog";
import { fetchAllElements } from "../ducks/boom_filter_modules";
import {
  convertToMongoAggregation,
  formatMongoAggregation,
  isValidPipeline,
} from "../utils/mongoConverter";
import { schemaParser } from "@rjsf/utils";
import { flattenFieldOptions } from "../constants/filterConstants";

export const UnifiedBuilderContext = createContext();

export const UnifiedBuilderProvider = ({ children, mode = "filter" }) => {
  const dispatch = useDispatch();

  // Core state - can be used for both filters and annotations
  const [filters, setFilters] = useState([]);
  const [annotations, setAnnotations] = useState([]);
  const [collapsedBlocks, setCollapsedBlocks] = useState({});
  const [hasInitialized, setHasInitialized] = useState(false);

  // Local filter state management to persist across view changes
  const [localFilterData, setLocalFilterData] = useState(null);
  const [hasBeenModified, setHasBeenModified] = useState(false);

  // Projection fields state (primarily for annotations)
  const [projectionFields, setProjectionFields] = useState([]);

  // Custom data state
  const [customBlocks, setCustomBlocks] = useState([]);
  const [customVariables, setCustomVariables] = useState([]);
  const [customListVariables, setCustomListVariables] = useState([]);

  // Local filters updater function reference (for FilterBuilderContent)
  const [localFiltersUpdater, setLocalFiltersUpdater] = useState(null);

  const store_schema = useSelector((state) => state.filter_modules?.schema);

  const [schema, setSchema] = useState(null);
  const [fieldOptions, setFieldOptions] = useState([]);

  useEffect(() => {
    if (store_schema) {
      setSchema(store_schema);
      setFieldOptions(flattenFieldOptions(store_schema));
    }
  }, [store_schema]);

  // const schema = useSelector((state) => state.filter_modules?.schema);

  // Load saved data on mount (similar to useFilterBuilderData)
  useEffect(() => {
    const loadData = async () => {
      try {
        // Load all saved data in parallel using the same pattern as FilterBuilderData
        const blocks = await dispatch(fetchAllElements({ elements: "blocks" }));
        const variables = await dispatch(
          fetchAllElements({ elements: "variables" }),
        );
        const listVariables = await dispatch(
          fetchAllElements({ elements: "listVariables" }),
        );
        setCustomBlocks(blocks.data.blocks || []);
        setCustomVariables(variables.data.variables || []);
        setCustomListVariables(listVariables.data.listVariables || []);
      } catch (error) {
        console.error("Error loading unified builder data:", error);
        // Set empty arrays as fallback
        setCustomBlocks([]);
        setCustomVariables([]);
        setCustomListVariables([]);
      }
    };

    loadData();
  }, [dispatch, mode]);

  // Determine which data to use based on mode
  const currentData = mode === "annotation" ? annotations : filters;
  const setCurrentData = mode === "annotation" ? setAnnotations : setFilters;

  // Get hook functionalities
  const dataManipulation = useFilterManipulation(currentData, setCurrentData);
  const factories = useFilterFactories();
  const dialogs = useDialogStates();

  // MongoDB aggregation conversion
  const generateMongoQuery = () => {
    // Always use filters as the base query, regardless of mode
    // This ensures annotations show both filters + projections
    const baseQuery = convertToMongoAggregation(
      filters,
      schema,
      fieldOptions,
      customVariables,
      customListVariables,
    );

    // If there are projection fields (annotations), add projection stage
    if (projectionFields && projectionFields.length > 0) {
      // Generate projection stage
      const projection = { objectId: 1 };
      const annotations = {};

      projectionFields.forEach((field) => {
        if (!field.fieldName || field.fieldName === "objectId") return;
        const outputName = field.outputName || field.fieldName;

        // For now, handle basic projection types
        switch (field.type) {
          case "include":
            annotations[outputName] = `$${field.fieldName}`;
            break;
          case "exclude":
            annotations[outputName] = 0;
            break;
          case "round":
            annotations[outputName] = {
              $round: [`$${field.fieldName}`, field.roundDecimals || 4],
            };
            break;
          default:
            annotations[outputName] = `$${field.fieldName}`;
        }
      });

      // Add annotations to projection if there are any
      if (Object.keys(annotations).length > 0) {
        projection.annotations = annotations;
      }

      if (Object.keys(projection).length > 1) {
        // More than just objectId
        return [...baseQuery, { $project: projection }];
      }
    }

    return baseQuery;
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
    // Mode
    mode,

    // Schema and field options
    schema,
    fieldOptions,

    // Core state - provide both for backward compatibility
    filters: currentData,
    setFilters: setCurrentData,
    annotations,
    setAnnotations,
    collapsedBlocks,
    setCollapsedBlocks,
    hasInitialized,
    setHasInitialized,

    // Local filter state management
    localFilterData,
    setLocalFilterData,
    hasBeenModified,
    setHasBeenModified,

    // Projection fields state
    projectionFields,
    setProjectionFields,

    // Custom data
    customBlocks,
    setCustomBlocks,
    customVariables,
    setCustomVariables,
    customListVariables,
    setCustomListVariables,

    // Local filters updater (for filter mode)
    localFiltersUpdater,
    setLocalFiltersUpdater,

    // Spread hook functionalities
    ...dataManipulation,
    ...factories,
    ...dialogs,

    // MongoDB aggregation functions
    generateMongoQuery,
    getFormattedMongoQuery,
    hasValidQuery,
  };

  return (
    <UnifiedBuilderContext.Provider value={value}>
      {children}
    </UnifiedBuilderContext.Provider>
  );
};

// props validation
UnifiedBuilderProvider.propTypes = {
  children: PropTypes.node.isRequired,
  mode: PropTypes.oneOf(["filter", "annotation"]),
};
