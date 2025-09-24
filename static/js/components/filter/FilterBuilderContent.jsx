import React, { useMemo, useState, useEffect } from "react";
import PropTypes from "prop-types";
import { useDispatch, useSelector } from "react-redux";
import { Button, Box, Typography } from "@mui/material";
import {
  Code as CodeIcon,
  Note as NoteIcon,
  Save as SaveIcon,
} from "@mui/icons-material";
import { useNavigate } from "react-router-dom";
import { useFilterBuilder } from "../../hooks/useContexts";
import { flattenFieldOptions } from "../../constants/filterConstants";
import AddVariableDialog from "./dialog/AddVariableDialog";
import BlockComponent from "./block/BlockComponent";
import AddListConditionDialog from "./dialog/AddListConditionDialog";
import SaveBlockDialogMenu from "./block/SaveBlockDialogMenu";
import MongoQueryDialog from "./dialog/MongoQueryDialog";
import { filterBuilderStyles } from "../../styles/componentStyles";
import { showNotification } from "baselayer/components/Notifications";

import { updateGroupFilter } from "../../ducks/boom_filter";
import { fetchSchema } from "../../ducks/boom_filter_modules";

const FilterBuilderContent = ({
  onToggleAnnotationBuilder,
  filter,
  setInlineNewVersion,
  setShowAnnotationBuilder,
}) => {
  const {
    setMongoDialog,
    hasValidQuery,
    collapsedBlocks,
    generateMongoQuery,
    setFilters,
    setLocalFiltersUpdater,
    // Use context state for local filter management
    localFilterData,
    setLocalFilterData,
    hasBeenModified,
    setHasBeenModified,
  } = useFilterBuilder();
  const navigate = useNavigate();
  const dispatch = useDispatch();
  const filter_stream = useSelector(
    (state) => state.filter_v.stream.name.split(" ")[0],
  );
  const store_schema = useSelector((state) => state.filter_modules?.schema);

  const [schema, setSchema] = useState(null);
  const [fieldOptions, setFieldOptions] = useState([]);

  // Initialize local filter data when filter prop changes
  useEffect(() => {
    // Don't override if user has already made modifications
    if (hasBeenModified) {
      console.log("Skipping filter initialization - user has modifications");
      return;
    }

    // First, check if we have filter data in the expected structure
    if (filter && filter.filters && filter.active_fid) {
      // This seems to be the original working structure
      const activeFilters = filter.filters.filter(
        (version) => version.fid === filter.active_fid,
      );

      if (
        activeFilters.length > 0 &&
        activeFilters[0].version &&
        activeFilters[0].version[0]
      ) {
        // Convert the original structure to editable format
        // Extract the actual filter blocks from version[0]
        const editableData = activeFilters.map(
          (filterVersion) => filterVersion.version[0],
        );

        setLocalFilterData(editableData);
        if (setFilters) {
          setFilters(editableData);
        }
        return;
      }
    }

    // Fallback: try the pipeline structure
    if (filter && filter.fv && filter.active_fid) {
      const activeVersion = filter.fv.find(
        (version) => version.fid === filter.active_fid,
      );

      if (activeVersion && activeVersion.pipeline) {
        try {
          const pipelineData = JSON.parse(activeVersion.pipeline);
          setLocalFilterData(pipelineData);
          if (setFilters && pipelineData) {
            setFilters(pipelineData);
          }
        } catch (error) {
          console.error("Error parsing pipeline data:", error);
          const emptyFilter = [
            {
              id: "root-block",
              category: "block",
              operator: "and",
              children: [],
            },
          ];
          setLocalFilterData(emptyFilter);
          if (setFilters) {
            setFilters(emptyFilter);
          }
        }
      } else {
        const emptyFilter = [
          {
            id: "root-block",
            category: "block",
            operator: "and",
            children: [],
          },
        ];
        setLocalFilterData(emptyFilter);
        if (setFilters) {
          setFilters(emptyFilter);
        }
      }
    } else if (!localFilterData) {
      const emptyFilter = [
        {
          id: "root-block",
          category: "block",
          operator: "and",
          children: [],
        },
      ];
      setLocalFilterData(emptyFilter);
      if (setFilters) {
        setFilters(emptyFilter);
      }
    }
  }, [filter, setFilters, hasBeenModified]);

  // Update context filters when local filter data changes
  useEffect(() => {
    if (localFilterData && setFilters) {
      setFilters(localFilterData);
    }
  }, [localFilterData, setFilters]);

  useEffect(() => {
    dispatch(fetchSchema({ name: filter_stream, elements: "schema" }));
  }, [filter_stream]);

  useEffect(() => {
    if (store_schema) {
      setSchema(store_schema);
      setFieldOptions(flattenFieldOptions(store_schema));
    }
  }, [store_schema]);

  // Callback to handle filter updates from child components
  const handleFilterUpdate = (updatedFilters) => {
    setHasBeenModified(true); // Mark as modified to prevent useEffect override
    setLocalFilterData(updatedFilters);
    // Also update the context immediately for MongoDB generation
    if (setFilters) {
      setFilters(updatedFilters);
    }
  };

  // Set the local filters updater in the context so dialogs can access it
  useEffect(() => {
    if (setLocalFiltersUpdater) {
      setLocalFiltersUpdater(() => handleFilterUpdate);
    }
  }, [setLocalFiltersUpdater]);

  // Use local filter data or fallback to context filters
  const { filters: contextFilters } = useFilterBuilder();
  const filtersToRender = localFilterData || contextFilters;

  // Find the most nested non-collapsed block to make its header sticky
  // Use useMemo to ensure this recalculates when filters or collapsedBlocks change
  const getMostNestedNonCollapsedBlock = useMemo(() => {
    if (!filtersToRender || filtersToRender.length === 0)
      return { blockId: null, path: [] };

    // Recursively find the deepest non-collapsed block
    const findDeepest = (blocks, path = [], depth = 0) => {
      let deepestBlock = { blockId: null, path: [], depth: -1 };

      for (let i = 0; i < blocks.length; i++) {
        const block = blocks[i];
        if (!block || !block.id || block.category !== "block") continue;

        const currentPath = [...path, i];
        // Root blocks (depth 0) are never collapsed, only nested blocks can be collapsed
        const isCollapsed = depth > 0 && collapsedBlocks?.[block.id] === true;

        if (!isCollapsed) {
          // This block is not collapsed, it's a candidate for sticky header
          if (depth >= deepestBlock.depth) {
            deepestBlock = { blockId: block.id, path: currentPath, depth };
          }

          // If this block has children blocks, recursively search them
          if (block.children && block.children.length > 0) {
            const childBlocks = block.children.filter(
              (child) => child?.category === "block",
            );
            if (childBlocks.length > 0) {
              const deepestChild = findDeepest(
                childBlocks,
                currentPath,
                depth + 1,
              );
              // Only update if we found a deeper block
              if (
                deepestChild.blockId &&
                deepestChild.depth > deepestBlock.depth
              ) {
                deepestBlock = deepestChild;
              }
            }
          }
        }
      }

      return deepestBlock;
    };

    const result = findDeepest(filtersToRender);
    return result;
  }, [filtersToRender, collapsedBlocks]);

  const handleShowMongoQuery = () => {
    setMongoDialog({ open: true });
  };

  const handleSaveFilter = async () => {
    const mongoQuery = generateMongoQuery();
    if (!mongoQuery || (Array.isArray(mongoQuery) && mongoQuery.length === 0)) {
      dispatch(showNotification("No valid MongoDB query to save", "error"));
      return;
    }

    try {
      // Use the current local filter data (which includes user modifications)
      const currentFilters =
        localFilterData || contextFilters || filtersToRender;
      const result = await dispatch(
        updateGroupFilter(filter.id, mongoQuery, currentFilters),
      );
      dispatch(showNotification("Filter saved to boom database!"));
      if (result.status === "success") {
        setInlineNewVersion(false);
        setShowAnnotationBuilder(false);
      }
    } catch (err) {
      console.error("Error saving filter:", err);
      const errorMessage =
        err.message ||
        "Failed to save filter to boom database. Please try again.";
      dispatch(showNotification(errorMessage, "error"));
    }
  };

  const handleAddAnnotations = () => {
    if (onToggleAnnotationBuilder) {
      onToggleAnnotationBuilder();
    } else {
      navigate("/annotations");
    }
  };

  return (
    <Box
      sx={{
        display: "flex",
        flexDirection: "column",
        gap: 2,
        ...filterBuilderStyles.container,
        // Ensure this container allows sticky positioning
        position: "relative",
        height: "100%",
      }}
    >
      {/* Header with buttons */}
      <Box
        sx={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          mb: 2,
        }}
      >
        <Typography variant="h2" sx={{ color: "text.primary" }}>
          Filter Builder
        </Typography>
        <Box sx={{ display: "flex", gap: 2 }}>
          <Button
            variant="contained"
            startIcon={<SaveIcon />}
            onClick={handleSaveFilter}
            disabled={!hasValidQuery()}
            sx={{
              backgroundColor: hasValidQuery() ? "primary.main" : undefined,
              "&:hover": {
                backgroundColor: hasValidQuery() ? "primary.dark" : undefined,
              },
            }}
          >
            Save
          </Button>
          <Button
            variant="outlined"
            startIcon={<NoteIcon />}
            onClick={handleAddAnnotations}
            sx={{
              "&:hover": {
                backgroundColor: "secondary.50",
                borderColor: "secondary.main",
              },
            }}
          >
            Add Annotations
          </Button>
          <Button
            variant="outlined"
            startIcon={<CodeIcon />}
            onClick={handleShowMongoQuery}
            disabled={!hasValidQuery()}
            sx={{
              borderColor: hasValidQuery() ? "primary.main" : undefined,
              color: hasValidQuery() ? "primary.main" : undefined,
              "&:hover": {
                borderColor: hasValidQuery() ? "primary.dark" : undefined,
                backgroundColor: hasValidQuery() ? "primary.50" : undefined,
              },
            }}
          >
            Preview MongoDB Query
          </Button>
        </Box>
      </Box>

      {/* Filter Blocks */}
      {filtersToRender && filtersToRender.length > 0 ? (
        filtersToRender.map((block, index) => {
          return (
            <BlockComponent
              key={block.id || index}
              block={block}
              parentBlockId={null}
              isRoot={index === 0}
              fieldOptionsList={fieldOptions}
              stickyBlockId={getMostNestedNonCollapsedBlock.blockId}
              localFilters={filtersToRender}
              setLocalFilters={handleFilterUpdate}
            />
          );
        })
      ) : (
        <Typography variant="body2" color="text.secondary">
          No filter blocks to display. Add conditions to get started.
        </Typography>
      )}

      {/* Dialogs */}
      <AddVariableDialog />
      <AddListConditionDialog />
      <SaveBlockDialogMenu />
      <MongoQueryDialog />
    </Box>
  );
};

FilterBuilderContent.propTypes = {
  onToggleAnnotationBuilder: PropTypes.func.isRequired,
  filter: PropTypes.shape({
    id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    name: PropTypes.string,
    stream_id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    group_id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    filt: PropTypes.object,
    version: PropTypes.arrayOf(PropTypes.object),
    active_fid: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    fv: PropTypes.arrayOf(
      PropTypes.shape({
        id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
      }),
    ),
    filters: PropTypes.arrayOf(
      PropTypes.shape({
        id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
      }),
    ),
  }),
  setInlineNewVersion: PropTypes.func.isRequired,
  setShowAnnotationBuilder: PropTypes.func.isRequired,
};

export default FilterBuilderContent;
