import { useContext, useEffect, useCallback, useState } from "react";
import { useLocation } from "react-router-dom";
import { useDispatch, useSelector } from "react-redux";
import { v4 as uuidv4 } from "uuid";
import { FilterBuilderContext } from "../contexts/FilterBuilderContext";
import { AnnotationBuilderContext } from "../contexts/AnnotationBuilderContext";
import {
  fetchSavedBlocks,
  fetchSavedVariables,
  fetchSavedListVariables,
} from "../services/filterApi";
import { fetchAllElements } from "../ducks/boom_filter_modules";
import { useFilterBuilder } from "./useContexts";

export const useCurrentBuilder = () => {
  const location = useLocation();
  const annotationContext = useContext(AnnotationBuilderContext);
  const filterContext = useContext(FilterBuilderContext);

  // Determine which context to use based on the current route
  if (location.pathname === "/annotations") {
    if (annotationContext && filterContext) {
      // For annotation page, use annotation context for most things,
      // but use filter context for MongoDB query functionality
      return {
        ...annotationContext,
        // Override MongoDB-related functions to use filter context
        mongoDialog: filterContext.mongoDialog,
        setMongoDialog: filterContext.setMongoDialog,
        generateMongoQuery: filterContext.generateMongoQuery,
        getFormattedMongoQuery: filterContext.getFormattedMongoQuery,
        hasValidQuery: filterContext.hasValidQuery,
      };
    }
    throw new Error(
      "useCurrentBuilder: AnnotationBuilderProvider or FilterBuilderProvider not found on annotation page",
    );
  } else {
    // Default to filter context for all other routes (including '/')
    if (filterContext) {
      return filterContext;
    }
    throw new Error(
      "useCurrentBuilder: FilterBuilderProvider not found on filter page",
    );
  }
};

export const useFilterBuilderData = () => {
  const {
    filters,
    setFilters,
    setCustomBlocks,
    setCustomVariables,
    setCustomListVariables,
    createDefaultBlock,
    hasInitialized,
    setHasInitialized,
  } = useFilterBuilder();

  // Initialize filters if empty and not initialized yet
  useEffect(() => {
    if (filters.length === 0 && !hasInitialized) {
      setFilters([createDefaultBlock("And")]);
      setHasInitialized(true);
    }
  }, [
    filters.length,
    hasInitialized,
    createDefaultBlock,
    setFilters,
    setHasInitialized,
  ]);

  const dispatch = useDispatch();

  // Load saved data on mount
  useEffect(() => {
    const loadData = async () => {
      try {
        // Load all saved data in parallel
        const blocks = await dispatch(fetchAllElements({ elements: "blocks" }));
        const variables = await dispatch(
          fetchAllElements({ elements: "variables" }),
        );
        const listVariables = await dispatch(
          fetchAllElements({ elements: "listVariables" }),
        );

        setCustomBlocks(blocks.data.blocks);
        setCustomVariables(variables.data.variables);
        setCustomListVariables(listVariables.data.listVariables);
      } catch (error) {
        console.error("Error loading filter builder data:", error);
        // Set empty arrays as fallback
        setCustomBlocks([]);
        setCustomVariables([]);
        setCustomListVariables([]);
      }
    };

    loadData();
  }, [setCustomBlocks, setCustomVariables, setCustomListVariables, dispatch]);

  return {
    // This hook doesn't return anything directly, but loads data into context
    // Consumer components can use useFilterBuilder() to access the loaded data
  };
};

export const useFilterFactories = () => {
  // Factory functions
  const createDefaultCondition = useCallback(
    () => ({
      id: uuidv4(),
      category: "condition",
      field: null,
      operator: null,
      value: "",
      createdAt: Date.now(),
    }),
    [],
  );

  const createDefaultBlock = useCallback(
    (logic = "And") => ({
      id: uuidv4(),
      category: "block",
      logic,
      children: [createDefaultCondition()],
      createdAt: Date.now(),
    }),
    [createDefaultCondition],
  );

  const createCondition = useCallback(
    (overrides = {}) => ({
      id: uuidv4(),
      category: "condition",
      field: null,
      operator: null,
      value: "",
      createdAt: Date.now(),
      ...overrides,
    }),
    [],
  );

  const createBlock = useCallback(
    (logic = "And", overrides = {}) => ({
      id: uuidv4(),
      category: "block",
      logic,
      children: [],
      createdAt: Date.now(),
      ...overrides,
    }),
    [],
  );

  return {
    createDefaultCondition,
    createDefaultBlock,
    createCondition,
    createBlock,
  };
};

export const useFilterManipulation = (filters, setFilters) => {
  // Utility functions for filter manipulation
  const addConditionToBlock = useCallback(
    (blockId, condition) => {
      setFilters((prevFilters) => {
        const updateBlock = (block) => {
          if (block.id === blockId) {
            return {
              ...block,
              children: [...block.children, condition],
            };
          }
          if (block.children) {
            return {
              ...block,
              children: block.children.map((child) =>
                child.category === "block" ? updateBlock(child) : child,
              ),
            };
          }
          return block;
        };
        return prevFilters.map(updateBlock);
      });
    },
    [setFilters],
  );

  const removeConditionFromBlock = useCallback(
    (blockId, conditionId) => {
      setFilters((prevFilters) => {
        const updateBlock = (block) => {
          if (block.id === blockId) {
            return {
              ...block,
              children: block.children.filter(
                (child) => child.id !== conditionId,
              ),
            };
          }
          if (block.children) {
            return {
              ...block,
              children: block.children.map((child) =>
                child.category === "block" ? updateBlock(child) : child,
              ),
            };
          }
          return block;
        };
        return prevFilters.map(updateBlock);
      });
    },
    [setFilters],
  );

  const updateCondition = useCallback(
    (blockId, conditionId, updates) => {
      setFilters((prevFilters) => {
        const updateBlock = (block) => {
          if (block.id === blockId) {
            return {
              ...block,
              children: block.children.map((child) =>
                child.id === conditionId ? { ...child, ...updates } : child,
              ),
            };
          }
          if (block.children) {
            return {
              ...block,
              children: block.children.map((child) =>
                child.category === "block" ? updateBlock(child) : child,
              ),
            };
          }
          return block;
        };
        return prevFilters.map(updateBlock);
      });
    },
    [setFilters],
  );

  const updateBlockLogic = useCallback(
    (blockId, logic) => {
      setFilters((prevFilters) => {
        const updateBlock = (block) => {
          if (block.id === blockId) return { ...block, logic };
          return {
            ...block,
            children: block.children.map((child) =>
              child.category === "block" ? updateBlock(child) : child,
            ),
          };
        };
        return prevFilters.map(updateBlock);
      });
    },
    [setFilters],
  );

  const removeBlock = useCallback(
    (blockId, parentBlockId) => {
      if (parentBlockId === null) return;

      setFilters((prevFilters) => {
        const removeBlockFromTree = (block) => {
          if (block.id !== parentBlockId) {
            return {
              ...block,
              children: block.children.map((child) =>
                child.category === "block" ? removeBlockFromTree(child) : child,
              ),
            };
          }
          return {
            ...block,
            children: block.children.filter((child) => child.id !== blockId),
          };
        };
        return prevFilters.map(removeBlockFromTree);
      });
    },
    [setFilters],
  );

  return {
    addConditionToBlock,
    removeConditionFromBlock,
    updateCondition,
    updateBlockLogic,
    removeBlock,
  };
};

export const useHoverState = (conditionId, filters) => {
  const [hoveredItems, setHoveredItems] = useState(new Set());

  const handleMouseEnter = (itemId) => {
    setHoveredItems((prev) => new Set([...prev, itemId]));
  };

  const handleMouseLeave = (itemId) => {
    setHoveredItems((prev) => {
      const newSet = new Set(prev);
      newSet.delete(itemId);
      return newSet;
    });
  };

  const getYoungestHoveredItem = () => {
    if (hoveredItems.size === 0) return null;

    const findAllItems = (blocks) => {
      let allItems = [];
      blocks?.forEach((block) => {
        const traverse = (item) => {
          allItems.push(item);
          if (item.category === "block" && item.children) {
            item.children.forEach(traverse);
          }
        };
        traverse(block);
      });
      return allItems;
    };

    const allItems = findAllItems(filters);
    const hoveredItemsArray = allItems.filter((item) =>
      hoveredItems.has(item.id),
    );

    if (hoveredItemsArray.length === 0) return null;

    return hoveredItemsArray.reduce((youngest, item) =>
      item.createdAt > youngest.createdAt ? item : youngest,
    );
  };

  const youngestHovered = getYoungestHoveredItem();
  const isYoungestHovered =
    youngestHovered && conditionId === youngestHovered.id;

  return {
    hoveredItems,
    isYoungestHovered,
    handleMouseEnter,
    handleMouseLeave,
  };
};
