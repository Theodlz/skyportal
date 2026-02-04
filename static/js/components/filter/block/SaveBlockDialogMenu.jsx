import React, { useState } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  TextField,
  DialogActions,
  Button,
} from "@mui/material";
import { fetchElement, postElement } from "../../../ducks/boom_filter_modules";
import { useCurrentBuilder } from "../../../hooks/useContexts";
import { useDispatch } from "react-redux";

const SaveBlockDialogMenu = () => {
  const {
    saveDialog,
    setSaveDialog,
    saveName,
    setSaveName,
    saveError,
    setSaveError,
    setCustomBlocks,
    setCollapsedBlocks,
    setFilters,
    localFiltersUpdater,
    customVariables,
    customListVariables,
  } = useCurrentBuilder();

  const dispatch = useDispatch();

  const handleSaveDialogConfirm = async () => {
    if (!saveName || !saveName.trim()) {
      setSaveError("Name is required.");
      return;
    }

    const nameValue = saveName.trim();

    // Check if an arithmetic variable with the same name already exists
    if (customVariables?.some((v) => v.name === nameValue)) {
      setSaveError(
        "A variable with this name already exists. Please choose another.",
      );
      return;
    }

    // Check if a list variable with the same name already exists
    if (customListVariables?.some((lv) => lv.name === nameValue)) {
      setSaveError(
        "A variable with this name already exists. Please choose another.",
      );
      return;
    }

    // Check for duplicate name
    const notAvailable = await dispatch(
      fetchElement({ name: nameValue, elements: "blocks" }),
    );
    if (notAvailable.data.blocks) {
      setSaveError("Name already exists. Please choose another.");
      return;
    }
    // Save customBlocks
    const saved = await dispatch(
      postElement({
        name: nameValue,
        data: { block: saveDialog.block },
        elements: "blocks",
      }),
    );
    if (saved) {
      const blockId = saveDialog.block.id;

      // Update filters using the local updater if available (for FilterBuilderContent)
      // This ensures both localFilterData and context filters are updated
      const updateFilters = localFiltersUpdater || setFilters;

      updateFilters((prevFilters) => {
        const replaceBlock = (block) => {
          if (block.id !== blockId) {
            return {
              ...block,
              children:
                block.children?.map((child) =>
                  child.category === "block" ? replaceBlock(child) : child,
                ) || [],
            };
          }
          // Create new block object with customBlockName and isTrue
          // Force new object reference to trigger re-render
          const updatedBlock = {
            ...block,
            customBlockName: nameValue,
            isTrue: true, // Explicitly set to true for root custom blocks
          };
          return updatedBlock;
        };
        return prevFilters.map(replaceBlock);
      });

      // Then add to customBlocks registry
      setCustomBlocks((prev) => {
        const newName = `Custom.${nameValue}`;
        return [
          ...prev.filter(
            (cb) => cb.block?.id !== blockId && cb.name !== newName,
          ),
          { name: newName, block: saveDialog.block },
        ];
      });

      // Finally collapse the block
      setCollapsedBlocks((prev) => ({
        ...prev,
        [blockId]: true,
      }));
      setSaveDialog({ open: false, block: null });
      setSaveName("");
      setSaveError("");
    } else {
      setSaveError("Failed to save block.");
    }
  };

  return (
    <Dialog
      open={saveDialog.open}
      onClose={() => {
        setSaveDialog({ open: false, block: null });
        setSaveName("");
        setSaveError("");
      }}
    >
      <DialogTitle>Save Block</DialogTitle>
      <DialogContent>
        <TextField
          autoFocus
          margin="dense"
          label="Block Name"
          fullWidth
          value={saveName}
          onChange={(e) => {
            setSaveName(e.target.value);
            setSaveError("");
          }}
          error={!!saveError}
          helperText={saveError || "Enter a unique name for this custom block"}
          sx={{ mt: 1 }}
        />
      </DialogContent>
      <DialogActions>
        <Button
          onClick={() => {
            setSaveDialog({ open: false, block: null });
            setSaveName("");
            setSaveError("");
          }}
        >
          Cancel
        </Button>
        <Button onClick={handleSaveDialogConfirm} variant="contained">
          Save
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default SaveBlockDialogMenu;
