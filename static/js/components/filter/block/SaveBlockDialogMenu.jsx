import React, { useEffect } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  TextField,
  DialogActions,
  Button,
} from "@mui/material";
import {
  saveBlock,
  checkBlockNameAvailable,
} from "../../../services/filterApi";
import { fetchElement, postElement } from "../../../ducks/boom_filter_modules";
import { useCurrentBuilder } from "../../../hooks/useContexts";
import { useDispatch, useSelector } from "react-redux";

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
  } = useCurrentBuilder();

  const dispatch = useDispatch();

  const handleSaveDialogConfirm = async () => {
    if (!saveName.trim()) {
      setSaveError("Name is required.");
      return;
    }
    // Check for duplicate name
    const notAvailable = await dispatch(
      fetchElement({ name: saveName.trim(), elements: "blocks" }),
    );
    if (notAvailable.data.blocks) {
      setSaveError("Name already exists. Please choose another.");
      return;
    }
    // Save customBlocks
    const saved = await dispatch(
      postElement({
        name: saveName.trim(),
        data: { block: saveDialog.block },
        elements: "blocks",
      }),
    );
    if (saved) {
      setCustomBlocks((prev) => {
        const newId = saveDialog.block.id;
        const newName = `Custom.${saveName.trim()}`;
        return [
          ...prev.filter((cb) => cb.block?.id !== newId && cb.name !== newName),
          { name: newName, block: saveDialog.block },
        ];
      });
      // Collapse the block in the main filter builder (collapsed by default)
      setCollapsedBlocks((prev) => ({
        ...prev,
        [saveDialog.block.id]: true,
      }));
      setFilters((prevFilters) => {
        const replaceBlock = (block) => {
          if (block.id !== saveDialog.block.id) {
            return {
              ...block,
              children: block.children.map((child) =>
                child.category === "block" ? replaceBlock(child) : child,
              ),
            };
          }
          const newBlock = {
            ...saveDialog.block,
            customBlockName: saveName.trim(),
            isTrue: block.isTrue, // preserve boolean state if present
          };
          return { ...newBlock };
        };
        return prevFilters.map(replaceBlock);
      });
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
      onClose={() => setSaveDialog({ open: false, block: null })}
    >
      <DialogTitle>Save Block</DialogTitle>
      <DialogContent>
        <TextField
          autoFocus
          margin="dense"
          label="Name"
          fullWidth
          value={saveName}
          onChange={(e) => setSaveName(e.target.value)}
          error={!!saveError}
          helperText={saveError}
        />
      </DialogContent>
      <DialogActions>
        <Button onClick={() => setSaveDialog({ open: false, block: null })}>
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
