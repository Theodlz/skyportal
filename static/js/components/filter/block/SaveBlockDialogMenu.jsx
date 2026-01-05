import React, { useEffect, useState } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  TextField,
  DialogActions,
  Button,
  Autocomplete,
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
    fieldOptions,
  } = useCurrentBuilder();

  const dispatch = useDispatch();

  // Track whether the selected name is a field reference
  const [isFieldReference, setIsFieldReference] = useState(false);

  const handleSaveDialogConfirm = async () => {
    if (!saveName || (typeof saveName === 'string' && !saveName.trim())) {
      setSaveError("Name is required.");
      return;
    }

    const nameValue = typeof saveName === 'object' && saveName.label 
      ? saveName.label 
      : saveName.trim ? saveName.trim() : saveName;

    // If it's a field reference, we don't save to database - just set it locally
    // It will be used in a $set stage later during query execution
    if (isFieldReference) {
      setCustomBlocks((prev) => {
        const newId = saveDialog.block.id;
        const newName = `Custom.${nameValue}`;
        return [
          ...prev.filter((cb) => cb.block?.id !== newId && cb.name !== newName),
          { 
            name: newName, 
            block: saveDialog.block,
            isFieldReference: true, // Flag to indicate this is a field reference
            fieldPath: nameValue, // Store the field path
          },
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
            customBlockName: nameValue,
            isFieldReference: true, // Mark that this uses a field reference
            isTrue: block.isTrue, // preserve boolean state if present
          };
          return { ...newBlock };
        };
        return prevFilters.map(replaceBlock);
      });
      setSaveDialog({ open: false, block: null });
      setSaveName("");
      setSaveError("");
      setIsFieldReference(false);
      return;
    }

    // For custom names (non-field references), save to database
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
      setCustomBlocks((prev) => {
        const newId = saveDialog.block.id;
        const newName = `Custom.${nameValue}`;
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
            customBlockName: nameValue,
            isTrue: block.isTrue, // preserve boolean state if present
          };
          return { ...newBlock };
        };
        return prevFilters.map(replaceBlock);
      });
      setSaveDialog({ open: false, block: null });
      setSaveName("");
      setSaveError("");
      setIsFieldReference(false);
    } else {
      setSaveError("Failed to save block.");
    }
  };

  return (
    <Dialog
      open={saveDialog.open}
      onClose={() => {
        setSaveDialog({ open: false, block: null });
        setIsFieldReference(false);
        setSaveName("");
        setSaveError("");
      }}
    >
      <DialogTitle>Save Block</DialogTitle>
      <DialogContent>
        <Autocomplete
          freeSolo
          autoFocus
          options={fieldOptions || []}
          getOptionLabel={(option) => {
            if (typeof option === 'string') return option;
            return option.label || '';
          }}
          value={saveName}
          onChange={(event, newValue) => {
            if (typeof newValue === 'object' && newValue?.label) {
              // User selected a field from the list
              setSaveName(newValue.label);
              setIsFieldReference(true);
            } else {
              // User typed a custom name
              setSaveName(newValue || '');
              setIsFieldReference(false);
            }
            setSaveError("");
          }}
          onInputChange={(event, newInputValue) => {
            // Handle manual typing
            setSaveName(newInputValue);
            setIsFieldReference(false);
            setSaveError("");
          }}
          renderInput={(params) => (
            <TextField
              {...params}
              margin="dense"
              label="Name (select a field or type custom name)"
              fullWidth
              error={!!saveError}
              helperText={saveError || (isFieldReference ? "Field reference - will use $set stage" : "Custom name - will be saved to database")}
            />
          )}
          sx={{ mt: 1 }}
        />
      </DialogContent>
      <DialogActions>
        <Button onClick={() => {
          setSaveDialog({ open: false, block: null });
          setIsFieldReference(false);
          setSaveName("");
          setSaveError("");
        }}>
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
