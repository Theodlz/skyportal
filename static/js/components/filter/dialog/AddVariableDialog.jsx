import React from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
} from "@mui/material";
import { saveVariable } from "../../../services/filterApi";
import EquationEditor from "equation-editor-react";
import { v4 as uuidv4 } from "uuid";
import { useCurrentBuilder } from "../../../hooks/useContexts";
import { postElement } from "../../../ducks/boom_filter_modules";
import { useDispatch } from "react-redux";

const AddVariableDialog = () => {
  const {
    specialConditionDialog,
    setSpecialConditionDialog,
    setCustomVariables,
    setFilters,
  } = useCurrentBuilder();

  const dispatch = useDispatch();

  const handleCloseSpecialCondition = () => {
    setSpecialConditionDialog({ open: false, blockId: null, equation: "" });
  };

  const addVariableDialogStyles = {
    container: {
      minWidth: 500,
      minHeight: 200,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
    },

    equation: {
      minHeight: 60,
      fontSize: 40,
    },
  };

  return (
    <Dialog
      open={specialConditionDialog.open}
      onClose={handleCloseSpecialCondition}
      maxWidth="md"
      fullWidth
      disableRestoreFocus={false}
      slotProps={{
        paper: {
          "aria-labelledby": "add-variable-dialog-title",
        },
        root: {
          "aria-hidden": false,
        },
      }}
    >
      <DialogTitle id="add-variable-dialog-title">
        Add Special Condition
      </DialogTitle>
      <Box sx={{ px: 3, pb: 1 }}>
        <Typography variant="body2" color="text.secondary">
          Modify the equation to fit the variable that you want to create
        </Typography>
      </Box>
      <DialogContent>
        {/* Equation Editor for special condition */}
        <div style={addVariableDialogStyles.container}>
          <EquationEditor
            value={
              specialConditionDialog.equation ||
              "yourVariableName = yourEquation"
            }
            onChange={(val) => {
              setSpecialConditionDialog((prev) => ({
                ...prev,
                equation: val,
              }));
            }}
            autoCommands="pi theta sqrt sum prod alpha beta gamma rho"
            autoOperatorNames="sin cos tan log ln exp"
            style={addVariableDialogStyles.equation}
          />
        </div>
      </DialogContent>
      <DialogActions>
        <Button onClick={handleCloseSpecialCondition}>Cancel</Button>
        <Button
          variant="contained"
          color="primary"
          onClick={() => {
            const eq = (specialConditionDialog.equation || "").trim();
            const eqParts = eq.split("=");
            if (
              eqParts.length !== 2 ||
              !eqParts[0].trim() ||
              !eqParts[1].trim()
            ) {
              alert(
                "Please enter a valid equation in the form: variable = expression",
              );
              return;
            }
            const variableName = eqParts[0].trim();
            dispatch(
              postElement({
                name: variableName,
                data: { variable: eq, type: "number" },
                elements: "variables",
              }),
            );
            setCustomVariables((prev) => {
              if (prev.some((v) => v.name === variableName)) return prev;
              return [
                ...prev,
                { name: variableName, type: "number", variable: eq },
              ];
            });
            // Add a new special condition to the block
            setFilters((prevFilters) => {
              // Recursively update the correct block by id
              const addConditionToBlock = (block) => {
                if (block.id === specialConditionDialog.blockId) {
                  return {
                    ...block,
                    children: [
                      ...block.children,
                      {
                        id: uuidv4(),
                        category: "condition",
                        type: "number",
                        field: variableName,
                        operator: "$eq",
                        value: "",
                        createdAt: Date.now(),
                      },
                    ],
                  };
                }
                if (block.children) {
                  return {
                    ...block,
                    children: block.children.map((child) =>
                      child.category === "block"
                        ? addConditionToBlock(child)
                        : child,
                    ),
                  };
                }
                return block;
              };
              return prevFilters.map(addConditionToBlock);
            });
            handleCloseSpecialCondition();
          }}
        >
          Add Variable
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default AddVariableDialog;
