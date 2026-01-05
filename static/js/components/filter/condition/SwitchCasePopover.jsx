import React from "react";
import PropTypes from "prop-types";
import { Popover, Box, Typography, Paper, Divider } from "@mui/material";
import BlockComponent from "../block/BlockComponent";

const SwitchCasePopover = ({
  switchPopoverAnchor,
  setSwitchPopoverAnchor,
  customSwitchCases,
  fieldOptionsList,
}) => {
  const isOpen = Boolean(switchPopoverAnchor);

  const handleClose = () => {
    const anchorElement = switchPopoverAnchor;
    setSwitchPopoverAnchor(null);
    window.currentSwitchCase = null;

    if (anchorElement) {
      setTimeout(() => {
        if (anchorElement && typeof anchorElement.focus === "function") {
          try {
            anchorElement.focus();
          } catch {
            document.body.focus();
          }
        }
      }, 100);
    }
  };

  const renderSwitchCaseContent = () => {
    const switchCase = window.currentSwitchCase;
    if (!switchCase) {
      return null;
    }

    const { name, switchCondition } = switchCase;
    const cases = switchCondition?.value?.cases || [];
    const defaultValue = switchCondition?.value?.default || "";

    return (
      <Box sx={{ 
        p: 3, 
        width: 1000, 
        maxWidth: "95vw", 
        background: "linear-gradient(135deg, #f3f4f6 0%, #e5e7eb 50%, #d1d5db 100%)"
      }}>
        <Box
          sx={{
            display: "flex",
            alignItems: "center",
            gap: 2,
            mb: 3,
            pb: 2,
            borderBottom: "2px solid",
            borderColor: "#6b7280",
          }}
        >
          <Box
            sx={{
              width: 4,
              height: 32,
              backgroundColor: "#6b7280",
              borderRadius: 1,
            }}
          />
          <Typography variant="h6" sx={{ fontWeight: 600, color: "#374151" }}>
            Switch Case: {name}
          </Typography>
        </Box>

        <Box sx={{ maxHeight: "70vh", overflowY: "auto", overflowX: "hidden", pr: 1 }}>
          {cases.map((caseItem, index) => (
            <Paper
              key={index}
              sx={{
                mb: 3,
                p: 2.5,
                border: "1px solid",
                borderColor: "#d1d5db",
                backgroundColor: "white",
                borderRadius: 2,
                boxShadow: "0 2px 8px 0 rgba(107,114,128,0.12)",
                transition: "all 0.2s",
                "&:hover": {
                  borderColor: "#6b7280",
                  boxShadow: "0 4px 12px 0 rgba(107,114,128,0.18)",
                },
              }}
            >
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1,
                  mb: 2,
                }}
              >
                <Box
                  sx={{
                    width: 24,
                    height: 24,
                    borderRadius: "50%",
                    backgroundColor: "#6b7280",
                    color: "white",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    fontSize: 12,
                    fontWeight: 700,
                  }}
                >
                  {index + 1}
                </Box>
                <Typography
                  variant="subtitle2"
                  sx={{ fontWeight: 600, color: "#374151" }}
                >
                  CASE {index + 1}
                </Typography>
              </Box>

              {/* Display the block conditions (read-only) */}
              {caseItem.block && caseItem.block.children && caseItem.block.children.length > 0 ? (
                <Box sx={{ mb: 2, width: "100%", minWidth: 0 }}>
                  <BlockComponent
                    block={caseItem.block}
                    parentBlockId={null}
                    isRoot={true}
                    fieldOptionsList={fieldOptionsList}
                    localFilters={[caseItem.block]}
                    setLocalFilters={() => {}} // Read-only
                    disableSwitchOption={true}
                  />
                </Box>
              ) : (
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2, pl: 1, fontStyle: "italic" }}>
                  No conditions
                </Typography>
              )}

              {/* Display THEN value */}
              <Box
                sx={{
                  display: "flex",
                  alignItems: "center",
                  gap: 1,
                  mt: 2,
                  pl: 1,
                  pt: 2,
                  borderTop: "1px dashed",
                  borderColor: "#d1d5db",
                }}
              >
                <Typography variant="caption" sx={{ fontWeight: 700, minWidth: 50, color: "#374151" }}>
                  THEN:
                </Typography>
                <Typography
                  variant="body2"
                  sx={{
                    backgroundColor: "#f9fafb",
                    color: "#1f2937",
                    px: 1.5,
                    py: 0.75,
                    borderRadius: 1,
                    fontFamily: "monospace",
                    flex: 1,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    border: "1px solid #e5e7eb",
                  }}
                >
                  {caseItem.then || "(empty)"}
                </Typography>
              </Box>
            </Paper>
          ))}

          {/* Display DEFAULT value */}
          <Paper
            sx={{
              p: 2.5,
              border: "2px solid",
              borderColor: "#6b7280",
              backgroundColor: "white",
              borderRadius: 2,
              boxShadow: "0 2px 8px 0 rgba(107,114,128,0.15)",
            }}
          >
            <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 700, color: "#374151", minWidth: 80 }}>
                DEFAULT:
              </Typography>
              <Typography
                variant="body2"
                sx={{
                  backgroundColor: "#f9fafb",
                  color: "#1f2937",
                  px: 1.5,
                  py: 0.75,
                  borderRadius: 1,
                  fontFamily: "monospace",
                  flex: 1,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  border: "1px solid #e5e7eb",
                }}
              >
                {defaultValue || "(empty)"}
              </Typography>
            </Box>
          </Paper>
        </Box>
      </Box>
    );
  };

  return (
    <Popover
      open={isOpen}
      anchorEl={switchPopoverAnchor}
      onClose={handleClose}
      anchorOrigin={{
        vertical: "bottom",
        horizontal: "left",
      }}
      transformOrigin={{
        vertical: "top",
        horizontal: "left",
      }}
      slotProps={{
        paper: {
          sx: {
            maxHeight: "85vh",
            overflowY: "visible",
            overflowX: "hidden",
            width: "auto",
            maxWidth: "95vw",
          },
        },
      }}
    >
      {renderSwitchCaseContent()}
    </Popover>
  );
};

SwitchCasePopover.propTypes = {
  switchPopoverAnchor: PropTypes.object,
  setSwitchPopoverAnchor: PropTypes.func.isRequired,
  customSwitchCases: PropTypes.array.isRequired,
  fieldOptionsList: PropTypes.array.isRequired,
};

export default SwitchCasePopover;
