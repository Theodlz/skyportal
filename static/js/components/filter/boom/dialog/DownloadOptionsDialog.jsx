import React from "react";
import PropTypes from "prop-types";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  IconButton,
  Alert,
  CircularProgress,
} from "@mui/material";
import {
  Close as CloseIcon,
  Download as DownloadIcon,
  DataObject as DataObjectIcon,
} from "@mui/icons-material";

/**
 * Dialog that lets the user choose between two download modes:
 *  - "Query results"  – the current pipeline output (only fields projected by the query)
 *  - "Full alerts"    – the same matched documents but with *all* alert fields,
 *                       obtained by running the pipeline without its final $project stage.
 *
 * The full-alerts download may be slow and is likely to time out for large result
 * sets; we surface a clear warning and handle errors gracefully.
 */
const DownloadOptionsDialog = ({
  open,
  onClose,
  onDownloadQuery,
  onDownloadFull,
  isDownloadingQuery,
  isDownloadingFull,
  downloadProgress,
  totalDocuments,
}) => {
  const anyDownloading = isDownloadingQuery || isDownloadingFull;

  return (
    <Dialog
      open={open}
      onClose={anyDownloading ? undefined : onClose}
      maxWidth="sm"
      fullWidth
    >
      <DialogTitle>
        <Box
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <Typography variant="h6">Download Results</Typography>
          <IconButton onClick={onClose} size="small" disabled={anyDownloading}>
            <CloseIcon />
          </IconButton>
        </Box>
      </DialogTitle>

      <DialogContent dividers>
        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Choose what to include in the downloaded JSON file.
        </Typography>

        {/* ── Option 1 – Query results ───────────────────────────────────── */}
        <Box
          sx={{
            border: "1px solid",
            borderColor: "divider",
            borderRadius: 1,
            p: 2,
            mb: 2,
          }}
        >
          <Box sx={{ display: "flex", alignItems: "flex-start", gap: 1.5 }}>
            <DownloadIcon color="primary" sx={{ mt: 0.25 }} />
            <Box sx={{ flex: 1 }}>
              <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                Query results
              </Typography>
              <Typography
                variant="body2"
                color="text.secondary"
                sx={{ mb: 1.5 }}
              >
                Download only the fields used in the query (not the full
                alerts). Faster, smaller file.
              </Typography>
              <Button
                variant="contained"
                size="small"
                startIcon={
                  isDownloadingQuery ? (
                    <CircularProgress size={14} color="inherit" />
                  ) : (
                    <DownloadIcon />
                  )
                }
                onClick={onDownloadQuery}
                disabled={anyDownloading}
              >
                {isDownloadingQuery
                  ? downloadProgress > 0
                    ? `Downloading… ${downloadProgress} docs`
                    : "Preparing…"
                  : "Download query results"}
              </Button>
            </Box>
          </Box>
        </Box>

        {/* ── Option 2 – Full alerts ─────────────────────────────────────── */}
        <Box
          sx={{
            border: "1px solid",
            borderColor: "divider",
            borderRadius: 1,
            p: 2,
          }}
        >
          <Box sx={{ display: "flex", alignItems: "flex-start", gap: 1.5 }}>
            <DataObjectIcon color="primary" sx={{ mt: 0.25 }} />
            <Box sx={{ flex: 1 }}>
              <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
                Full alerts
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                Download complete alert documents (all fields) for the same
                matched documents.
              </Typography>

              <Alert severity="warning" sx={{ mb: 1.5, py: 0.5 }}>
                <Typography variant="caption">
                  Full-alert documents are much larger. For{" "}
                  {totalDocuments > 0
                    ? `${totalDocuments} document${
                        totalDocuments !== 1 ? "s" : ""
                      }`
                    : "large result sets"}{" "}
                  this may be slow or time out. If it does, try a shorter date
                  range or a more restrictive query.
                </Typography>
              </Alert>

              <Button
                variant="contained"
                size="small"
                color="primary"
                startIcon={
                  isDownloadingFull ? (
                    <CircularProgress size={14} color="inherit" />
                  ) : (
                    <DataObjectIcon />
                  )
                }
                onClick={onDownloadFull}
                disabled={anyDownloading}
              >
                {isDownloadingFull
                  ? downloadProgress > 0
                    ? `Downloading… ${downloadProgress} docs`
                    : "Preparing…"
                  : "Download full alerts"}
              </Button>
            </Box>
          </Box>
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, py: 2 }}>
        <Button onClick={onClose} color="inherit" disabled={anyDownloading}>
          Cancel
        </Button>
      </DialogActions>
    </Dialog>
  );
};

DownloadOptionsDialog.propTypes = {
  open: PropTypes.bool.isRequired,
  onClose: PropTypes.func.isRequired,
  onDownloadQuery: PropTypes.func.isRequired,
  onDownloadFull: PropTypes.func.isRequired,
  isDownloadingQuery: PropTypes.bool.isRequired,
  isDownloadingFull: PropTypes.bool.isRequired,
  downloadProgress: PropTypes.number.isRequired,
  totalDocuments: PropTypes.number,
};

DownloadOptionsDialog.defaultProps = {
  totalDocuments: 0,
};

export default DownloadOptionsDialog;
