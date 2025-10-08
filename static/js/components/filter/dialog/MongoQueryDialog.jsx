import React, { useState, useEffect } from "react";
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  IconButton,
  Snackbar,
  Alert,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  CircularProgress,
  Divider,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Collapse,
  Tooltip,
  Tabs,
  Tab,
} from "@mui/material";
import {
  ContentCopy as CopyIcon,
  Close as CloseIcon,
  PlayArrow as RunIcon,
  ExpandMore as ExpandMoreIcon,
  ExpandLess as ExpandLessIcon,
} from "@mui/icons-material";
import { useCurrentBuilder } from "../../../hooks/useContexts";
import { mongoQueryService } from "../../../services/mongoQueryService";
import ReactJson from "react-json-view";
import { useDispatch, useSelector } from "react-redux";
import { runBoomFilter } from "../../../ducks/boom_run_filter";

// Helper function to get stage descriptions
const getStageDescription = (stageName) => {
  const descriptions = {
    $match:
      "Filters documents to pass only those that match the specified condition(s)",
    $project:
      "Reshapes documents by including, excluding, or adding new fields",
    $lookup: "Performs a left outer join to documents from another collection",
    $unwind:
      "Deconstructs an array field to output a document for each element",
    $group:
      "Groups documents by a specified identifier and applies aggregation functions",
    $sort: "Sorts documents by specified field(s)",
    $limit: "Limits the number of documents passed to the next stage",
    $skip: "Skips a specified number of documents",
    $addFields: "Adds new fields to documents",
    $replaceRoot: "Replaces the input document with the specified document",
    $facet: "Processes multiple aggregation pipelines within a single stage",
    $bucket: "Categorizes documents into groups based on specified boundaries",
    $count: "Returns a count of the number of documents at this stage",
    $out: "Writes the resulting documents to a collection",
    $merge:
      "Writes the results of the aggregation pipeline to a specified collection",
    $filter: "Filters array elements based on specified criteria",
    $map: "Applies an expression to each element in an array",
    $reduce:
      "Applies an expression to each element in an array and combines them",
  };
  return descriptions[stageName] || "MongoDB aggregation stage";
};

const MongoQueryDialog = () => {
  const {
    mongoDialog = { open: false },
    setMongoDialog,
    generateMongoQuery,
    getFormattedMongoQuery,
    hasValidQuery,
  } = useCurrentBuilder();
  const filter_stream = useSelector(
    (state) => state.filter_v.stream?.name?.split(" ")[0],
  );
  const dispatch = useDispatch();
  const results = useSelector((state) => state.query_result);

  const [copySuccess, setCopySuccess] = useState(false);
  //set default to "ZTF_alerts" collection if filter_stream is "ZTF" or "LSST_alerts" if filter_stream is "LSST"
  const [selectedCollection, setSelectedCollection] = useState(
    filter_stream === "ZTF"
      ? "ZTF_alerts"
      : filter_stream === "LSST"
        ? "LSST_alerts"
        : "",
  );
  const [availableCollections, setAvailableCollections] = useState([]);
  const [isRunning, setIsRunning] = useState(false);
  const [queryError, setQueryError] = useState(null);
  const [showResults, setShowResults] = useState(false);
  const [showPipeline, setShowPipeline] = useState(true);
  const [pipelineView, setPipelineView] = useState("complete"); // 'stages' or 'complete'
  const [connectionStatus, setConnectionStatus] = useState("unknown"); // 'connected', 'disconnected', 'unknown'
  const [expandedCells, setExpandedCells] = useState(new Set()); // Track expanded JSON cells
  const [expandedStages, setExpandedStages] = useState(new Set()); // Track expanded pipeline stages

  // Handle JSON expansion/collapse
  const handleJsonToggle = (isExpanded, level, rowIndex, cellIndex) => {
    const cellKey = `${rowIndex}-${cellIndex}`;
    setExpandedCells((prev) => {
      const newSet = new Set(prev);
      if (isExpanded) {
        newSet.add(cellKey);
      } else {
        newSet.delete(cellKey);
      }
      return newSet;
    });
  };

  // Handle stage expansion/collapse
  const handleStageToggle = (stageIndex) => {
    setExpandedStages((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(stageIndex)) {
        newSet.delete(stageIndex);
      } else {
        newSet.add(stageIndex);
      }
      return newSet;
    });
  };

  // Load available collections when dialog opens
  useEffect(() => {
    if (mongoDialog?.open) {
      loadCollections();
    }
  }, [mongoDialog?.open]);

  useEffect(() => {
    setShowResults(true);
  }, [results]);

  const loadCollections = async () => {
    try {
      setConnectionStatus("connected");
    } catch (error) {
      console.error("Failed to load collections:", error);
      setAvailableCollections([{ name: "", type: "collection" }]);
      setConnectionStatus("disconnected");
    }
  };

  const handleClose = () => {
    setMongoDialog({ open: false });
    // Reset query results when closing
    setQueryError(null);
    setShowResults(false);
    setShowPipeline(true); // Keep pipeline expanded by default
    setPipelineView("complete"); // Reset to default view
    setExpandedCells(new Set()); // Reset expanded cells
    setExpandedStages(new Set()); // Reset expanded stages
  };

  const handleCopy = async () => {
    try {
      const query = getFormattedMongoQuery();
      await navigator.clipboard.writeText(query);
      setCopySuccess(true);
    } catch (err) {
      console.error("Failed to copy query:", err);
      // Fallback for older browsers
      const textArea = document.createElement("textarea");
      textArea.value = getFormattedMongoQuery();
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand("copy");
      document.body.removeChild(textArea);
      setCopySuccess(true);
    }
  };

  const handleCopyStage = async (stageName, stageContent) => {
    try {
      const stageObject = { [stageName]: stageContent };
      const formattedStage = JSON.stringify(stageObject, null, 2);
      await navigator.clipboard.writeText(formattedStage);
      setCopySuccess(true);
    } catch (err) {
      console.error("Failed to copy stage:", err);
      // Fallback for older browsers
      const stageObject = { [stageName]: stageContent };
      const formattedStage = JSON.stringify(stageObject, null, 2);
      const textArea = document.createElement("textarea");
      textArea.value = formattedStage;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand("copy");
      document.body.removeChild(textArea);
      setCopySuccess(true);
    }
  };

  const handleRunQuery = async () => {
    setIsRunning(true);
    setQueryError(null);
    setExpandedCells(new Set()); // Reset expanded cells for new query

    try {
      const pipeline = generateMongoQuery();
      const prepend = [
        {
          $lookup: {
            from: "ZTF_alerts_aux",
            localField: "objectId",
            foreignField: "_id",
            as: "aux",
          },
        },
        {
          $project: {
            objectId: 1,
            candidate: 1,
            classifications: 1,
            coordinates: 1,
            cross_matches: {
              $arrayElemAt: ["$aux.cross_matches", 0],
            },
            aliases: {
              $arrayElemAt: ["$aux.aliases", 0],
            },
            prv_candidates: {
              $filter: {
                input: {
                  $arrayElemAt: ["$aux.prv_candidates", 0],
                },
                as: "x",
                cond: {
                  $and: [
                    {
                      // maximum 1 year of past data
                      $lt: [
                        {
                          $subtract: ["$candidate.jd", "$$x.jd"],
                        },
                        365,
                      ],
                    },
                    {
                      // only datapoints up to (and including) current alert
                      $lte: ["$$x.jd", "$candidate.jd"],
                    },
                  ],
                },
              },
            },
            fp_hists: {
              $filter: {
                input: {
                  $arrayElemAt: ["$aux.fp_hists", 0],
                },
                as: "x",
                cond: {
                  $and: [
                    {
                      // maximum 1 year of past data
                      $lt: [
                        {
                          $subtract: ["$candidate.jd", "$$x.jd"],
                        },
                        365,
                      ],
                    },
                    {
                      // only datapoints up to (and including) current alert
                      $lte: ["$$x.jd", "$candidate.jd"],
                    },
                  ],
                },
              },
            },
          },
        },
      ];
      // Prepend the pipeline with the prepend stages
      pipeline.unshift(...prepend);
      await dispatch(
        runBoomFilter({
          pipeline: pipeline,
          selectedCollection: selectedCollection,
        }),
      );
    } catch (error) {
      setQueryError(error.message);
    } finally {
      setIsRunning(false);
    }
  };

  const handleSnackbarClose = () => {
    setCopySuccess(false);
  };

  if (!mongoDialog?.open) {
    return null;
  }

  const pipeline = generateMongoQuery();
  const formattedQuery = getFormattedMongoQuery();
  const isValid = hasValidQuery();

  return (
    <>
      <Dialog
        open={mongoDialog.open}
        onClose={handleClose}
        maxWidth="lg"
        fullWidth
        PaperProps={{
          sx: { minHeight: "500px", maxHeight: "90vh" },
        }}
      >
        <DialogTitle
          sx={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
            <Typography variant="h6" component="div">
              MongoDB Aggregation Pipeline
            </Typography>
            {connectionStatus === "connected" && (
              <Chip label="Connected" color="success" size="small" />
            )}
            {connectionStatus === "disconnected" && (
              <Chip label="Disconnected" color="error" size="small" />
            )}
          </Box>
          <IconButton onClick={handleClose} size="small">
            <CloseIcon />
          </IconButton>
        </DialogTitle>

        <DialogContent dividers>
          {!isValid ? (
            <Box sx={{ textAlign: "center", py: 4 }}>
              <Typography variant="body1" color="text.secondary">
                No filters defined. Add some conditions to generate a MongoDB
                query.
              </Typography>
            </Box>
          ) : (
            <Box>
              {/* Connection Warning */}
              {connectionStatus === "disconnected" && (
                <Alert severity="warning" sx={{ mb: 3 }}>
                  <Typography variant="subtitle2">
                    MongoDB Connection Issue
                  </Typography>
                  <Typography variant="body2">
                    Unable to connect to MongoDB. Make sure MongoDB is running
                    on localhost:27017 and the backend server is started.
                  </Typography>
                </Alert>
              )}

              {/* Collection Selector and Run Controls */}
              <Box
                sx={{ display: "flex", gap: 2, mb: 3, alignItems: "center" }}
              >
                <Button
                  variant="contained"
                  color="primary"
                  startIcon={
                    isRunning ? <CircularProgress size={16} /> : <RunIcon />
                  }
                  onClick={handleRunQuery}
                  disabled={isRunning || connectionStatus === "disconnected"}
                  sx={{ minWidth: 120 }}
                >
                  {isRunning ? "Running..." : "Run Query"}
                </Button>
              </Box>

              {/* Query Error Display */}
              {queryError && (
                <Alert severity="error" sx={{ mb: 3 }}>
                  <Typography variant="subtitle2">Query Error:</Typography>
                  <Typography variant="body2">{queryError}</Typography>
                </Alert>
              )}

              {/* Query Results */}
              {Object.keys(results).length > 0 && (
                <Box sx={{ mb: 3 }}>
                  <Box
                    sx={{
                      display: "flex",
                      alignItems: "center",
                      gap: 1,
                      mb: 2,
                    }}
                  >
                    <Typography variant="subtitle1" fontWeight="bold">
                      Query Results
                    </Typography>
                    <Chip
                      label={`${results.data?.length} documents`}
                      size="small"
                      color="success"
                    />
                    <IconButton
                      size="small"
                      onClick={() => setShowResults(!showResults)}
                    >
                      {showResults ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                    </IconButton>
                  </Box>

                  <Collapse in={showResults}>
                    {results.data?.length > 0 ? (
                      <TableContainer
                        component={Paper}
                        sx={{
                          maxHeight: 400,
                          overflow: "auto",
                          width: "100%",
                          "& .MuiTable-root": {
                            minWidth: "100%",
                            width: "max-content",
                            tableLayout: "auto",
                          },
                          // Smooth scrollbar styling
                          "&::-webkit-scrollbar": {
                            width: 8,
                            height: 8,
                          },
                          "&::-webkit-scrollbar-track": {
                            backgroundColor: "rgba(0,0,0,0.1)",
                          },
                          "&::-webkit-scrollbar-thumb": {
                            backgroundColor: "rgba(0,0,0,0.3)",
                            borderRadius: 4,
                          },
                        }}
                      >
                        <Table
                          size="small"
                          stickyHeader
                          sx={{
                            tableLayout: "auto",
                            width: "max-content",
                            minWidth: "100%",
                          }}
                        >
                          <TableHead>
                            <TableRow>
                              {Object.keys(results.data[0] || {})
                                .filter((key) => key !== "_id")
                                .map((key) => (
                                  <TableCell
                                    key={key}
                                    sx={{
                                      fontWeight: "bold",
                                      minWidth: 150,
                                      whiteSpace: "nowrap",
                                      position: "sticky",
                                      top: 0,
                                      backgroundColor: "background.paper",
                                      zIndex: 1,
                                    }}
                                  >
                                    {key}
                                  </TableCell>
                                ))}
                            </TableRow>
                          </TableHead>
                          <TableBody>
                            {results.data.slice(0, 50).map((row, rowIndex) => (
                              <TableRow key={rowIndex}>
                                {Object.entries(row)
                                  .filter(([key]) => key !== "_id")
                                  .map(([key, value], cellIndex) => {
                                    const cellKey = `${rowIndex}-${cellIndex}`;
                                    const isJsonExpanded =
                                      expandedCells.has(cellKey);
                                    const hasJsonContent =
                                      typeof value === "object";

                                    return (
                                      <TableCell
                                        key={cellIndex}
                                        sx={{
                                          verticalAlign: "top",
                                          minWidth: hasJsonContent
                                            ? isJsonExpanded
                                              ? 300
                                              : 150
                                            : 100,
                                          maxWidth: hasJsonContent
                                            ? isJsonExpanded
                                              ? 600
                                              : 300
                                            : 200,
                                          width: hasJsonContent
                                            ? isJsonExpanded
                                              ? "auto"
                                              : "auto"
                                            : "auto",
                                          padding: 1,
                                          borderRight: "1px solid",
                                          borderColor: "divider",
                                          transition: "all 0.3s ease",
                                          overflow: "visible",
                                        }}
                                      >
                                        {hasJsonContent ? (
                                          <Box
                                            sx={{
                                              minWidth: isJsonExpanded
                                                ? 250
                                                : 150,
                                              maxWidth: isJsonExpanded
                                                ? 550
                                                : 350,
                                              width: "100%",
                                            }}
                                          >
                                            <ReactJson
                                              src={value}
                                              name={false}
                                              // theme={darkTheme ? "monokai" : "rjv-default"}
                                            />
                                          </Box>
                                        ) : (
                                          <Typography
                                            variant="body2"
                                            sx={{
                                              fontFamily: "monospace",
                                              wordBreak: "break-word",
                                            }}
                                          >
                                            {String(value)}
                                          </Typography>
                                        )}
                                      </TableCell>
                                    );
                                  })}
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                        {results.data?.length > 50 && (
                          <Typography
                            variant="caption"
                            sx={{ p: 1, display: "block", textAlign: "center" }}
                          >
                            Showing first 50 of {results.data.length} results
                          </Typography>
                        )}
                      </TableContainer>
                    ) : (
                      <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{ p: 2, textAlign: "center" }}
                      >
                        No documents matched the query
                      </Typography>
                    )}
                  </Collapse>

                  <Divider sx={{ my: 2 }} />
                </Box>
              )}

              {/* Pipeline Visualization */}
              <Box sx={{ mb: 3 }}>
                <Box
                  sx={{ display: "flex", alignItems: "center", gap: 1, mb: 2 }}
                >
                  <Typography variant="subtitle1" fontWeight="bold">
                    MongoDB Pipeline ({pipeline.length} stage
                    {pipeline.length !== 1 ? "s" : ""})
                  </Typography>
                  <IconButton
                    size="small"
                    onClick={() => setShowPipeline(!showPipeline)}
                  >
                    {showPipeline ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                  </IconButton>
                </Box>

                <Collapse in={showPipeline}>
                  {/* Pipeline View Tabs */}
                  <Box sx={{ borderBottom: 1, borderColor: "divider", mb: 2 }}>
                    <Tabs
                      value={pipelineView}
                      onChange={(e, newValue) => setPipelineView(newValue)}
                      aria-label="pipeline view tabs"
                    >
                      <Tab
                        label="Complete Pipeline"
                        value="complete"
                        sx={{ textTransform: "none" }}
                      />
                      <Tab
                        label="Stage by Stage"
                        value="stages"
                        sx={{ textTransform: "none" }}
                      />
                    </Tabs>
                  </Box>

                  {/* Tab Content Container */}
                  <Box
                    sx={{
                      minHeight: "400px",
                      position: "relative",
                      backgroundColor: "background.paper",
                    }}
                  >
                    {/* Complete Pipeline View */}
                    {pipelineView === "complete" && (
                      <Box
                        sx={{
                          position: "absolute",
                          top: 0,
                          left: 0,
                          right: 0,
                          minHeight: "100%",
                          backgroundColor: "background.paper",
                        }}
                      >
                        <Box
                          sx={{
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                            mb: 2,
                          }}
                        >
                          <Typography variant="subtitle2" fontWeight="bold">
                            Complete Pipeline JSON:
                          </Typography>
                          <Button
                            variant="outlined"
                            size="small"
                            startIcon={<CopyIcon />}
                            onClick={handleCopy}
                          >
                            Copy to Clipboard
                          </Button>
                        </Box>

                        <Box
                          sx={{
                            backgroundColor: "#f5f5f5",
                            border: "1px solid #ddd",
                            borderRadius: 1,
                            p: 2,
                            maxHeight: "400px",
                            overflow: "auto",
                          }}
                        >
                          <ReactJson
                            src={pipeline}
                            name={false}
                            // theme={darkTheme ? "monokai" : "rjv-default"}
                          />
                        </Box>

                        <Typography
                          variant="caption"
                          color="text.secondary"
                          sx={{ mt: 2, display: "block" }}
                        >
                          This aggregation pipeline can be used directly with
                          MongoDB's aggregate() method.
                        </Typography>
                      </Box>
                    )}

                    {/* Stage by Stage View */}
                    {pipelineView === "stages" && (
                      <Box
                        sx={{
                          position: "absolute",
                          top: 0,
                          left: 0,
                          right: 0,
                          minHeight: "100%",
                          backgroundColor: "background.paper",
                        }}
                      >
                        {/* Individual Stage Details */}
                        <Box
                          sx={{
                            display: "flex",
                            flexDirection: "column",
                            gap: 2,
                          }}
                        >
                          {pipeline.map((stage, index) => {
                            const stageName = Object.keys(stage)[0];
                            const stageContent = stage[stageName];
                            const description = getStageDescription(stageName);
                            const isStageExpanded = expandedStages.has(index);

                            return (
                              <Paper
                                key={index}
                                elevation={1}
                                sx={{
                                  p: 2,
                                  border: "1px solid",
                                  borderColor: "divider",
                                }}
                              >
                                <Box
                                  sx={{
                                    display: "flex",
                                    alignItems: "center",
                                    justifyContent: "space-between",
                                    mb: 1,
                                  }}
                                >
                                  <Box
                                    sx={{
                                      display: "flex",
                                      alignItems: "center",
                                      gap: 1,
                                    }}
                                  >
                                    <Chip
                                      label={`Stage ${index + 1}`}
                                      size="small"
                                      color="primary"
                                      variant="outlined"
                                    />
                                    <Typography
                                      variant="h6"
                                      sx={{
                                        color: "primary.main",
                                        fontFamily: "monospace",
                                      }}
                                    >
                                      {stageName}
                                    </Typography>
                                    <IconButton
                                      size="small"
                                      onClick={() => handleStageToggle(index)}
                                      sx={{ ml: 1 }}
                                    >
                                      {isStageExpanded ? (
                                        <ExpandLessIcon />
                                      ) : (
                                        <ExpandMoreIcon />
                                      )}
                                    </IconButton>
                                  </Box>
                                  <Tooltip title={`Copy ${stageName} stage`}>
                                    <IconButton
                                      size="small"
                                      onClick={() =>
                                        handleCopyStage(stageName, stageContent)
                                      }
                                      sx={{
                                        opacity: 0.7,
                                        "&:hover": { opacity: 1 },
                                      }}
                                    >
                                      <CopyIcon fontSize="small" />
                                    </IconButton>
                                  </Tooltip>
                                </Box>

                                <Typography
                                  variant="body2"
                                  color="text.secondary"
                                  sx={{ mb: 2, fontStyle: "italic" }}
                                >
                                  {description}
                                </Typography>

                                <Collapse in={isStageExpanded}>
                                  <Box
                                    component="pre"
                                    sx={{
                                      backgroundColor: "#f8f9fa",
                                      border: "1px solid #e9ecef",
                                      borderRadius: 1,
                                      p: 1.5,
                                      overflow: "auto",
                                      maxHeight: "300px",
                                      fontFamily:
                                        'Monaco, Consolas, "Courier New", monospace',
                                      fontSize: "13px",
                                      lineHeight: 1.4,
                                      whiteSpace: "pre-wrap",
                                      wordBreak: "break-word",
                                      margin: 0,
                                    }}
                                  >
                                    {JSON.stringify(stageContent, null, 2)}
                                  </Box>
                                </Collapse>
                              </Paper>
                            );
                          })}
                        </Box>
                      </Box>
                    )}
                  </Box>
                </Collapse>
              </Box>
            </Box>
          )}
        </DialogContent>

        <DialogActions>
          <Button onClick={handleClose} variant="contained">
            Close
          </Button>
        </DialogActions>
      </Dialog>

      <Snackbar
        open={copySuccess}
        autoHideDuration={3000}
        onClose={handleSnackbarClose}
        anchorOrigin={{ vertical: "bottom", horizontal: "center" }}
      >
        <Alert
          onClose={handleSnackbarClose}
          severity="success"
          variant="filled"
        >
          MongoDB query copied to clipboard!
        </Alert>
      </Snackbar>
    </>
  );
};

export default MongoQueryDialog;
