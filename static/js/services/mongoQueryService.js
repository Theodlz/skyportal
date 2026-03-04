// Service for executing MongoDB queries
const API_BASE_URL = "http://localhost:3001/api";

export const mongoQueryService = {
  // Run an aggregation pipeline against MongoDB
  async runQuery(pipeline, collectionName) {
    try {
      const response = await fetch(`${API_BASE_URL}/run-query`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          pipeline,
          collection: collectionName,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || "Failed to execute query");
      }

      return data;
    } catch (error) {
      console.error("Error running MongoDB query:", error);
      throw error;
    }
  },

  // Get available collections
  async getCollections() {
    try {
      const response = await fetch(`${API_BASE_URL}/collections`);
      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || "Failed to get collections");
      }

      return data.collections;
    } catch (error) {
      console.error("Error getting collections:", error);
      throw error;
    }
  },
};
