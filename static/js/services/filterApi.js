import * as API from "../API";

// Fetch all saved custom blocks from the backend
export async function fetchSavedBlocks() {
  const response = await fetch("http://localhost:3001/api/custom-blocks");
  if (!response.ok) throw new Error("Failed to fetch saved blocks");
  const data = await response.json();
  return data.blocks;
}

// Fetch all saved variables from the backend
export async function fetchSavedVariables() {
  const response = await fetch("http://localhost:3001/api/custom-variables");
  if (!response.ok) throw new Error("Failed to fetch saved variables");
  const data = await response.json();
  return data.variables;
}

// Save a new custom block to the backend
export async function saveBlock(block, name) {
  const response = await fetch("http://localhost:3001/api/save-block", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ block, name }),
  });
  if (!response.ok) throw new Error("Failed to save block");
  return await response.json();
}

// Save a new custom variable to the backend
export async function saveVariable(variable, name, type) {
  const response = await fetch("http://localhost:3001/api/save-variable", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ variable, name, type }),
  });
  if (!response.ok) throw new Error("Failed to save block");
  return await response.json();
}

// Save a new custom list variable to the backend
export async function saveListVariable(listCondition, name, type = "array") {
  const response = await fetch("http://localhost:3001/api/list-variables", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ listCondition, name, type }),
  });
  if (!response.ok) throw new Error("Failed to save list variable");
  return await response.json();
}

// Check if a block name is available
export async function checkBlockNameAvailable(name) {
  const response = await fetch(
    `http://localhost:3001/api/check-block-name?name=${encodeURIComponent(
      name,
    )}`,
  );
  if (!response.ok) throw new Error("Failed to check block name");
  const data = await response.json();
  return data.available;
}

export const fetchSavedListVariables = async () => {
  try {
    const response = await fetch("http://localhost:3001/api/list-variables");
    if (response.ok) {
      const test = await response.json();
      return Object.entries(test)[0]?.[1];
    }
    return [];
  } catch (error) {
    console.error("Error fetching list variables:", error);
    return [];
  }
};

export const checkListVariableNameAvailable = async (name) => {
  try {
    const response = await fetch(
      `/api/list-variables/check-name?name=${encodeURIComponent(name)}`,
    );
    if (response.ok) {
      const data = await response.json();
      return data.available;
    }
    return false;
  } catch (error) {
    console.error("Error checking list variable name:", error);
    return false;
  }
};

// Check if a filter name is available
export async function checkFilterNameAvailable(name) {
  const response = await fetch(
    `http://localhost:3001/api/check-filter-name?name=${encodeURIComponent(
      name,
    )}`,
  );
  if (!response.ok) throw new Error("Failed to check filter name");
  const data = await response.json();
  return data.available;
}

// Authentication function for boom database
async function authenticate(username, password) {
  const response = await fetch("http://localhost:4000/auth", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      username: username,
      password: password,
    }),
  });

  if (!response.ok) {
    throw new Error(`Authentication failed: ${response.status}`);
  }

  const data = await response.json();
  return data.data.token;
}
