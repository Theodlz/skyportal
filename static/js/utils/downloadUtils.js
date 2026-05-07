/**
 * Fetches all pages of results from a paginated data source and returns
 * the combined array.
 *
 * @param {Function} fetchPage - async (cursor: string|null) => results[]
 *   Called for each page. Must return an array of documents (or empty array
 *   to signal the end). The cursor used for the next call is taken from the
 *   `_id` field of the last item in the slice of size `pageSize`.
 * @param {number} pageSize - Number of items per page.
 * @param {Function} [onProgress] - optional (downloadedCount: number) => void
 *   Called after each page is appended.
 * @returns {Promise<Array>} All collected documents in ascending _id order.
 */
export async function fetchAllPages(fetchPage, pageSize, onProgress) {
  const allData = [];
  let cursor = null;
  let hasMore = true;

  while (hasMore) {
    const results = await fetchPage(cursor);
    if (!results || results.length === 0) break;

    hasMore = results.length > pageSize;
    const pageData = hasMore ? results.slice(0, pageSize) : results;
    allData.push(...pageData);

    if (onProgress) onProgress(allData.length);

    if (hasMore) {
      cursor = pageData[pageData.length - 1]._id;
    }
  }

  return allData;
}

/**
 * Triggers a browser download of the provided data as a JSON file.
 *
 * @param {Array|Object} data - The data to serialize.
 * @param {string} filename - The suggested filename (without extension).
 */
export function downloadAsJson(data, filename) {
  const jsonString = JSON.stringify(data, null, 2);
  const blob = new Blob([jsonString], { type: "application/json" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
}
