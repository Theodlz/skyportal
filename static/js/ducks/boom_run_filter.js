import * as API from "../API";
import store from "../store";
import { fetchAllPages } from "../utils/downloadUtils";

export const RUN_BOOM_FILTER = "skyportal/RUN_BOOM_FILTER";
export const RUN_BOOM_FILTER_OK = "skyportal/RUN_BOOM_FILTER_OK";
export const RUN_BOOM_FILTER_ERROR = "skyportal/RUN_BOOM_FILTER_ERROR";
export const RUN_BOOM_FILTER_FAIL = "skyportal/RUN_BOOM_FILTER_FAIL";
export const BOOM_FILTER_CLEAR = "skyportal/BOOM_FILTER_CLEAR";

export const DOWNLOAD_ALL_BOOM_FILTER_RESULTS =
  "skyportal/DOWNLOAD_ALL_BOOM_FILTER_RESULTS";
export const DOWNLOAD_ALL_BOOM_FILTER_RESULTS_OK =
  "skyportal/DOWNLOAD_ALL_BOOM_FILTER_RESULTS_OK";
export const DOWNLOAD_ALL_BOOM_FILTER_RESULTS_ERROR =
  "skyportal/DOWNLOAD_ALL_BOOM_FILTER_RESULTS_ERROR";
export const DOWNLOAD_ALL_BOOM_FILTER_RESULTS_FAIL =
  "skyportal/DOWNLOAD_ALL_BOOM_FILTER_RESULTS_FAIL";

export function runBoomFilter({
  pipeline,
  selectedCollection,
  start_jd,
  end_jd,
  filter_id,
}) {
  return API.POST("/api/boom/run_filter", RUN_BOOM_FILTER, {
    pipeline,
    selectedCollection,
    start_jd,
    end_jd,
    filter_id,
  });
}

export function runBoomTestFilter({
  pipeline,
  selectedCollection,
  start_jd,
  end_jd,
  filter_id,
  sort_by,
  sort_order,
  limit,
  cursor = null,
}) {
  return API.POST("/api/boom/run_filter", RUN_BOOM_FILTER, {
    pipeline,
    selectedCollection,
    start_jd,
    end_jd,
    filter_id,
    sort_by,
    sort_order,
    limit,
    cursor,
  });
}

export function downloadAllBoomFilterResults({
  pipeline,
  selectedCollection,
  start_jd,
  end_jd,
  filter_id,
  pageSize,
  onProgress,
}) {
  return async (dispatch) => {
    return fetchAllPages(
      async (cursor) => {
        const result = await dispatch(
          API.POST("/api/boom/run_filter", DOWNLOAD_ALL_BOOM_FILTER_RESULTS, {
            pipeline,
            selectedCollection,
            start_jd,
            end_jd,
            filter_id,
            sort_by: "_id",
            sort_order: "Ascending",
            limit: pageSize + 1,
            cursor,
          }),
        );
        if (
          result.type === DOWNLOAD_ALL_BOOM_FILTER_RESULTS_ERROR ||
          result.type === DOWNLOAD_ALL_BOOM_FILTER_RESULTS_FAIL
        ) {
          throw new Error(result.message || "Failed to fetch page");
        }
        return result.data?.data?.results ?? [];
      },
      pageSize,
      onProgress,
    );
  };
}

export function clearBoomFilter() {
  return {
    type: BOOM_FILTER_CLEAR,
  };
}

const reducer = (state = {}, action) => {
  switch (action.type) {
    case RUN_BOOM_FILTER_OK: {
      return action.data;
    }
    case RUN_BOOM_FILTER_FAIL:
    case RUN_BOOM_FILTER_ERROR:
    case BOOM_FILTER_CLEAR: {
      return {};
    }
    default:
      return state;
  }
};

store.injectReducer("query_result", reducer);
