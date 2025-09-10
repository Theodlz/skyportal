import * as API from "../API";
import store from "../store";

export const RUN_BOOM_FILTER = "skyportal/RUN_BOOM_FILTER";
export const RUN_BOOM_FILTER_OK = "skyportal/RUN_BOOM_FILTER_OK";
export const RUN_BOOM_FILTER_ERROR =
  "skyportal/RUN_BOOM_FILTER_ERROR";
export const RUN_BOOM_FILTER_FAIL = "skyportal/RUN_BOOM_FILTER_FAIL";

export function runBoomFilter({ pipeline, selectedCollection }) {
  return API.POST("/api/queries/pipeline", RUN_BOOM_FILTER, {
    pipeline,
    selectedCollection,
  });
}

const reducer = (state = {}, action) => {
  switch (action.type) {
    case RUN_BOOM_FILTER_OK: {
      return action.data;
    }
    case RUN_BOOM_FILTER_FAIL:
    case RUN_BOOM_FILTER_ERROR: {
      return {};
    }
    default:
      return state;
  }
};

store.injectReducer("query_result", reducer);
