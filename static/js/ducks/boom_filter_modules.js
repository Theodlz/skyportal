import * as API from "../API";
import store from "../store";

export const FETCH_ALL_ELEMENTS = "skyportal/FETCH_ALL_ELEMENTS";
export const FETCH_ALL_ELEMENTS_OK = "skyportal/FETCH_ALL_ELEMENTS_OK";
export const FETCH_ALL_ELEMENTS_ERROR = "skyportal/FETCH_ALL_ELEMENTS_ERROR";
export const FETCH_ALL_ELEMENTS_FAIL = "skyportal/FETCH_ALL_ELEMENTS_FAIL";

export const FETCH_ELEMENT = "skyportal/FETCH_ELEMENT";
export const FETCH_ELEMENT_OK = "skyportal/FETCH_ELEMENT_OK";

export const FETCH_SCHEMA = "skyportal/FETCH_SCHEMA";
export const FETCH_SCHEMA_OK = "skyportal/FETCH_SCHEMA_OK";

export const POST_ELEMENT = "skyportal/POST_ELEMENT";
export const POST_ELEMENT_OK = "skyportal/POST_ELEMENT_OK";

export function fetchAllElements({ elements }) {
  return API.GET(`/api/filter_modules`, FETCH_ALL_ELEMENTS, { elements });
}

export function fetchElement({ name, elements }) {
  return API.GET(`/api/filter_modules/${name}`, FETCH_ELEMENT, { elements });
}

export function fetchSchema({ name, elements }) {
  return API.GET(`/api/filter_modules/${name}`, FETCH_SCHEMA, { elements });
}

export function postElement({ name, data, elements }) {
  return API.POST(`/api/filter_modules/${name}`, POST_ELEMENT, {
    data,
    elements,
  });
}

const reducer = (state = {}, action) => {
  switch (action.type) {
    case FETCH_SCHEMA_OK: {
      const schemaVersion = action.data.schema?.versions?.find(
        (e) => e.vid === action.data.schema?.active_id,
      );

      if (!schemaVersion?.schema) {
        console.warn(
          "No schema found for active version:",
          action.data.schema?.active_id,
        );
        return { schema: null };
      }

      try {
        const schema_from_db = JSON.parse(schemaVersion.schema);
        const res = { schema: schema_from_db };
        return res;
      } catch (error) {
        console.error(
          "Error parsing schema JSON:",
          error,
          schemaVersion.schema,
        );
        return { schema: null };
      }
    }
    case FETCH_ELEMENT_OK:
    case FETCH_ALL_ELEMENTS_OK: {
      return action.data;
    }
    case FETCH_ALL_ELEMENTS_FAIL:
    case FETCH_ALL_ELEMENTS_ERROR: {
      return {};
    }
    default:
      return state;
  }
};

store.injectReducer("filter_modules", reducer);
