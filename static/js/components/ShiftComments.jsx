import React from "react";
import { useSelector, useDispatch } from "react-redux";
import { makeStyles } from "@material-ui/core/styles";
import Button from "@material-ui/core/Button";
import OutlinedInput from "@material-ui/core/OutlinedInput";
import InputLabel from "@material-ui/core/InputLabel";
import MenuItem from "@material-ui/core/MenuItem";
import FormControl from "@material-ui/core/FormControl";
import Select from "@material-ui/core/Select";
import Chip from "@material-ui/core/Chip";
import Checkbox from "@material-ui/core/Checkbox";
import Box from "@material-ui/core/Box";
import ListItemText from "@material-ui/core/ListItemText";
import Tooltip from "@material-ui/core/Tooltip";
import PropTypes from "prop-types";

import { showNotification } from "baselayer/components/Notifications";
import * as shiftActions from "../ducks/shift";

const useStyles = makeStyles((theme) => ({
  root: {
    marginBottom: theme.spacing(2),
  },
  shift_comments_list: {
    margin: "0",
    padding: "0",
  },
  add_shift_comment: {
    padding: "1rem",
    paddingBottom: "0",
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
  },
}));

//function to check if current user is in the shift -> can be found in ShiftManagement.jsx
//function to handle logic of submitting new comment (dspatch actions to the ducks and stuff)

function CurrentShiftComments({ currentShift, currentShiftComments }) {
  const classes = useStyles();
  const currentUser = useSelector((state) => state.profile);
  const dispatch = useDispatch();

  return (
    currentShift.name != null && (
      <div id="current_shift" className={classes.root}>
        <div className={classes.shift_comments_list}>
          Show List of Comments here
        </div>
        <div className={classes.add_shift_comment}>
          Show Form to Add Comments here
        </div>
      </div>
    )
  );
}

CurrentShiftComments.propTypes = {
  currentShift: PropTypes.shape({
    id: PropTypes.number,
    name: PropTypes.string,
    description: PropTypes.string,
    start_date: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.instanceOf(Date),
    ]),
    end_date: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.instanceOf(Date),
    ]),
    group: PropTypes.shape({
      id: PropTypes.number,
      name: PropTypes.string,
      group_users: PropTypes.arrayOf(
        PropTypes.shape({
          id: PropTypes.number,
          admin: PropTypes.bool,
        })
      ),
    }),
    shift_users: PropTypes.arrayOf(
      PropTypes.shape({
        id: PropTypes.number,
        admin: PropTypes.bool,
      })
    ),
  }).isRequired,
  currentShiftComments: PropTypes.arrayOf(
    PropTypes.shape({
      id: PropTypes.number,
      shift_id: PropTypes.number,
      text: PropTypes.string,
    })
  ).isRequired,
};

export default CurrentShiftComments;
