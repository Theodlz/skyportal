import PropTypes from "prop-types";
import React from "react";
import ThumbnailList from "./ThumbnailList";

const ThumbnailsOnPage = ({
  ra,
  dec,
  thumbnails,
  rightPanelVisible,
  downSmall,
  downLarge,
}) => {
  if (!rightPanelVisible && !downLarge) {
    return (
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr 1fr",
          gap: "0.5rem",
          gridAutoFlow: "row",
        }}
      >
        <ThumbnailList
          ra={ra}
          dec={dec}
          thumbnails={thumbnails}
          size="100%"
          minSize="10rem"
          maxSize="20rem"
          useGrid={false}
          noMargin
        />
      </div>
    );
  }
  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: "1fr 1fr 1fr",
        gap: "0.5rem",
        gridAutoFlow: "row",
        alignItems: "center",
        maxWidth: "fit-content",
      }}
    >
      <ThumbnailList
        ra={ra}
        dec={dec}
        thumbnails={thumbnails}
        size="100%"
        minSize="6rem"
        maxSize="13rem"
        titleSize={
          !downSmall || (rightPanelVisible && !downLarge) ? "0.8rem" : "0.55em"
        }
        useGrid={false}
        noMargin
      />
    </div>
  );
};

ThumbnailsOnPage.propTypes = {
  ra: PropTypes.number.isRequired,
  dec: PropTypes.number.isRequired,
  thumbnails: PropTypes.arrayOf(PropTypes.object).isRequired, // eslint-disable-line react/forbid-prop-types
  rightPanelVisible: PropTypes.bool.isRequired,
  downSmall: PropTypes.bool.isRequired,
  downLarge: PropTypes.bool.isRequired,
};

// export default ThumbnailsOnPage;

const ThumbnailsOnPageV2 = ({
  ra,
  dec,
  thumbnails,
  rightPanelVisible,
  downSmall,
  downLarge,
}) => {
  if (!rightPanelVisible && !downLarge) {
    return (
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr 1fr 1fr 1fr",
          gap: "0.5rem",
          gridAutoFlow: "row",
        }}
      >
        <ThumbnailList
          ra={ra}
          dec={dec}
          thumbnails={thumbnails}
          size="100%"
          minSize="10rem"
          maxSize="20rem"
          useGrid={false}
          noMargin
        />
      </div>
    );
  }
  // otherwise, show 3 thumbnails in a row for the new/ref/sub thumbnails (if they exist)
  // and then 3 thumbnails in a row for the old thumbnails (if they exist), but scrollable horizontally
  // to see more thumbnails
  return (
    <div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr",
          gap: "0.5rem",
          gridAutoFlow: "row",
          paddingBottom: "0.5rem",
        }}
      >
        <ThumbnailList
          ra={ra}
          dec={dec}
          thumbnails={(thumbnails || []).filter((thumbnail) =>
            ["new", "ref", "sub"].includes(thumbnail.type),
          )}
          size="100%"
          minSize="6rem"
          maxSize="13rem"
          titleSize={
            !downSmall || (rightPanelVisible && !downLarge)
              ? "0.8rem"
              : "0.55em"
          }
          useGrid={false}
          noMargin
        />
      </div>
      <div
        style={{
          // we want 1 row of 3 thumbnails, and scroll vertically to see more rows
          display: "grid",
          gridTemplateColumns: "1fr 1fr 1fr",
          gridTemplateRows: "1fr",
          overflowY: "scroll",
          gap: "0.5rem",
          minHeight: "8.5rem",
          maxHeight: "15.5rem",
          // gridAutoFlow: "column",
          // overflowX: "auto",
          // alignItems: "center",
          // maxWidth: "fit-content",
          // minHeight: "100%",
        }}
      >
        <ThumbnailList
          ra={ra}
          dec={dec}
          thumbnails={(thumbnails || []).filter(
            (thumbnail) => !["new", "ref", "sub"].includes(thumbnail.type),
          )}
          size="100%"
          minSize="6rem"
          maxSize="13rem"
          titleSize={
            !downSmall || (rightPanelVisible && !downLarge)
              ? "0.8rem"
              : "0.55em"
          }
          useGrid={false}
          noMargin
        />
      </div>
    </div>
  );
};

ThumbnailsOnPageV2.propTypes = {
  ra: PropTypes.number.isRequired,
  dec: PropTypes.number.isRequired,
  thumbnails: PropTypes.arrayOf(PropTypes.object).isRequired, // eslint-disable-line react/forbid-prop-types
  rightPanelVisible: PropTypes.bool.isRequired,
  downSmall: PropTypes.bool.isRequired,
  downLarge: PropTypes.bool.isRequired,
};

export default ThumbnailsOnPageV2;
